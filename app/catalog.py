"""底图库目录:SQLite 元数据 + 不可变 COG 文件。

发布一个底图/新版本的步骤:

1. 把任务结果 COG 分块复制到 ``staging/``。复制期间不开启数据库写事务,
   因此原底图始终可以被查询、下载。
2. 对暂存文件 fsync,并校验字节数与源文件一致。
3. 原子重命名到 ``blobs/<随机名>.tif`` 并 fsync 目录。每个修订对应唯一文件,
   旧修订永不覆盖、也不主动删除,所以在途下载始终读到同一份内容。
4. 用一个短事务登记来源任务、范围、尺寸、大小等全部元数据,并推进修订号。
   复制、重命名或登记任一步失败,旧版本保持不变。

崩溃恢复:数据库只引用已经 fsync 并重命名完成的 blob;未登记 blob 与
``staging/`` 中的残留文件在下次启动时清理。SQLite 使用 WAL + synchronous=FULL,
提交具有原子性与持久性,重启后仍提供最近一次完整提交的版本。
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import math
import os
import shutil
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Optional, Tuple

COPY_CHUNK = 1 << 20  # 1 MiB,复制大文件时内存占用恒定
MAX_NAME_LEN = 120

SCHEMA = """
CREATE TABLE IF NOT EXISTS basemaps (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT    NOT NULL UNIQUE,
    current_revision INTEGER NOT NULL,
    created_at       REAL    NOT NULL,
    updated_at       REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS basemap_revisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    basemap_id      INTEGER NOT NULL REFERENCES basemaps(id),
    revision        INTEGER NOT NULL,
    blob_name       TEXT    NOT NULL UNIQUE,
    size_bytes      INTEGER NOT NULL,
    source_task_id  TEXT    NOT NULL,
    width           INTEGER NOT NULL,
    height          INTEGER NOT NULL,
    extent_json     TEXT    NOT NULL,
    crs             TEXT    NOT NULL,
    created_at      REAL    NOT NULL,
    UNIQUE(basemap_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_revisions_basemap
    ON basemap_revisions(basemap_id, revision);
"""

_REVISION_ROW_SQL = """
SELECT b.id              AS basemap_id,
       b.name            AS name,
       b.current_revision AS current_revision,
       b.created_at      AS basemap_created_at,
       b.updated_at      AS updated_at,
       r.revision        AS revision,
       r.blob_name       AS blob_name,
       r.size_bytes      AS size_bytes,
       r.source_task_id  AS source_task_id,
       r.width           AS width,
       r.height          AS height,
       r.extent_json     AS extent_json,
       r.crs             AS crs,
       r.created_at      AS revision_created_at
FROM basemaps b
JOIN basemap_revisions r ON r.basemap_id = b.id
"""

_CURRENT_COND = "r.revision = b.current_revision"


class CatalogError(Exception):
    """底图库通用错误。"""


class CatalogValidationError(CatalogError):
    """调用方给出的名称/元数据不合法。"""


class CatalogNotFound(CatalogError):
    """底图或修订不存在。"""


class CatalogConflict(CatalogError):
    """名称已存在或所见修订已过期;原版本保持不变。"""

    def __init__(self, message: str, current_revision: Optional[int] = None):
        super().__init__(message)
        self.current_revision = current_revision


class BasemapCatalog:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.db_path = self.root / "catalog.sqlite3"
        self.staging_dir = self.root / "staging"
        self.blobs_dir = self.root / "blobs"
        self.copy_chunk = COPY_CHUNK
        # 测试注入点:每次复制读块前回调(用于观察"复制期间不持写锁")
        self._copy_hook = None
        # 只保护"复制完成后的改名 + 短事务登记",绝不覆盖大文件复制。
        self._write_lock = threading.Lock()
        self._publish_lock_path = self.root / ".publish.lock"

    # ---------- 初始化 / 清理 ----------

    def initialize(self) -> None:
        for d in (self.root, self.staging_dir, self.blobs_dir):
            d.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")  # 读写不互斥
            conn.execute("PRAGMA synchronous=FULL")
            conn.executescript(SCHEMA)
        finally:
            conn.close()
        _fsync_dir_quietly(self.blobs_dir)
        self.cleanup_unpublished()

    @contextlib.contextmanager
    def _publish_flock(self, exclusive: bool, blocking: bool):
        """发布与启动清理之间的进程间锁(与数据库写锁无关)。

        - 发布全程持**共享**锁:多个发布可并行,但启动清理(独占)拿不到锁;
        - 启动清理持**独占**非阻塞锁:若有发布在途则跳过,绝不清掉在途文件。

        每次调用独立打开句柄,同进程多线程也会正确竞争同一把锁。
        """
        f = open(self._publish_lock_path, "a+")
        try:
            op = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            if not blocking:
                op |= fcntl.LOCK_NB
            try:
                fcntl.flock(f.fileno(), op)
            except OSError:
                if blocking:
                    raise
                yield False
                return
            try:
                yield True
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        finally:
            f.close()

    def cleanup_unpublished(self) -> int:
        """删除暂存残留与未被任何已登记修订引用的 blob。

        发布顺序保证:数据库引用生效前,文件已经完整落盘;因此未被引用的
        blob 一定来自中途退出的发布,可以安全删除。
        """
        with self._publish_flock(exclusive=True, blocking=False) as locked:
            if not locked:
                return 0  # 有发布在途,本次启动清理跳过,避免误删未登记文件
            return self._cleanup_locked()

    def _cleanup_locked(self) -> int:
        removed = 0
        for p in self.staging_dir.iterdir():
            if p.is_dir() and not p.is_symlink():
                _rmtree_quietly(p)
            else:
                _unlink_quietly(p)
            removed += 1

        conn = self._connect()
        try:
            rows = conn.execute("SELECT blob_name FROM basemap_revisions").fetchall()
        finally:
            conn.close()
        referenced = {row["blob_name"] for row in rows}
        for p in self.blobs_dir.iterdir():
            if p.name in referenced or (p.is_dir() and not p.is_symlink()):
                continue
            _unlink_quietly(p)
            removed += 1
        return removed

    # ---------- 查询(不持写锁,复制大文件期间亦可进行) ----------

    def list_basemaps(self) -> list:
        conn = self._connect()
        try:
            rows = conn.execute(
                _REVISION_ROW_SQL
                + f" WHERE {_CURRENT_COND} ORDER BY b.updated_at DESC, b.id DESC"
            ).fetchall()
        finally:
            conn.close()
        return [self._public_basemap(r) for r in rows]

    def get_basemap(self, basemap_id: int) -> dict:
        conn = self._connect()
        try:
            row = conn.execute(
                _REVISION_ROW_SQL + f" WHERE {_CURRENT_COND} AND b.id = ?", (basemap_id,)
            ).fetchone()
            if row is None:
                raise CatalogNotFound(f"底图 {basemap_id} 不存在")
            history = conn.execute(
                _REVISION_ROW_SQL
                + " WHERE b.id = ? AND r.basemap_id = b.id ORDER BY r.revision DESC",
                (basemap_id,),
            ).fetchall()
        finally:
            conn.close()
        public = self._public_basemap(row)
        public["revisions"] = [
            self._public_revision(r, public["name"], public["revision"])
            for r in history
        ]
        return public

    def get_current_revision(self, basemap_id: int) -> Tuple[dict, Path]:
        """返回当前修订的公开信息与不可变文件路径。"""
        conn = self._connect()
        try:
            row = conn.execute(
                _REVISION_ROW_SQL + f" WHERE {_CURRENT_COND} AND b.id = ?", (basemap_id,)
            ).fetchone()
            if row is None:
                raise CatalogNotFound(f"底图 {basemap_id} 不存在")
        finally:
            conn.close()
        basemap = self._public_basemap(row)
        return basemap, self.blob_path(row["blob_name"])

    def get_revision(self, basemap_id: int, revision: int) -> Tuple[dict, Path]:
        """按修订号定位历史文件;该文件内容永不改变。"""
        conn = self._connect()
        try:
            base = conn.execute(
                "SELECT name, current_revision FROM basemaps WHERE id = ?", (basemap_id,)
            ).fetchone()
            if base is None:
                raise CatalogNotFound(f"底图 {basemap_id} 不存在")
            row = conn.execute(
                _REVISION_ROW_SQL + " WHERE r.basemap_id = ? AND r.revision = ?",
                (basemap_id, revision),
            ).fetchone()
            if row is None:
                raise CatalogNotFound(f"底图 {basemap_id} 的修订 {revision} 不存在")
        finally:
            conn.close()
        return (
            self._public_revision(row, base["name"], base["current_revision"]),
            self.blob_path(row["blob_name"]),
        )

    # ---------- 发布 ----------

    def publish(
        self,
        name: str,
        source_path: Path,
        source_task_id: str,
        metadata: dict,
        expected_revision: Optional[int] = None,
    ) -> Tuple[dict, bool]:
        """发布首版或替换现有底图。

        ``expected_revision`` 为空/0 表示首次发布;替换时必须等于调用方看到的
        当前修订。返回 ``(底图信息, 是否新建)``。
        并发同名发布由进程内写锁 + 事务内修订检查串行化,仅一份成功;
        进行中的发布持共享 flock,确保启动清理不会删掉尚未登记的文件。
        """
        clean_name = _validate_name(name)
        expected = _validate_expected_revision(expected_revision)
        meta = _validate_metadata(metadata)
        task_id = str(source_task_id or "").strip()
        if not task_id:
            raise CatalogValidationError("来源任务不能为空")

        blob_name = f"{uuid.uuid4().hex}.tif"
        final_path = self.blobs_dir / blob_name
        staged: Optional[Path] = None
        renamed = False
        committed = False
        try:
            # 共享锁阻止启动清理删除在途文件;复制期间仍不持任何数据库写锁,
            # 因此旧版本的查询与下载不受影响。
            with self._publish_flock(exclusive=False, blocking=True):
                staged, size_bytes = self._copy_to_staging(Path(source_path))
                with self._write_lock:
                    os.replace(staged, final_path)
                staged = None
                renamed = True
                _fsync_dir(self.blobs_dir)
                public, created = self._commit(
                    clean_name, expected, blob_name, size_bytes, task_id, meta
                )
            committed = True
            return public, created
        finally:
            if not committed:
                # 冲突/复制或登记失败:未生效的新文件不能留下
                if staged is not None:
                    _unlink_quietly(staged)
                if renamed:
                    _unlink_quietly(final_path)
                    _fsync_dir_quietly(self.blobs_dir)

    def _commit(
        self,
        name: str,
        expected: Optional[int],
        blob_name: str,
        size_bytes: int,
        source_task_id: str,
        meta: dict,
    ) -> Tuple[dict, bool]:
        """短事务:检查名称/修订 + 登记全部元数据 + 推进 current_revision。"""
        now = time.time()
        basemap_id = 0
        with self._write_lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT id, current_revision FROM basemaps WHERE name = ?", (name,)
                ).fetchone()
                if row is None:
                    if expected not in (None, 0):
                        raise CatalogConflict(
                            f"底图名称 {name!r} 不存在,无法替换", current_revision=None
                        )
                    cur = conn.execute(
                        "INSERT INTO basemaps(name, current_revision, created_at, updated_at)"
                        " VALUES (?, 1, ?, ?)",
                        (name, now, now),
                    )
                    basemap_id = int(cur.lastrowid)
                    revision = 1
                    created = True
                else:
                    basemap_id = int(row["id"])
                    current = int(row["current_revision"])
                    if expected is None:
                        raise CatalogConflict(
                            f"底图名称 {name!r} 已存在;替换必须携带所见修订号",
                            current_revision=current,
                        )
                    if expected != current:
                        raise CatalogConflict(
                            f"底图 {name!r} 已更新:所见修订 {expected},当前修订 {current}",
                            current_revision=current,
                        )
                    revision = current + 1
                    created = False

                conn.execute(
                    """INSERT INTO basemap_revisions
                       (basemap_id, revision, blob_name, size_bytes, source_task_id,
                        width, height, extent_json, crs, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        basemap_id,
                        revision,
                        blob_name,
                        size_bytes,
                        source_task_id,
                        meta["width"],
                        meta["height"],
                        json.dumps(meta["extent"], separators=(",", ":")),
                        meta["crs"],
                        now,
                    ),
                )
                conn.execute(
                    "UPDATE basemaps SET current_revision = ?, updated_at = ? WHERE id = ?",
                    (revision, now, basemap_id),
                )
                # 在提交前读回本次事务写入的修订,响应不会误报他人并发写入的版本
                written = conn.execute(
                    _REVISION_ROW_SQL + " WHERE b.id = ? AND r.revision = ?",
                    (basemap_id, revision),
                ).fetchone()
                conn.execute("COMMIT")
            except CatalogConflict:
                _rollback_quietly(conn)
                raise
            except sqlite3.IntegrityError as e:
                _rollback_quietly(conn)
                current = self._current_revision_by_name(name)
                raise CatalogConflict(
                    f"底图名称 {name!r} 同名发布冲突,请刷新后重试",
                    current_revision=current,
                ) from e
            except BaseException:
                _rollback_quietly(conn)
                raise
            finally:
                conn.close()

        public = self._public_revision(written, written["name"], int(written["current_revision"]))
        return public, created

    # ---------- 文件与内部工具 ----------

    def blob_path(self, blob_name: str) -> Path:
        """把数据库中的 blob 名解析为库内绝对路径,拒绝路径穿越。"""
        if (
            not isinstance(blob_name, str)
            or not blob_name
            or "/" in blob_name
            or "\\" in blob_name
            or blob_name in (".", "..")
        ):
            raise CatalogError("底图文件路径非法")
        base = self.blobs_dir.resolve()
        p = (self.blobs_dir / blob_name).resolve()
        try:
            p.relative_to(base)
        except ValueError as e:
            raise CatalogError("底图文件路径越界") from e
        return p

    def _copy_to_staging(self, source: Path) -> Tuple[Path, int]:
        staged = self.staging_dir / f"{uuid.uuid4().hex}.part"
        try:
            src_before = source.stat()
        except OSError as e:
            raise CatalogValidationError(f"源 COG 不可用: {e}") from e
        if not os.path.isfile(source):
            raise CatalogValidationError(f"源 COG 不是普通文件: {source}")

        total = 0
        try:
            with open(source, "rb") as rf, open(staged, "xb") as wf:
                while True:
                    if self._copy_hook is not None:
                        self._copy_hook(self)
                    chunk = rf.read(self.copy_chunk)
                    if not chunk:
                        break
                    wf.write(chunk)
                    total += len(chunk)
                wf.flush()
                os.fsync(wf.fileno())
            src_after = source.stat()
        except OSError as e:
            _unlink_quietly(staged)
            raise CatalogError(f"复制 COG 失败: {e}") from e

        if (
            src_after.st_size != src_before.st_size
            or total != src_before.st_size
            or src_after.st_mtime_ns != src_before.st_mtime_ns
        ):
            _unlink_quietly(staged)
            raise CatalogError("复制 COG 不完整:源文件在复制期间发生变化")
        if total <= 0:
            _unlink_quietly(staged)
            raise CatalogValidationError("源 COG 为空文件")
        return staged, total

    def _current_revision_by_name(self, name: str) -> Optional[int]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT current_revision FROM basemaps WHERE name = ?", (name,)
            ).fetchone()
        finally:
            conn.close()
        return int(row["current_revision"]) if row else None

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    @staticmethod
    def _extent(row: sqlite3.Row) -> list:
        try:
            extent = json.loads(row["extent_json"])
        except (TypeError, ValueError):
            return []
        return extent if isinstance(extent, list) else []

    def _public_basemap(self, row: sqlite3.Row) -> dict:
        basemap_id = int(row["basemap_id"])
        revision = int(row["revision"])
        return {
            "id": basemap_id,
            "name": row["name"],
            "revision": revision,
            "created_at": float(row["basemap_created_at"]),
            "updated_at": float(row["updated_at"]),
            "source_task_id": row["source_task_id"],
            "width": int(row["width"]),
            "height": int(row["height"]),
            "extent": self._extent(row),
            "crs": row["crs"],
            "size_bytes": int(row["size_bytes"]),
            "download_url": f"/api/basemaps/{basemap_id}/revisions/{revision}/download",
        }

    def _public_revision(self, row: sqlite3.Row, name: str, current: int) -> dict:
        basemap_id = int(row["basemap_id"])
        revision = int(row["revision"])
        return {
            "id": basemap_id,
            "basemap_id": basemap_id,
            "name": name,
            "revision": revision,
            "is_current": revision == current,
            "source_task_id": row["source_task_id"],
            "width": int(row["width"]),
            "height": int(row["height"]),
            "extent": self._extent(row),
            "crs": row["crs"],
            "size_bytes": int(row["size_bytes"]),
            "created_at": float(row["revision_created_at"]),
            "download_url": f"/api/basemaps/{basemap_id}/revisions/{revision}/download",
        }


# ---------- 校验与文件系统工具 ----------

def _validate_name(name: str) -> str:
    if not isinstance(name, str):
        raise CatalogValidationError("底图名称必须是字符串")
    clean = name.strip()
    if not clean:
        raise CatalogValidationError("底图名称不能为空")
    if len(clean) > MAX_NAME_LEN:
        raise CatalogValidationError(f"底图名称不能超过 {MAX_NAME_LEN} 个字符")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in clean):
        raise CatalogValidationError("底图名称不能包含控制字符")
    return clean


def _validate_expected_revision(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise CatalogValidationError("expected_revision 必须是整数")
    if value < 0:
        raise CatalogValidationError("expected_revision 不能为负数")
    return value


def _validate_metadata(metadata: dict) -> dict:
    if not isinstance(metadata, dict):
        raise CatalogValidationError("底图元数据必须是对象")
    try:
        width = int(metadata["width"])
        height = int(metadata["height"])
    except (KeyError, TypeError, ValueError) as e:
        raise CatalogValidationError(f"底图尺寸无效: {e}") from e
    if width <= 0 or height <= 0:
        raise CatalogValidationError("底图尺寸必须为正整数")

    extent = metadata.get("extent")
    if not isinstance(extent, (list, tuple)) or len(extent) != 4:
        raise CatalogValidationError("底图范围必须是 [minx, miny, maxx, maxy]")
    try:
        ext = [float(v) for v in extent]
    except (TypeError, ValueError) as e:
        raise CatalogValidationError(f"底图范围含非数字: {e}") from e
    if not all(math.isfinite(v) for v in ext):
        raise CatalogValidationError("底图范围含非有限数值")
    if ext[0] > ext[2] or ext[1] > ext[3]:
        raise CatalogValidationError("底图范围下限不能大于上限")

    crs = metadata.get("crs")
    if not isinstance(crs, str) or not crs.strip():
        raise CatalogValidationError("底图坐标系不能为空")
    return {"width": width, "height": height, "extent": ext, "crs": crs.strip()}


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def _unlink_quietly(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _rmtree_quietly(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir_quietly(path: Path) -> None:
    try:
        _fsync_dir(path)
    except OSError:
        pass
