"""底图库(发布/替换/并发/崩溃恢复)测试,无需 GDAL 与 FastAPI。

运行: python3 -I tests/test_catalog.py
"""
import hashlib
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

import catalog as cat  # noqa: E402

META = {
    "width": 120,
    "height": 80,
    "extent": [100.0, 40.0, 340.0, 200.0],
    "crs": "EPSG:3857",
}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def new_source(root: Path, name: str, payload: bytes) -> Path:
    p = root / name
    p.write_bytes(payload)
    return p


def expect_conflict(fn, current_marker="unset"):
    try:
        fn()
    except cat.CatalogConflict as e:
        if current_marker != "unset":
            assert e.current_revision == current_marker, (e.current_revision, current_marker)
        return e
    raise AssertionError("应抛出 CatalogConflict")


def expect_invalid(fn):
    try:
        fn()
    except cat.CatalogValidationError:
        return
    raise AssertionError("应抛出 CatalogValidationError")


def main():
    tmp = Path(tempfile.mkdtemp(prefix="catalog-test-"))
    lib = tmp / "basemaps"
    src_dir = tmp / "tasks"  # 模拟任务暂存,与底图库完全分离
    src_dir.mkdir()
    c = cat.BasemapCatalog(lib)
    c.initialize()

    # --- 1. 首次发布:名称不存在;全部元数据随文件一起生效 ---
    s1 = new_source(src_dir, "task1.tif", b"COG-ONE" * 1000)
    pub, created = c.publish("图层A", s1, "task1", META)
    assert created is True
    assert pub["name"] == "图层A" and pub["revision"] == 1
    info, p1 = c.get_current_revision(pub["id"])
    assert info["source_task_id"] == "task1"
    assert info["width"] == 120 and info["height"] == 80
    assert info["extent"] == META["extent"] and info["crs"] == "EPSG:3857"
    assert info["size_bytes"] == s1.stat().st_size
    assert p1.read_bytes() == s1.read_bytes()
    assert pub["download_url"].endswith("/revisions/1/download")

    # 底图库独立存储:删除任务侧源文件后仍可下载
    s1.unlink()
    assert c.get_current_revision(pub["id"])[1].read_bytes() == b"COG-ONE" * 1000

    # --- 2. 首次发布名称已存在 -> 409 语义,原版与文件均不变 ---
    s2 = new_source(src_dir, "task2.tif", b"COG-TWO" * 500)
    expect_conflict(lambda: c.publish("图层A", s2, "task2", META), current_marker=1)
    info, p_cur = c.get_current_revision(pub["id"])
    assert info["revision"] == 1 and sha(p_cur) == sha(p1)
    assert not list(c.staging_dir.iterdir()), "冲突时暂存文件必须删除"
    assert len(list(c.blobs_dir.iterdir())) == 1, "冲突时不得留下新 blob"

    # 名称两端空白视为同一名称
    expect_conflict(lambda: c.publish(" 图层A ", s2, "task2", META), current_marker=1)

    # --- 3. 替换必须携带所见修订;成功推进修订号 ---
    expect_conflict(lambda: c.publish("图层A", s2, "task2", META, None), current_marker=1)
    expect_conflict(lambda: c.publish("图层A", s2, "task2", META, 0), current_marker=1)
    expect_conflict(lambda: c.publish("图层A", s2, "task2", META, 2), current_marker=1)
    pub2, created2 = c.publish("图层A", s2, "task2", META, 1)
    assert created2 is False and pub2["revision"] == 2
    assert pub2["source_task_id"] == "task2"
    _, p2 = c.get_current_revision(pub["id"])
    assert p2.read_bytes() == s2.read_bytes()

    # 旧修订引用仍指向原文件,内容不变(在途下载/旧链接均安全)
    old_info, old_path = c.get_revision(pub["id"], 1)
    assert old_info["is_current"] is False
    assert sha(old_path) == sha(p1) and sha(p2) != sha(p1)
    assert len(list(c.blobs_dir.iterdir())) == 2, "每个修订对应独立文件,不覆盖旧文件"

    # 在途下载:换版前打开的旧文件句柄仍完整读到旧内容
    handle = open(old_path, "rb")
    try:
        c.publish("图层A", new_source(src_dir, "task2b.tif", b"COG-TWO-B" * 500), "task2b", META, 2)
        data = handle.read()
        assert data == b"COG-ONE" * 1000, "换版不得影响已打开的旧修订下载"
    finally:
        handle.close()
    assert sha(old_path) == sha(p1), "旧修订文件仍存在且未被改写"

    detail = c.get_basemap(pub["id"])
    assert [r["revision"] for r in detail["revisions"]] == [3, 2, 1]
    assert [r["is_current"] for r in detail["revisions"]] == [True, False, False]

    # --- 4. 复制大文件期间不持数据库写锁:查询/下载并发可用 ---
    cur_before_copy_info, cur_before_copy = c.get_current_revision(pub["id"])
    assert cur_before_copy_info["revision"] == 3
    before_copy_sha = sha(cur_before_copy)
    big_bytes = os.urandom(12 << 20)  # 12 MiB
    sbig = new_source(src_dir, "task4.tif", big_bytes)
    c.copy_chunk = 1 << 16
    copy_started = threading.Event()
    release_copy = threading.Event()

    def hook(_catalog):
        copy_started.set()
        release_copy.wait(10)

    c._copy_hook = hook

    def do_publish():
        c.publish("图层A", sbig, "task4", META, 3)

    t = threading.Thread(target=do_publish)
    t.start()
    try:
        assert copy_started.wait(10), "复制钩子未触发"
        # 复制尚未完成、短写事务尚未开启:此刻读取当前版本应立即成功
        t0 = time.monotonic()
        info_mid, path_mid = c.get_current_revision(pub["id"])
        assert time.monotonic() - t0 < 2.0, "复制期间查询不应阻塞"
        assert info_mid["revision"] == 3
        assert sha(path_mid) == before_copy_sha, "复制期间仍应读到最近已提交版本"
        assert c.list_basemaps()[0]["revision"] == 3
    finally:
        release_copy.set()
        t.join(30)
        c._copy_hook = None
    assert not t.is_alive(), "发布线程未结束"
    _, p4 = c.get_current_revision(pub["id"])
    assert p4.read_bytes() == big_bytes, "大文件复制内容必须完整"
    assert old_path.exists() and p1.exists() and p2.exists()

    # --- 5. 同名并发发布只能一份成功,其余 409,原版不变 ---
    base_rev = c.get_current_revision(pub["id"])[0]["revision"]  # 4
    srcs = [new_source(src_dir, f"conc{i}.tif", bytes([i]) * 4096) for i in range(6)]
    results = []
    barrier = threading.Barrier(len(srcs))

    def racer(i):
        barrier.wait(10)
        try:
            r, _ = c.publish("图层A", srcs[i], f"conc{i}", META, base_rev)
            results.append(("ok", i, r["revision"]))
        except cat.CatalogConflict as e:
            results.append(("409", i, e.current_revision))

    threads = [threading.Thread(target=racer, args=(i,)) for i in range(len(srcs))]
    for th in threads:
        th.start()
    for th in threads:
        th.join(30)
    assert all(not th.is_alive() for th in threads)
    oks = [r for r in results if r[0] == "ok"]
    assert len(oks) == 1, f"同名并发发布应只有一份成功: {results}"
    assert len(results) - len(oks) == len(srcs) - 1
    assert all(r[2] == base_rev + 1 for r in results if r[0] == "409"), results
    winner = oks[0][1]
    assert c.get_current_revision(pub["id"])[0]["revision"] == base_rev + 1
    _, pf = c.get_current_revision(pub["id"])
    assert pf.read_bytes() == srcs[winner].read_bytes(), "当前版本应为唯一成功者的内容"
    assert not list(c.staging_dir.iterdir()), "并发失败者的暂存文件必须全部清理"

    # 并发首次发布同名(均未携带修订)同样只有一份成功
    first_srcs = [new_source(src_dir, f"first{i}.tif", bytes([i]) * 1024) for i in range(4)]
    first_results = []
    barrier2 = threading.Barrier(len(first_srcs))

    def first_racer(i):
        barrier2.wait(10)
        try:
            _, created = c.publish("并发新图", first_srcs[i], f"first{i}", META)
            first_results.append(("ok", i, created))
        except cat.CatalogConflict:
            first_results.append(("409", i, None))

    fthreads = [threading.Thread(target=first_racer, args=(i,)) for i in range(len(first_srcs))]
    for th in fthreads:
        th.start()
    for th in fthreads:
        th.join(30)
    assert len([r for r in first_results if r[0] == "ok"]) == 1, first_results
    pub_first = next(b for b in c.list_basemaps() if b["name"] == "并发新图")
    assert pub_first["revision"] == 1
    first_winner = next(r[1] for r in first_results if r[0] == "ok")
    assert c.get_current_revision(pub_first["id"])[1].read_bytes() == first_srcs[first_winner].read_bytes()

    # --- 6. 复制失败(源文件在复制期间变化)-> 旧版保留,无残留 ---
    rev_before = c.get_current_revision(pub["id"])[0]["revision"]
    p_before = c.get_current_revision(pub["id"])[1]
    changing = new_source(src_dir, "task6.tif", b"CHANGING" * (1 << 17))
    c.copy_chunk = 1 << 15
    calls = {"n": 0}

    def mutate_hook(_catalog):
        calls["n"] += 1
        if calls["n"] == 3:
            with open(changing, "ab") as f:
                f.write(b"EXTRA-DATA")

    c._copy_hook = mutate_hook
    try:
        try:
            c.publish("图层A", changing, "task6", META, rev_before)
        except cat.CatalogError as e:
            assert "复制" in str(e), e
        else:
            raise AssertionError("源文件变化时应复制失败")
    finally:
        c._copy_hook = None
        c.copy_chunk = cat.COPY_CHUNK
    assert c.get_current_revision(pub["id"])[0]["revision"] == rev_before
    assert sha(c.get_current_revision(pub["id"])[1]) == sha(p_before)
    assert not list(c.staging_dir.iterdir())

    # --- 7. 登记失败 -> 已改名的新 blob 回滚,旧版保留 ---
    final_rev = c.get_current_revision(pub["id"])[0]["revision"]
    final_p = c.get_current_revision(pub["id"])[1]
    s7 = new_source(src_dir, "task7.tif", b"REG-FAIL" * 1000)
    blobs_snapshot = set(os.listdir(c.blobs_dir))

    def failing_commit(*a, **kw):
        raise sqlite3.OperationalError("disk I/O error (模拟登记失败)")

    real_commit = c._commit
    c._commit = failing_commit
    try:
        try:
            c.publish("图层A", s7, "task7", META, final_rev)
        except sqlite3.OperationalError:
            pass
        else:
            raise AssertionError("应抛出版本登记失败")
    finally:
        c._commit = real_commit
    assert c.get_current_revision(pub["id"])[0]["revision"] == final_rev
    assert sha(c.get_current_revision(pub["id"])[1]) == sha(final_p)
    assert set(os.listdir(c.blobs_dir)) == blobs_snapshot, "登记失败不得留下新 blob"
    assert not list(c.staging_dir.iterdir())

    # --- 8. 崩溃恢复:已提交版本可查;未登记 blob 与 staging 被清理 ---
    pre = c.get_current_revision(pub["id"])[0]
    pre_sha = sha(c.get_current_revision(pub["id"])[1])
    orphan_blob = c.blobs_dir / "crashed-orphan.tif"
    orphan_blob.write_bytes(b"UNPUBLISHED" * 1000)
    leftover = c.staging_dir / "crashed.part"
    leftover.write_bytes(b"HALF-COPIED" * 1000)
    c2 = cat.BasemapCatalog(lib)  # 模拟重启:同目录新实例
    c2.initialize()
    assert not orphan_blob.exists(), "未登记 blob 应在重启后清理"
    assert not leftover.exists(), "未完成暂存应在重启后清理"
    info_r, path_r = c2.get_current_revision(pub["id"])
    assert info_r["revision"] == pre["revision"]
    assert sha(path_r) == pre_sha, "重启后仍是最近已提交的完整版本"
    assert {b["name"] for b in c2.list_basemaps()} == {"图层A", "并发新图"}
    assert len(list(c2.blobs_dir.iterdir())) == 6, "只保留已登记修订的文件(图层A 5 版 + 并发新图 1 版)"
    assert c2.get_revision(pub["id"], 1)[1].exists()

    # --- 9. 重启清理任务暂存不影响底图库 ---
    for p in src_dir.iterdir():
        shutil.rmtree(p) if p.is_dir() else p.unlink()
    s9 = new_source(src_dir, "task9.tif", b"SEPARATE" * 1000)
    c2.publish("图层B", s9, "task9", META)
    for p in src_dir.iterdir():
        shutil.rmtree(p) if p.is_dir() else p.unlink()
    c3 = cat.BasemapCatalog(lib)
    c3.initialize()
    assert {b["name"] for b in c3.list_basemaps()} == {"图层A", "并发新图", "图层B"}
    pub_b = next(b for b in c3.list_basemaps() if b["name"] == "图层B")
    assert c3.get_current_revision(pub_b["id"])[1].read_bytes() == b"SEPARATE" * 1000

    # --- 10. 校验与路径安全 ---
    expect_invalid(lambda: c3.publish("   ", s2, "t", META))
    expect_invalid(lambda: c3.publish("x" * 121, s2, "t", META))
    expect_invalid(lambda: c3.publish("bad\x00name", s2, "t", META))
    expect_invalid(lambda: c3.publish("图层C", s2, "", META))
    expect_invalid(lambda: c3.publish("图层C", s2, "t", {**META, "width": -1}))
    expect_invalid(lambda: c3.publish("图层C", s2, "t", {**META, "extent": [0, 0]}))
    expect_invalid(
        lambda: c3.publish("图层C", s2, "t", {**META, "extent": [0, 0, 10, float("nan")]})
    )
    expect_invalid(lambda: c3.publish("图层C", s2, "t", {**META, "crs": ""}))
    expect_invalid(lambda: c3.publish("图层C", tmp / "missing.tif", "t", META))
    expect_invalid(lambda: c3.publish("图层C", s2, "t", META, "1"))
    expect_invalid(lambda: c3.publish("图层C", s2, "t", META, -1))
    for bad in ("../etc/passwd", "a/b.tif", "", ".", ".."):
        try:
            c3.blob_path(bad)
        except cat.CatalogError:
            pass
        else:
            raise AssertionError(f"路径穿越应被拒绝: {bad!r}")

    # 名称大小写不同视为不同底图
    s10 = new_source(src_dir, "task10.tif", b"LOWER" * 1000)
    c3.publish("layer-a", s10, "task10", META)
    assert {b["name"] for b in c3.list_basemaps()} == {"图层A", "并发新图", "图层B", "layer-a"}

    # 发布期间被删除的源文件 -> 404/400 语义,不产生记录
    expect_invalid(lambda: c3.publish("图层D", tmp / "gone.tif", "t", META))
    assert not any(b["name"] == "图层D" for b in c3.list_basemaps())

    print("ALL CATALOG TESTS PASSED")


if __name__ == "__main__":
    main()
