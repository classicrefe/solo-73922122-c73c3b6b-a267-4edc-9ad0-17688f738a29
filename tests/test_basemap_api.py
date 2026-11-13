"""底图库 API 端到端测试(真实 FastAPI TestClient;无 GDAL,用假任务)。

运行: /tmp/venvtest/bin/python -I tests/test_basemap_api.py
需要 fastapi + python-multipart;不依赖真实 GDAL。
"""
import hashlib
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

from fastapi.testclient import TestClient  # noqa: E402

import main as appmod  # noqa: E402
from taskmanager import STATUS_DONE, Task, TaskManager  # noqa: E402

META = {"width": 120, "height": 80, "extent": [100.0, 40.0, 340.0, 200.0], "crs": "EPSG:3857"}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main():
    tmp = Path(tempfile.mkdtemp(prefix="api-test-"))
    appmod.DATA_DIR = tmp
    appmod.TASKS_ROOT = tmp / "tasks"
    appmod.SAMPLES_DIR = tmp / "samples"
    appmod.BASEMAPS_DIR = tmp / "basemaps"
    appmod.TASKS_ROOT.mkdir(parents=True)
    appmod.SAMPLES_DIR.mkdir(parents=True)

    with TestClient(appmod.app) as client:
        mgr = appmod.manager
        cat = appmod.catalog

        # 构造两个"已完成"任务(替代真实 GDAL 流水线)
        def fake_task(tid: str, payload: bytes) -> Task:
            t = Task(id=tid, resolution=2.0, filenames=["a.tif"], workdir=mgr.tasks_root / tid)
            t.workdir.mkdir(parents=True)
            (t.workdir / "result.tif").write_bytes(payload)
            t.status = STATUS_DONE
            t.result = dict(META, file_size_bytes=len(payload))
            mgr.tasks[tid] = t
            return t

        ta = fake_task("t-aaa", b"COG-A" * 1000)
        tb = fake_task("t-bbb", b"COG-B" * 1000)
        tq = fake_task("t-queued", b"NOT-READY" * 100)
        tq.status = "QUEUED"

        # 页面
        assert client.get("/healthz").json() == {"ok": True}
        r = client.get("/basemaps")
        assert r.status_code == 200 and "底图库" in r.text
        assert client.get("/").status_code == 200

        # 空库
        assert client.get("/api/basemaps").json() == []

        # 非成功任务不能发布
        r = client.post("/api/basemaps", data={"name": "X", "task_id": "t-queued"})
        assert r.status_code == 409, r.text
        r = client.post("/api/basemaps", data={"name": "X", "task_id": "nope"})
        assert r.status_code == 404

        # 首次发布
        r = client.post("/api/basemaps", data={"name": "city-dem", "task_id": "t-aaa"})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["created"] is True
        bm = body["basemap"]
        assert bm["name"] == "city-dem" and bm["revision"] == 1
        assert bm["source_task_id"] == "t-aaa"
        assert bm["width"] == 120 and bm["height"] == 80
        assert bm["extent"] == META["extent"]
        bm_id = bm["id"]

        # 首次发布重名 -> 409,无 X-Current-Revision 也可;原版不变
        r = client.post("/api/basemaps", data={"name": "city-dem", "task_id": "t-bbb"})
        assert r.status_code == 409
        assert "已存在" in r.json()["detail"]
        assert client.get(f"/api/basemaps/{bm_id}").json()["revision"] == 1

        # 下载当前修订
        r = client.get(bm["download_url"])
        assert r.status_code == 200
        assert r.content == b"COG-A" * 1000
        assert "city-dem_r1.tif" in r.headers["content-disposition"]

        # 替换:无 expected_revision -> 409
        r = client.post("/api/basemaps", data={"name": "city-dem", "task_id": "t-bbb"})
        assert r.status_code == 409
        # 替换:过期修订 -> 409 且响应头给出当前修订
        r = client.post(
            "/api/basemaps",
            data={"name": "city-dem", "task_id": "t-bbb", "expected_revision": "2"},
        )
        assert r.status_code == 409 and r.headers["x-current-revision"] == "1"
        # 替换成功
        r = client.post(
            "/api/basemaps",
            data={"name": "city-dem", "task_id": "t-bbb", "expected_revision": "1"},
        )
        assert r.status_code == 200 and r.json()["created"] is False
        bm2 = r.json()["basemap"]
        assert bm2["revision"] == 2 and bm2["source_task_id"] == "t-bbb"

        # 旧修订链接下载原内容,新链接下载新内容
        r = client.get(f"/api/basemaps/{bm_id}/revisions/1/download")
        assert r.status_code == 200 and r.content == b"COG-A" * 1000
        r = client.get(f"/api/basemaps/{bm_id}/revisions/2/download")
        assert r.content == b"COG-B" * 1000
        # 不存在的修订
        assert client.get(f"/api/basemaps/{bm_id}/revisions/99/download").status_code == 404

        # 列表与详情
        listing = client.get("/api/basemaps").json()
        assert len(listing) == 1 and listing[0]["revision"] == 2
        detail = client.get(f"/api/basemaps/{bm_id}").json()
        assert [x["revision"] for x in detail["revisions"]] == [2, 1]
        assert client.get("/api/basemaps/9999").status_code == 404

        # 原始任务被清理(模拟 TaskManager._gc/重启),底图库不受影响
        import shutil
        shutil.rmtree(ta.workdir)
        shutil.rmtree(tb.workdir)
        mgr.cleanup_stale()
        r = client.get(f"/api/basemaps/{bm_id}/revisions/2/download")
        assert r.status_code == 200 and r.content == b"COG-B" * 1000
        # 源结果已不在:不能再从该任务发布
        r = client.post(
            "/api/basemaps",
            data={"name": "city-dem", "task_id": "t-aaa", "expected_revision": "2"},
        )
        assert r.status_code == 410, r.text
        assert client.get(f"/api/basemaps/{bm_id}").json()["revision"] == 2

        # 发布必须基于成功任务:排队/失败任务一律 409
        tf = fake_task("t-failed", b"X" * 10)
        tf.status = "FAILED"
        r = client.post("/api/basemaps", data={"name": "另一底图", "task_id": "t-failed"})
        assert r.status_code == 409
        assert client.get("/api/basemaps").json()[0]["name"] == "city-dem"

    # 重启:新应用实例(同一数据目录)仍能提供最近已提交版本
    from contextlib import asynccontextmanager
    # 直接重新初始化目录对象,模拟进程重启
    appmod.BASEMAPS_DIR = tmp / "basemaps"
    with TestClient(appmod.app) as client:
        listing = client.get("/api/basemaps").json()
        assert len(listing) == 1 and listing[0]["revision"] == 2
        assert client.get(listing[0]["download_url"]).content == b"COG-B" * 1000

    print("ALL BASEMAP API TESTS PASSED")


if __name__ == "__main__":
    main()
