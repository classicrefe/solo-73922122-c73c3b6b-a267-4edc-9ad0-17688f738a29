"""跨进程崩溃恢复测试:abort 正在登记的发布进程,验证重启后数据一致。

子进程真实地处于 publish() 的登记阶段(已把文件改名进 blobs/,尚未提交事务),
主进程收到"已改名"信号后 SIGKILL 之;随后新进程初始化同一目录,检查:
- 最近已提交版本仍是完整可读的;
- 未登记的 blob 与 staging 残片被清理;
- 崩溃当次的内容不会生效。

运行: python3 -I tests/test_catalog_crash.py
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP_DIR = ROOT / "app"

META = {"width": 10, "height": 10, "extent": [0, 0, 10, 10], "crs": "EPSG:3857"}

CHILD = r"""
import os, sys, time
sys.path.insert(0, sys.argv[1])
import catalog as cat
root, src, marker = sys.argv[2], sys.argv[3], sys.argv[4]
c = cat.BasemapCatalog(root)
c.initialize()
real_replace = cat.os.replace

def replace_then_hang(a, b):
    real_replace(a, b)
    # 文件已改名进 blobs/,事务尚未提交。写标记后子进程自行终止,
    # 模拟"发布过程中进程退出"(不改数据,效果等同于被外部 SIGKILL)。
    open(marker, "w").write(str(b))
    os._exit(137)  # 不执行任何收尾代码

cat.os.replace = replace_then_hang
c.publish("底图", src, "child-task", {"width": 10, "height": 10,
          "extent": [0, 0, 10, 10], "crs": "EPSG:3857"}, 1)
"""


def main():
    tmp = Path(tempfile.mkdtemp(prefix="catalog-crash-"))
    lib = tmp / "basemaps"
    src_new = tmp / "new.tif"
    src_new.write_bytes(b"NEW-COG" * 10000)
    src_crash = tmp / "crash.tif"
    src_crash.write_bytes(b"CRASH-COG" * 10000)

    sys.path.insert(0, str(APP_DIR))
    import catalog as cat

    # 先提交一个完整版本
    c = cat.BasemapCatalog(lib)
    c.initialize()
    pub, created = c.publish("底图", src_new, "task-good", META)
    assert created and pub["revision"] == 1
    _, good_path = c.get_current_revision(pub["id"])
    good_bytes = good_path.read_bytes()
    good_sha = __import__("hashlib").sha256(good_bytes).hexdigest()

    # 子进程开始替换并停在被改名、未登记的临界点
    marker = tmp / "renamed.marker"
    child = subprocess.Popen(
        [sys.executable, "-I", "-c", CHILD, str(APP_DIR), str(lib), str(src_crash), str(marker)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for _ in range(400):
        if marker.exists():
            break
        if child.poll() is not None:
            out, err = child.communicate()
            raise AssertionError(f"子进程提前退出: {err.decode('utf-8', 'replace')}")
        time.sleep(0.025)
    else:
        child.kill()
        raise AssertionError("未等到发布进程进入登记临界点")

    renamed_blob = Path(marker.read_text())
    assert renamed_blob.exists(), "崩溃前文件应已改名进 blobs/"
    child.wait(10)
    assert child.returncode != 0, "子进程应在登记提交前异常退出"

    # 重启:新实例初始化同一目录
    c2 = cat.BasemapCatalog(lib)
    c2.initialize()
    info, path = c2.get_current_revision(pub["id"])
    assert info["revision"] == 1, "崩溃未提交的版本不得生效"
    assert __import__("hashlib").sha256(path.read_bytes()).hexdigest() == good_sha, (
        "重启后必须仍提供最近已提交的完整版本"
    )
    assert not renamed_blob.exists(), "未登记的 blob 必须清理"
    assert not any(c2.staging_dir.iterdir()), "staging 残片必须清理"
    assert len(list(c2.blobs_dir.iterdir())) == 1
    assert c2.list_basemaps()[0]["source_task_id"] == "task-good"
    assert b"CRASH-COG" not in path.read_bytes()

    # 重启后仍能正常替换,且复制新版本可成功
    src3 = tmp / "third.tif"
    src3.write_bytes(b"THIRD-COG" * 1000)
    pub3, created3 = c2.publish("底图", src3, "task-third", META, 1)
    assert pub3["revision"] == 2 and created3 is False
    _, p3 = c2.get_current_revision(pub["id"])
    assert p3.read_bytes() == src3.read_bytes()

    print("ALL CATALOG CRASH TESTS PASSED")


if __name__ == "__main__":
    main()
