# DEM 拼接服务(FastAPI + GDAL)

把相邻架次的高程栅格拼成统一底图:网页上传最多 **4 份带坐标系的单波段 GeoTIFF**,
调整覆盖顺序、设置目标像元边长(米,>0),后台统一重投影到 **EPSG:3857** 后镶嵌,
再转换为**带概览的 COG**。全部成功才开放下载,并显示范围与尺寸。

确认可用的拼接成果可发布到底图库(`/basemaps`),作为长期 GIS 底图保存,
服务重启后仍可查询、下载;底图库使用 SQLite 目录 + 不可变 COG 文件。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`samplegen` 服务会先生成演示样例(写入 `./data/samples`),完成后 `web` 自动启动。
浏览器中可直接勾选样例、调整顺序、提交拼接、取消任务、下载结果,无需准备数据。
任务成功后点击页头「底图库」即可发布长期底图,并可继续替换出新版本。

> 若无法拉取 `ghcr.io/osgeo/gdal`,把 Dockerfile 第一行改为
> `osgeo/gdal:ubuntu-small-3.9.2`(Docker Hub)。

## 演示样例(不同坐标系 / 重叠 / NoData)

| 文件 | CRS | 说明 |
|---|---|---|
| `dem_A_utm32n.tif` | EPSG:32632 | 2m,中部一块 NoData 空洞 |
| `dem_B_utm32n.tif` | EPSG:32632 | 与 A 重叠约 200m×500m,西侧 NoData 条带 |
| `dem_C_wgs84.tif` | EPSG:4326 | 经纬度(度),覆盖 A 西南角,含 NoData 空洞 |
| `dem_D_webmerc.tif` | EPSG:3857 | 与 B 东北角重叠,四周一圈 NoData 边缘 |

建议体验顺序:勾选 A、B、C、D 加入列表 → 像元边长 `2` → 开始拼接。
把 B 调到 A 之下/之上各跑一次,可看到**列表靠后的文件有效像元优先**,
而 NoData 不会盖掉已有高程。重新生成样例:`FORCE=1 docker compose run --rm samplegen`。

## 处理流水线

每个任务三个 GDAL 阶段,全部流式处理,**不把整幅栅格读入内存**:

1. **MOSAIC** — `gdalwarp -t_srs EPSG:3857 -tr <res> <res> -r bilinear -dstnodata -9999`
   多输入一次镶嵌:自动计算并集范围;输入按列表顺序依次写入,**后文件有效像元覆盖先文件**;
   各输入自带的 NoData 被识别为掩膜,不参与写入,因此不会盖掉已有高程。
2. **COG** — `gdal_translate -of COG -co COMPRESS=DEFLATE -co RESAMPLING=AVERAGE`,
   COG 驱动自动生成概览。
3. **METADATA** — `gdalinfo -json` 读取尺寸与地理变换,计算 EPSG:3857 范围(米)。

只有三个阶段全部成功,任务才进入 `DONE` 并开放下载。

## 并发、取消与资源回收

- 全局信号量限制 **2 个任务并行**,其余排队(`QUEUED`);取消/失败会释放槽位。
- 每个 GDAL 阶段是独立子进程,置于**独立进程组**(`start_new_session`)。
  取消时对该进程组发 `SIGKILL` 并 `await` 回收,只影响本任务,不影响另一并行任务。
- **阶段交接安全**:阶段启动前、子进程退出后、发布结果前都检查取消标志——
  取消后绝不启动下一阶段,也绝不发布迟到结果。
- 失败或取消后,先终止并回收本任务全部 GDAL 进程,再删除该任务暂存目录;
  成功任务保留 `result.tif` 供下载。服务重启时清空遗留暂存。

## 底图库(长期 GIS 底图)

页面:`http://localhost:8000/basemaps`。选择任一 `DONE` 任务,填写底图名即可发布;
底图库与任务暂存目录(`data/tasks`)完全分离,重启清理任务不会影响底图。

### 发布语义

- **首次发布**:底图名必须不存在;已存在则返回 `409`,原版不变。
- **替换**:请求必须携带调用方所见修订 `expected_revision`;与当前修订不符
  (他人已先替换)则返回 `409`,响应头 `X-Current-Revision` 给出当前修订,
  原版及其文件均不变。
- **同名并发发布**:进程内写锁 + `BEGIN IMMEDIATE` 短事务内的修订检查保证
  仅一份成功,其余 `409`;失败方复制/改名产生的临时文件全部回滚删除。
- **大文件复制不持数据库写锁**:COG 先分块复制到 `staging/`(常量内存),
  此期间列表、详情、下载照常进行;复制完成后才在短事务里登记元数据。
- **全部就绪才生效**:来源任务、范围、尺寸、CRS、大小、文件一起写入同一事务,
  提交前 blobs 已 fsync。复制、改名或登记任一步失败,旧版完整保留。
- **下载始终对应同一修订**:每个修订有唯一不可变文件,换版只改数据库指针;
  旧修订链接/在途下载不受影响,也不会混入新文件。
- **崩溃恢复**:提交是原子的;重启时清理 `staging/` 残留和未被任何修订引用的
  blob,随后提供最近一次完整提交的版本。进行中的发布会持有共享 `flock`,
  启动清理只拿独占非阻塞锁,有发布在途时跳过,避免误删。

### 存储布局

```
data/basemaps/catalog.sqlite3   目录数据库(WAL + synchronous=FULL)
data/basemaps/blobs/<随机>.tif  每个修订一个不可变 COG
data/basemaps/staging/          发布中的临时文件(启动时清理)
data/basemaps/.publish.lock     发布与启动清理的互斥 flock
```

修订历史目前永久保留(这正是"在途下载/旧链接始终可用"的保证);需要回收空间时,
应在确认无外部引用后离线删除旧 blob 及对应修订行。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 网页 UI |
| GET | `/api/samples` | 样例列表 |
| POST | `/api/tasks` | 创建任务(multipart:`resolution`、`plan` JSON、`files`) |
| GET | `/api/tasks` | 任务列表(状态/阶段/结果/日志尾) |
| POST | `/api/tasks/{id}/cancel` | 取消任务 |
| GET | `/api/tasks/{id}/download` | 下载 COG(仅 `DONE`,否则 409) |
| GET | `/basemaps` | 底图库页面 |
| GET | `/api/basemaps` | 底图列表(当前修订) |
| GET | `/api/basemaps/{id}` | 底图详情与修订历史 |
| POST | `/api/basemaps` | 发布/替换(multipart:`name`、`task_id`、可选 `expected_revision`) |
| GET | `/api/basemaps/{id}/revisions/{rev}/download` | 下载指定修订 COG |

`plan` 示例(按覆盖优先级从低到高):
`[{"type":"upload","index":0},{"type":"sample","name":"dem_B_utm32n.tif"}]`

## 目录结构

```
app/main.py          FastAPI 路由(上传校验、任务 API、底图库 API、下载)
app/taskmanager.py   任务调度:并发限制、三阶段流水线、取消与清理(不依赖 FastAPI)
app/catalog.py       底图库:SQLite 目录、不可变 blob、原子发布与崩溃恢复(不依赖 FastAPI)
app/static/index.html 拼接页 UI
app/static/basemaps.html 底图库页面
samples/generate_samples.py 样例生成(标准库 + gdal_translate)
tests/test_taskmanager.py  调度逻辑测试(GDAL 桩程序,无需真实 GDAL)
tests/test_catalog.py      底图库发布/替换/并发/清理测试
tests/test_catalog_crash.py 跨进程崩溃恢复测试
tests/test_basemap_api.py  底图库 HTTP API 测试(需 fastapi + python-multipart)
data/                运行期数据(样例 + 每任务暂存目录,Compose 自动创建)
```

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `/data` | 数据根目录 |
| `SAMPLES_DIR` | `$DATA_DIR/samples` | 样例目录 |
| `BASEMAPS_DIR` | `$DATA_DIR/basemaps` | 底图库目录(持久卷) |
| `MAX_PARALLEL` | `2` | 并行任务数 |

注意:任务注册表在进程内,uvicorn 必须单 worker(镜像默认如此)。

## 验证取消时的进程回收(容器内)

```bash
docker compose up --build -d
# 网页提交一个大任务后立即取消,然后:
docker compose exec web ps aux   # 不应残留 gdalwarp/gdal_translate 进程
ls data/tasks/                   # 被取消任务的目录已清理
```

## 本地测试

```bash
python3 -I tests/test_taskmanager.py   # 调度:无需 GDAL
python3 -I tests/test_catalog.py       # 底图库核心逻辑
python3 -I tests/test_catalog_crash.py # 发布中崩溃后的恢复
python3 -I tests/test_basemap_api.py   # 需安装 fastapi + python-multipart
```

调度测试覆盖:两任务并行与排队、取消运行中任务(killpg 参数与清理)、取消排队任务、
COG 阶段取消不发布结果、槽位释放。沙箱环境禁止真实信号时,测试用哨兵文件
模拟进程组终止;真实 `SIGKILL` 在 Docker 容器中生效。

底图库测试覆盖:首次发布/重名 409、替换须携带所见修订、过期修订 409 且原版不变、
多线程同名并发仅一份成功、12 MiB 文件复制期间查询与下载不阻塞、复制/登记失败回滚、
重启后清理未发布文件并保留最近提交版本、任务暂存清理不影响底图库;崩溃测试用子进程
在"文件已改名、事务未提交"的临界点直接 `_exit`,重启校验一致性。

## 手动验证底图库

```bash
docker compose up --build -d
# 1) 拼接页完成任一任务;2) 打开 http://localhost:8000/basemaps 发布
docker compose restart web
# 刷新底图库:底图仍在,可下载;data/basemaps/staging 为空
ls data/basemaps/blobs   # 每个修订一个文件
```
