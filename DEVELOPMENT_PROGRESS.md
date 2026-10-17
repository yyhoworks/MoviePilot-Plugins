---

# QBAutoClean v1.0.0 生产验证（2026-10-18）

## 验证

- 生产机经 source.install 安装（无需重启），配置启用：周期先 `*/2`（验证用）后 `*/15`（最终）、等待 30 分钟、豁免标签 `H&R,彩虹岛,保留`、下载器 `QB`。
- 演练模式：总数 8、豁免 1（彩虹岛 Zodiac）、候选 7，与预期一致；日志路由 `config/logs/plugins/qbautoclean.log`，全程无消息推送。
- 正式执行：删除积压 7 个任务（仅删任务、保留文件）；QB 剩余仅 Zodiac；逐一核对数据文件均保留（其中 MIDV-791-C.mp4 已由用户另行移至 `/sata2/特摄/宫下玲奈/MIDV-791/`，非本次删除所致）。
- 无操作轮次静默（候选 0）；真实推送核查 0 条；`/qbclean` 命令已注册且无重名。

---

# QBAutoClean v1.0.0 初始版本（2026-10-18）

## 背景

生产环境原由独立脚本 `qb_auto_clean.py` 配合 Agent 定时任务执行「QB 已完成任务自动清理」。Agent 定时任务的回合收尾文字会被平台强制作为消息推送、无法静默，已暂停。按用户要求改为静默插件：规则不变，只写插件日志与详情页摘要，不推送消息。

## 本轮更新

- 新增 `QBAutoClean` 插件（`plugins.v3/qbautoclean/`），定时删除 qBittorrent 已完成任务（仅删任务、保留数据文件）。
- 规则与旧脚本一致：完成时间超过 `delay_minutes`（默认 30 分钟）；豁免标签（默认 `H&R,彩虹岛,保留`）与豁免 Tracker 域名；可选仅处理 MoviePilot 任务、跳过暂停任务与单轮删除上限；支持多个下载器（默认全部已启用的 qBittorrent 实例）。
- 静默运行：周期执行仅记录插件日志与「最近一次运行」摘要（详情页可见）；`/qbclean` 命令可手动执行（`dry` 仅预览、`run` 强制执行）。
- 附带演练模式开关：开启后只记录将删除的候选、不实际删除。

## 验证

- `python3 -m py_compile plugins.v3/qbautoclean/__init__.py` 通过。
- 生产安装与演练/实跑验证记录见仓库后续提交。

---

# LinkedMediaDel v1.1.2 UI 恢复 v1.0 样式（2026-09-11）

## 本轮更新

- 按用户要求将 views.py 整体恢复为 v1.0（commit fbf307c）的表单与详情页样式：撤销 v1.1.0/v1.1.1 的界面改动（开关警示 hint、warning 条、字段 hint、alert 收敛、详情页统计行/100 条上限/路径行/卡片等高等）。
- 仅保留一处不可见的防御性修复：详情页排序对缺失 del_time 的历史记录用空串兜底（防 TypeError），不影响外观。
- v1.1.0 的全部功能修复（H1-H3/M1-M9/L1/L4/L6）不受影响。

## 验证

- `git show fbf307c:views.py` 恢复后仅排序一行有意的差异；`py_compile` 通过；pytest 91 条全绿。
- `plugin_version` 与 `package.v3.json` 均为 1.1.2。

---

# LinkedMediaDel v1.1.1 配置页与详情页排版修正（2026-09-11）

## 本轮更新

- 开关行：`del_source` 的长 persistent-hint 把整行撑高导致四个开关错位——移除该 hint（风险说明由下方 warning 条承担），`del_history` 长标签改为「清空历史记录」，一次性语义与「关闭删除源文件时仅删整理历史」并入 warning 条文案，开关行恢复等高。
- `library_path` 路径映射框 rows 2→4 并加 auto-grow（实际配置常见 3 行以上映射）。
- 详情页：路径行改 `text-caption` 小字号 + `word-break: break-all`（电影路径含完整长文件名不再撑破卡片）；VCard 加 `h-100` 统一卡片高度。

## 验证

- `python3 -m py_compile plugins.v3/linkedmediadel/views.py` 通过；pytest 91 条全绿（views 无测试覆盖，改动为纯字典文案/属性）。
- `plugin_version` 与 `package.v3.json` 均为 1.1.1。

---

# LinkedMediaDel v1.1.0 深度审查修复（2026-09-11）

## 本轮更新

- **审查方式**：对照 jxxghp/MoviePilot v3 分支真实源码逐条核实前提（`TransferHistory.list_by` 空 dest 跳过过滤、`str_to_timestamp` 失败返回 0 而非 None、`build_media_key` 输出 `tmdb:` 前缀等），关键场景实测复现后修复。
- **高危**：
  - 空目录回收越界（deleter `_remove_parent_dir`）：`shutil.rmtree` 改 `os.rmdir`（非空目录天然停止），新增终止白名单——路径映射目标根（payloads 新增 `library_mapping_dests`）、`os.path.ismount` 挂载点、文件系统根；保留三级上溯上限，各停止分支均有 debug 日志。修复前删除库内最后一部影片可上溯删掉媒体库根乃至挂载点内全部非媒体文件。
  - Season 0 特别篇退化整剧删除（transfer_query）：`if season_num:` 对 Emby 传入的 int 0 判假；改为 `is not None and str(...) != ""`，int 0 与字符串 "0" 统一按第 0 季查询。
  - 空 dest 命中全部版本（transfer_query 电影/单集分支）：Jellyfin ItemDeleted 场景 item_path 恒为空，真实 SQL 对空 dest 跳过过滤会命中该 media_id 全部记录；现为空路径记 warning 并拒绝执行。
- **中危**：时间解析失败退化 1970 导致永久静默跳过（payloads 改 `if not timestamp`）；Emby 原生整季删除兜底生效（Season 类型 episode 数字兜底为季号）；文件删除逐条 try/except，单条失败不炸穿批次且保留该条历史；转种后按目标任务 id（download_id）删种而非源 hash；无种子文件记录视为外部已清理（成功）而非失败；自动停用插件进程内即时生效（`self._enabled = False`）且保留全量配置键；源文件扩展名判断补 `.lower()`；排除路径归一化后按边界匹配（`/mnt/a` 不再命中 `/mnt/abc`，兼容 Windows 反斜杠配置）；路径映射改前缀边界单次替换且命中即停、行解析用 `rsplit(":", 1)` 兼容盘符（修掉中间误替换与多行级联）。
- **低危**：历史落盘加 `threading.Lock` 防并发丢记录并裁剪至最近 500 条；辅种递归加 visited 集合防循环引用爆栈；通知改用实际删除计数，0 条实际删除不发成功通知。
- **UI（views.py）**：`del_source` 加常驻警示 hint 并在开关行下新增 warning 级 VAlert；`del_history` 标签改为一次性动作语义；`exclude_path`/`library_path`/`sync_type` 补 placeholder 与 persistent-hint；底部 info alert 5 条收敛为 3 条（两条文档链接合并，URL 原样保留）；详情页只渲染最近 100 条并加统计行、排序容忍缺失 del_time、空状态文案补语义、卡片补路径展示、downloadfile 联动说明补「不受启用状态影响」。
- **测试桩件纠偏**（原桩件掩盖了 H3/M1 等 bug）：`str_to_timestamp` 失败返回 0；空 dest 跳过过滤；`MediaSource.__str__` 返回 value；`build_media_key` 输出 `tmdb:` 前缀；`get_data` 返回深拷贝。新增回归用例 39 条（tests/test_regressions.py 等），总用例 52 → 91，并对 11 处修复做变异验证（临时回退后对应测试如期失败）。

## 验证

- `cd plugins.v3/linkedmediadel && .venv/bin/python -m pytest`：91 条全绿（含 H1/H2/H3/M1/M2/M3/M5/M7/M8/M9/L1/L4/L6 回归用例）。
- `python3 -m py_compile` 全部模块通过；views.py 运行时冒烟（空态/统计行/100 条上限/None del_time）通过。
- `plugin_version` 与 `package.v3.json` 均为 1.1.0，history 已加 v1.1.0 条目。
- 遗留提示：未配置 `library_path` 时 H1 的「映射目标根」防线不存在，空目录上溯仅靠挂载点/非空/三级上限兜底，生产机建议补配路径映射。

---

# LinkedMediaDel v1.0 更名发布（原 MediaSyncDel 2.0.2 模块化重构）（2026-09-10）

## 本轮更新

- **身份更名**：插件 ID `MediaSyncDel` → `LinkedMediaDel`，显示名「媒体文件同步删除」→「媒体联动删除」，版本重置为 1.0，作者改署 Felix Yang；目录 `plugins.v3/linkedmediadel/`、配置前缀 `linkedmediadel_`、图标 `icons/linkedmediadel.png`、详情页 API 路径同步更换。MP 会将其视为全新插件：旧 MediaSyncDel 的配置与历史不迁移。
- 单文件（1280 行）拆分为多文件包：`payloads.py`（webhook 字段归一化 + `DeleteRequest`）、`transfer_query.py`（转移历史查询组装）、`deleter.py`（删除执行核心 `SyncDeleter`）、`torrents.py`（种子/辅种/合集 `TorrentCleaner`）、`views.py`（配置表单与详情页构建）；`__init__.py` 仅保留插件元数据、配置初始化与四个事件入口，方法体委托模块。
- `sync_del_by_webhook` / `sync_del_by_plugin` 的字段提取、排除路径、身份校验合并为共享流程 `_dispatch_request`。
- item_isvirtual 防线固化在 `payloads.normalize_bool`：字符串 `"False"` 判假（issue #359 回归防护）；`None` 与无法识别的值统一走防误删保护（自动停用插件），不再放行删除。
- 修复潜伏问题：`handle_torrent` 错误返回 `(False, False, 0)` 改为 `(False, False, [])`；`__del_seed` 遇残缺辅种记录裸 `return None`（会让调用方崩溃）改为跳过该条；`__del_collection` 异常 `print` 改为 `logger.error`；删除时间戳解析改为 None 安全（缺失/解析失败跳过时间比较）。
- 跳过分支补日志：事件类型不匹配 debug、虚拟 item 跳过 info；删除 `execute` 中不可达的身份兜底循环与无调用方的 `get_tmdbimage_url` / 类上 `format_timestamp`（逻辑移入 `payloads.parse_delete_time`）；保留公开方法 `handle_torrent` 作为 `TorrentCleaner.handle` 的兼容委托。
- 新增 `tests/`（stubs + 53 条用例）：入口级表征测试锁定 webhook/Scripter X/PluginAction 行为，模块级单测覆盖归一化、查询组装与种子清理。
- 范围说明：仅面向 Emby（原生 Webhook + Scripter X），Jellyfin 顺带兼容保持原样，不新增 Plex。
- 保留的既有语义（未改动）：`downloadfile_del_sync` 不检查插件启用状态（与上游一致）；路径映射按前缀替换；排除路径命中后转发 `networkdisk_del`；配置表单内 Scripter X 文档链接仍指向上游 thsrite 仓库（功能性引用）。

## 验证

- 重构前表征测试对旧单文件全绿（30 条），重构后及更名后同套测试全绿（53 条）：`.venv/bin/python -m pytest`（venv 内 pytest 9.1.1）。
- `python3 -m py_compile` 全部模块通过；`git diff --check` 通过；`plugin_version` 与 `package.v3.json` 均为 1.0。
- 生产走查（推送后）：换源刷新市场应出现「媒体联动删除」；安装启用后伪造 `media_del` webhook（`item_isvirtual='False'`、不存在的 tmdb_id）应出现「未获取到可删除数据」日志；本机已装文件与仓库 raw 文件 sha256 对比。

---

# V1.5.3 配置页映射规则增删与 SVG 对齐（2026-08-12）

## 本轮更新

- 配置页的映射规则编辑器按参考图重新整理为分类、CD2 挂载目录、实时监控、STRM 生成目录、OpenList 云盘目录和格式化模板。
- 保留一张始终可用的“新增映射规则”编辑卡片，进入配置页时默认展开，填写后随配置保存。
- 已有规则增加“删除此映射规则（保存后生效）”选项；保存时会压缩规则序号并清理旧的 `rule_i_*` 配置键。
- 继续同步写入旧版 `monitor_confs` 文本镜像，兼容已有配置和旧版本回退。

## 验证

- `python -m py_compile plugins.v3/cloudstrmhelper/__init__.py`：通过。
- `git diff --check`：通过。
- 规则增删序列化逻辑覆盖空规则、中间规则删除和旧版文本迁移场景。

# V1.4.3 监控页性能与映射规则迭代（2026-08-12）

## 修复内容

- 映射规则分类改为纯自定义标签输入，设置页只展示已有规则和一张“新增映射规则”卡片，不再预铺 1-5 空槽。
- 任务摘要不再深拷贝完整 `items`，页面首屏只取摘要和最多 100 条明细；任务详情接口只复制当前页明细。
- 任务历史新增明细保留策略：最近任务保留完整明细，旧任务只保留失败记录，单任务明细和文件体积均受限。
- 远程挂载目录改为后台线程探测并缓存“可访问 / 不可访问 / 检查中”状态，页面请求不再同步执行 `Path.is_dir()`。
- 监控页规则行可单独切换实时监控，目录不可访问时显示具体原因并跳过该规则的实时监控启动。
- 全量扫描已有互斥保护，被拒绝时补充日志；逐文件“已存在 / 创建成功 / 复制旁车”等日志降为 DEBUG，任务结束输出汇总耗时日志。

## 验证

- v3 py_compile 通过
- package JSON 解析校验通过
- 任务历史裁剪与自动写回冒烟测试通过

# V1.4.2 插件数据加载失败修复（2026-08-12）

## 修复内容

- 修复 MP v2/v3 下插件详情页“数据加载失败”：V1.4.1 引入的分类辅助方法仍引用旧类名 `CloudStrmCompanion`，实际类名已改为 `CloudStrmHelper`，导致 `get_page()` 抛 `NameError`，新版 MP 页面接口捕获异常后返回空对象。
- 全部 `CloudStrmCompanion` 引用统一修正为 `CloudStrmHelper`，覆盖分类解析、规则序列化、命令匹配和看板映射行渲染。

## 验证

- v3 py_compile 通过
- AST 检查无 `CloudStrmCompanion` 残留引用

# V1.4.1 布局对齐与交互稳定性修复（2026-08-12）

## 修复内容

- 首页双列布局：右栏改为 flex column，日志终端由固定 180px 改为自适应填充剩余空间，映射卡片固定不收缩。
- 任务中心统计栏：改为等宽深色 metric tile（总数/已处理/成功/跳过/失败），统一对齐。
- 最近任务列表：由 VList 改为固定列宽 VRow 网格（类型/时间/状态/重试/耗时/ID/详情），列宽合计 12。
- 页面溢出修复：页面根节点增加 overflow-x:hidden 与 scrollbar-gutter:stable，筛选切换不再抖动。
- 映射规则分类：VTextField 改为 VCombobox multiple + chips + smallChips，支持标签直接输入/删除；新增 __category_list/__category_string 兼容旧格式，看板映射行渲染多标签徽章，命令匹配支持标签级命中。

## 验证

- v3 py_compile 通过
- AST 解析通过，无重复函数定义
- API 11 个端点均正常注册
- 分类序列化往返测试通过

# CloudStrm 任务中心开发进度

更新时间：2026-08-12

## 一、项目目标

本轮改造围绕“任务中心优先”展开，解决以下问题：

- 配置项集中在一张长表单中，首次使用不易理解。
- 扫描结束后只能看日志或简单通知，无法快速知道成功、跳过和失败数量。
- 单个文件处理异常无法汇总到任务结果。
- 失败项没有明确的失败阶段和原因，也无法方便重试。
- 全量扫描、定向同步和失败重试之间缺少统一的任务记录。

发布版本统一升级为 `V1.1`。

## 二、当前已完成

### 1. 任务历史与结果模型

- 增加插件数据文件 `task_history.json`。
- 使用线程锁保护任务历史。
- 使用临时文件写入后 `os.replace`，保证历史文件原子替换。
- 对频繁进度更新做了节流保存，任务创建和结束时强制保存。
- 仅保留最近 30 条已结束任务，运行中的任务不参与清理。
- 插件重启时，历史中仍为 `running` 的任务会自动变为 `interrupted`。
- 中断原因记录为“插件重启时任务未完成”。

每条任务包含：

- `id`、`kind`、`status`。
- `created_at`、`started_at`、`finished_at`、`duration_seconds`。
- `scope`，包括路径、监控目录、源任务 ID 等信息。
- `stats`，包括 `discovered`、`processed`、`success`、`skipped`、`failed`。
- `items`，包括源文件、目标文件、动作、状态、阶段、原因、是否可重试和监控目录。

### 2. 统一任务执行链路

以下任务现在都进入统一后台任务执行器：

- 全量扫描 `full_scan`。
- 定向同步 `targeted`。
- 失败项重试 `retry`。

相关入口已经接入：

- 页面中的“立即全量扫描”。
- `onlyonce` 配置兼容入口。
- 原有 `/cloud_strm_companion` 命令。
- 原有 `/strm` 定向同步命令。
- 新增任务 API。

同一时间只允许一个显式全量、定向或重试任务执行。已有任务运行时，新的启动请求返回 `409`、当前任务 ID 和当前任务摘要。

实时文件监控仍然使用原来的文件级处理和通知聚合，不为每个实时文件创建独立历史任务，以避免任务记录膨胀。

### 3. 结构化文件处理结果

`__handle_file()` 现在返回结构化结果，并在任务上下文中自动归档。

当前统计口径：

- `success`：成功创建、覆盖或复制文件。
- `skipped`：目标已存在、内容未变化、文件类型未启用、文件正在处理等可预期情况。
- `failed`：模板错误、源文件不存在、权限错误、写入失败、复制失败等异常。

失败阶段已经细分为：

- `source`：源文件读取或源文件不存在。
- `mapping`：监控目录或目标映射错误。
- `format`：模板占位符错误。
- `generate`：STRM 文件生成或写入失败。
- `copy`：旁车文件或字幕复制失败。
- `push`：任务推送 URL 失败。
- `emby`：Emby 刷新失败。
- `queue`、`process`、`execution`：队列、处理或任务级异常。

已经保留的原有行为包括：

- STRM 原子写入。
- 已生成文件清单。
- 目录映射和路径替换。
- 旁车文件、字幕复制。
- 同步删除生成文件。
- Emby 刷新。
- 原有媒体入库通知。

### 4. 重试能力

- 重试会创建新的 `retry` 任务，不修改原任务。
- 新任务通过 `scope.source_task_id` 关联原任务。
- API 可以传 `item_ids`，未传时重试原任务全部可重试失败项。
- 生成、复制和源文件阶段的失败重试时会强制重新检查或覆盖目标。
- 模板 `format` 阶段失败重试同样会强制重新处理目标，避免目标 STRM 已存在时被错误记录为跳过。
- `push` 和 `emby` 阶段只重试对应下游阶段，避免不必要地重复生成文件。
- 支持单项重试。
- 支持全部可重试失败项批量重试。
- 页面新增失败项选择接口和复选框，可以选择部分失败项后批量重试。
- 选择状态只保存在当前插件实例内，不写入任务历史；插件重新初始化后会清空选择状态。

### 5. API

插件已经实现原生 MoviePilot 插件 API，使用当前宿主鉴权方式 `auth: bear`：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/tasks` | 获取当前任务和最近任务摘要，支持状态过滤 |
| `GET` | `/tasks/{task_id}` | 获取任务详情，支持 `success`、`skipped`、`failed` 过滤和分页 |
| `POST` | `/tasks` | 启动全量或定向任务，后台执行并立即返回任务 ID |
| `POST` | `/tasks/{task_id}/retry` | 单项或批量重试失败项 |
| `POST` | `/tasks/{task_id}/selection` | 选择或取消选择一个可重试失败项 |

已覆盖的错误场景包括：

- 任务 ID 不存在：`404`。
- 任务模式、分页参数、筛选状态或请求体格式错误：`400`。
- 目标路径不存在、不是监控目录或不符合全量扫描要求：`400`。
- 已有任务运行：`409`。
- 没有可重试失败项：`400`。

### 6. 任务中心页面

页面使用 MoviePilot 原生页面描述和 Vuetify 组件，当前包含：

- 当前任务状态。
- 进度条。
- 总数、已处理、成功、跳过、失败统计。
- 最近任务列表。
- 任务类型、状态、耗时、失败数和关联重试任务。
- 任务详情查看入口。
- 成功、跳过、失败筛选。
- 源路径、目标路径、动作、失败阶段和具体原因。
- 全部失败项重试。
- 失败项勾选后批量重试。
- 单项重试。
- 原任务与重试任务的关联信息。

页面事件路径已经统一使用相对路径，例如：

`plugin/CloudStrmHelper/tasks/{task_id}`

这是 MoviePilot 当前 `PageRender` 事件协议要求的形式。

MoviePilot 原生详情页的 `PageRender` 只会在按钮事件后重新加载，不能接收插件提供的自动刷新周期。因此：

- 插件详情页保留完整交互和手动“刷新任务状态/详情”入口。
- 仪表盘在有运行中任务时每 3 秒自动刷新。
- 仪表盘只展示只读的当前统计和最近 5 条任务摘要，不放置在仪表盘宿主中不会执行的交互按钮。

### 7. 配置页优化

`get_form()` 保留原有字段名和数据格式，调整为折叠分组：

- 基础设置：启用插件、实时监控、通知、立即执行。
- 文件处理：覆盖文件、旁车文件、字幕、同步删除。
- 目录映射：监控目录、媒体库路径映射、路径替换。
- 媒体格式：视频格式、非媒体格式。
- 媒体库与高级设置：Emby、URL 编码、任务推送 URL。

其他调整：

- 目录配置继续兼容 `#` 分隔格式。
- 路径替换推荐使用 `=>`，同时兼容旧的冒号格式和 Windows 盘符。
- 模板必须包含 `{local_file}` 或 `{cloud_file}`。
- 无效模板会在初始化时加入配置错误提示，并在任务中记录为 `format` 阶段失败。
- 即使插件当前关闭，配置重新保存时也会执行无副作用的目录配置校验，提前反馈目录格式、目录包含关系和模板占位符错误。
- `notify` 继续控制插件通知，同时新增任务完成摘要通知。关闭通知时，任务记录仍然保留。

### 8. 版本和发布目录

- v2 插件版本升级为 `V1.1`。
- v3 插件版本升级为 `V1.1`。
- `package.v2.json` 和 `package.v3.json` 已同步升级历史和版本信息。
- v2/v3 当前插件代码 SHA256 一致。
- 根目录旧版 `__init__.py` 没有修改。
- 根目录旧版 `package.json` 没有修改。

## 三、当前修改文件

当前工作区已修改或新增：

- `D:\Windows位置\Desktop\CloudStrmCompanion-1.3.4\MoviePilot-Plugins\plugins.v2\cloudstrmhelper\__init__.py`
- `D:\Windows位置\Desktop\CloudStrmCompanion-1.3.4\MoviePilot-Plugins\plugins.v3\cloudstrmhelper\__init__.py`
- `D:\Windows位置\Desktop\CloudStrmCompanion-1.3.4\MoviePilot-Plugins\package.v2.json`
- `D:\Windows位置\Desktop\CloudStrmCompanion-1.3.4\MoviePilot-Plugins\package.v3.json`
- `D:\Windows位置\Desktop\CloudStrmCompanion-1.3.4\MoviePilot-Plugins\DEVELOPMENT_PROGRESS.md`

当前没有提交 Git commit。不要使用 `git reset --hard` 或回滚未由本轮产生的改动。

## 四、已完成的验证

已完成：

- v2/v3 Python 语法编译检查。
- Python 全量编译检查。
- AST 解析和函数重复名检查。
- `git diff --check`。
- v2/v3 文件内容一致性检查。
- 任务历史原子保存和加载检查。
- 插件重启后 `running` 任务转为 `interrupted` 的隔离检查。
- 配置页、API 基础错误、非法路径和任务状态的隔离检查。
- 本轮选择接口、页面复选框和 API 路由的静态检查。
- 隔离回归：新建 STRM、目标已存在跳过、模板失败、旁车成功/未变化/复制异常。
- 隔离回归：Emby 刷新失败阶段、`partial`/`failed` 状态、单项和批量重试、原任务不可变。
- 隔离回归：并发任务 `409`、历史保留 30 条、重启任务转 `interrupted`。
- MoviePilot v3 后端和前端源码契约核对：插件 API 鉴权、相对 API 路径、详情页事件刷新和仪表盘周期刷新。

当前验证结果：

- `python -m py_compile plugins.v2/cloudstrmhelper/__init__.py plugins.v3/cloudstrmhelper/__init__.py`：通过。
- `python -m compileall -q plugins.v2/cloudstrmhelper plugins.v3/cloudstrmhelper`：通过。
- 隔离任务中心回归：通过。
- 配置校验与只读仪表盘结构检查：通过。
- v2/v3 SHA256：一致。
- 工作区差异检查：通过。

## 五、尚未完成和已知风险

### 1. 还没有完整 MoviePilot 服务联调

当前环境没有完整的 MoviePilot 运行时，因此还没有最终确认：

- `get_api()` 是否在目标 MoviePilot 版本中成功挂载。
- `Body` 参数在目标 v2/v3 运行环境中的实际解析行为。
- 页面事件执行后是否自动刷新页面。
- `VCheckbox` 的点击事件在宿主实际页面中是否符合预期。
- 仪表盘刷新和插件详情页刷新在目标前端版本中的最终表现。

### 2. 真实宿主回归还需要补齐

核心隔离回归已经完成。以下项目仍需要在真实 MoviePilot 环境中完整跑完并保留结果：

- 新建 STRM 文件记录为成功。
- 目标已存在且未开启覆盖时记录为跳过。
- 模板缺少占位符时记录为失败。
- 旁车复制成功、未变化和复制异常的统计。
- Emby 刷新失败记录 `emby` 阶段和具体原因。
- 混合成功、跳过、失败时状态为 `partial`。
- 全部失败时状态为 `failed`。
- 页面复选框选择后是否正确触发 `/selection`，再由“重试已选失败项”提交选中 ID。
- 详情页的按钮操作后是否会按宿主预期重新加载任务页面。
- 仪表盘在运行时每 3 秒刷新，任务结束后停止刷新。
- API 的 Bearer 鉴权和请求体解析。
- v2/v3 在各自目标 MoviePilot 运行环境中的行为一致。
- `/strm` 命令和实时监控不回归。

### 3. 配置即时校验仍可加强

目前模板和目录配置错误主要在插件启用或 `onlyonce` 初始化时解析。若插件完全关闭，保存错误配置后页面可能只能显示静态提示，具体错误不一定立即生成。后续可以将纯配置解析抽成无副作用校验函数，在 `get_form()` 或配置保存入口统一调用。

### 4. JSON 历史适合当前规模，但不是长期高并发方案

当前保留 30 条任务、单实例执行，JSON 足够简单。未来如果任务明细规模明显增大，建议考虑 SQLite 或宿主已有持久化能力，并保留现有 API 数据结构，降低迁移影响。

## 六、下一阶段规划

### 阶段 A：完成回归测试

1. 将当前临时隔离夹具整理为可重复运行的测试方案，或在后续允许时加入正式测试目录。
2. 补充字幕、任务推送 URL 和 API 参数分页的隔离断言。
3. 持续执行 v2/v3 同源行为检查。

### 阶段 B：完成真实宿主联调

1. 安装到测试 MoviePilot 实例。
2. 确认四个任务 API 和选择 API 实际挂载。
3. 通过页面点击验证全量扫描、详情、筛选和重试。
4. 验证运行中的刷新频率和任务完成后的停止刷新。
5. 验证通知正文、跳转链接和原有媒体入库通知没有互相替代。

### 阶段 C：收紧配置体验

1. 抽出独立配置校验函数。
2. 在保存配置前校验目录段数、路径关系、模板占位符和路径映射。
3. 对错误配置给出字段级提示。
4. 增加任务中心中的“配置错误”入口或提示。

### 阶段 D：发布准备

1. 更新 README 和变更日志。
2. 对比 v2/v3 发布包内容。
3. 清理测试产生的临时数据和缓存文件。
4. 再次执行语法、差异、回归和页面联调检查。
5. 确认后再提交 Git commit 和发布。

### 阶段 E：后续产品优化

- 任务详情支持更可靠的服务端分页和按阶段筛选。
- 增加任务范围搜索和失败原因聚合。
- 增加任务取消入口，并明确取消与插件重启中断的区别。
- 在任务通知中增加任务详情深链接。
- 当 JSON 任务明细增长到明显影响读写时迁移到 SQLite。
- 根据实际用户反馈调整默认配置和错误提示。

## 七、给下一位模型的交接说明

请先阅读本文件，再检查以下两个实际维护文件：

- `D:\Windows位置\Desktop\CloudStrmCompanion-1.3.4\MoviePilot-Plugins\plugins.v2\cloudstrmhelper\__init__.py`
- `D:\Windows位置\Desktop\CloudStrmCompanion-1.3.4\MoviePilot-Plugins\plugins.v3\cloudstrmhelper\__init__.py`

接手时遵守：

- 只修改发布目录下的 v2/v3 实现，根目录旧版暂不纳入。
- v2 和 v3 的行为保持一致，编辑后检查 SHA256 或逐段 diff。
- 不要删除用户已有改动，不要使用破坏性 Git 回滚。
- 优先完成“五、尚未完成和已知风险”中的回归和真实宿主联调。
- 修改任意一份插件代码后，必须同步另一份并运行 `py_compile`。
- 完成前检查 `git status --short`，确认没有误改根目录旧版或产生不需要发布的文件。

## 八、建议的下一步启动提示词

可以把下面这段直接交给新的模型：

> 请先阅读 `DEVELOPMENT_PROGRESS.md`，然后在 `D:\Windows位置\Desktop\CloudStrmCompanion-1.3.4` 继续 CloudStrm 任务中心开发。当前已完成任务历史、统一任务执行器、API、页面、失败重试和配置分组；下一步优先完成隔离回归测试和 MoviePilot 真实宿主联调。只修改 `MoviePilot-Plugins/plugins.v2/cloudstrmhelper/__init__.py`、`plugins.v3/cloudstrmhelper/__init__.py` 及确有必要的发布元数据，保持 v2/v3 一致，不要修改根目录旧版 `__init__.py`。先检查工作区现有改动，不要回滚。

---

# V1.2 界面与交互打磨（2026-08-11 第二轮）

本轮只做 UI/UX 迭代，不改任务逻辑。版本号 V1.1 → V1.2，v2/v3 插件代码保持逐字节一致。

## 渲染器机制结论（MoviePilot-Frontend 源码确认）

- `PageRender.vue`：`props` 原样 `v-bind`；`events` 的键是任意事件名，触发后调 API 并 `emit('action')` 让宿主整页重取 `get_page`；支持 `html`（`v-html`）与 `slots`；事件回调拿不到事件负载，参数只能是静态值。
- `FormRender.vue`：任何带 `model` 属性的组件都会 `v-model` 绑定到表单数据，默认值字典可预置初值，整个表单数据会保存为插件配置。
- `DashboardRender.vue`：纯展示，无事件；只读统计 + 宿主按 `refresh` 周期刷新是唯一更新手段。

## 配置表单（get_form）

- `VExpansionPanels` 增加 `model: _panel_open`：首组默认展开、展开状态随配置保存记忆；存在配置错误时默认值变为 `[0, 2]`，自动展开「目录映射」。`_panel_open` 会被存入配置但不被 `init_plugin` 读取，`__update_config` 重写配置时丢弃并回落默认，属预期行为。
- 配置错误 alert 从「目录映射」面板内移到表单顶部常显；模板警告留在面板内。
- 5 个面板标题加 mdi 图标（mdi-cog / mdi-file-sync / mdi-folder-sync / mdi-format-list-bulleted / mdi-server）。
- 目录配置帮助 alert 改用 `html` 分行排版，路径示例套 `<code>`。

## 任务中心页（get_page）

- 状态 chip 颜色动态化：running→info、success→success、partial→warning、failed→error、interrupted→grey、空闲→默认色。
- 空闲时隐藏进度条，统计区改显示最近一次已结束任务的数据（行首加「最近任务结果」chip），无历史则隐藏统计行。
- 「立即全量扫描」在任务运行中禁用且文案追加「（运行中）」。
- 「最近任务」由 VDataTable + 详情列表双结构合并为单个富列表：彩色状态 chip + 摘要 + 行内「详情」按钮，最多 10 条，有关联重试时追加计数 chip；空历史显示引导 alert。
- 「任务详情」从折叠面板改为常显卡片；筛选按钮有服务端驱动的选中态（当前筛选 flat/primary，其余 text）；明细表来源/目标列用 `__shorten_path` 尾部截断（`…/父目录/文件名`），并增加 `noDataText`；无可重试失败项时显示 success 色调提示而非空列表项。

## 仪表盘（get_dashboard）

- 状态 chip 用同一套颜色映射；任务运行时增加 `VProgressLinear`（discovered 为 0 时 indeterminate），空闲时隐藏；3 秒刷新逻辑不变。

## 图标与发布元数据

- 新增 `icons/cloudstrm.png` 专属图标（用户提供的最终设计：云 + 胶片 + 播放键，512×512）。
- 两个插件文件的 `plugin_icon` 改为自己仓库的 raw 地址，`plugin_version` 改为 `V1.2`。
- `package.v2.json`、`package.v3.json` 同步版本/图标/history；本轮同时更新了仓库根的 legacy `package.json`（V1.0 → V1.2，补齐 V1.1/V1.2 history），与 V1.1 轮“未修改根 package.json”的状态不同，特此说明。

## 本轮验证

- `py_compile` v2/v3：通过；两文件 MD5 一致。
- 打桩 MoviePilot 依赖后的隔离冒烟测试 39 项全部通过：表单（默认展开、错误置顶、图标、html 帮助）、任务中心（空历史/空闲/运行中三态、富列表、筛选选中态、路径截断、重试按钮）、仪表盘（进度条件渲染、状态色、刷新周期）、`__shorten_path` 边界。
- 待真实宿主回归项与第五节相同，另需确认：`_panel_open` 双向绑定在宿主表单中的实际表现、面板标题 content 子组件渲染、详情卡片常显后的页面高度体验。

## 本轮修改文件

- `plugins.v2/cloudstrmhelper/__init__.py`、`plugins.v3/cloudstrmhelper/__init__.py`（内容一致）
- `package.v2.json`、`package.v3.json`、`package.json`
- `icons/cloudstrm.png`（新增）
- `DEVELOPMENT_PROGRESS.md`

---

# V1.2.1 表格渲染修复（2026-08-12）

## 问题

V1.2 实机截图发现「任务详情」明细表只渲染了分页 footer（“1-10 of 100”），表头和数据行完全缺失。根因：MoviePilot 的 `PageRender.vue` / `DashboardRender.vue` 对每个组件都会渲染 `{{ config?.text }}` 和 content 子组件，即始终注入默认插槽（default slot）；Vuetify 的 `VDataTable` 一旦检测到默认插槽就跳过自带的 thead/tbody 渲染。v2/v3 前端的 PageRender 实现一致，均受影响；V1.1 引入的两处 VDataTable 从未真正显示过行数据。

## 修复

- 新增静态辅助 `__render_table_html(headers, rows, empty_text)`：输出 Vuetify 风格的紧凑 HTML 表格（`v-table v-table--density-compact` 等类名），单元格经 `html.escape` 转义，空数据时输出 colspan 占位行。
- `get_page`：明细表 VDataTable 节点替换为 `{"component": "div", "html": ...}`；超过 100 条截断时追加 `text-caption` 提示「仅显示前 100 条，共 N 条，可点击上方筛选按钮缩小范围」。
- `get_dashboard`：最近任务表同样替换为 HTML 表格，空态显示「暂无任务记录」。
- 版本号 V1.2 → V1.2.1，三个 package JSON 同步 version 并追加英文 history。

## 渲染器机制结论（补充 V1.2 结论）

- 在 `get_page` / `get_dashboard` 返回的组件树中不要使用 `VDataTable` / `VDataTableVirtual` / `VDataTableServer`——渲染器始终注入默认插槽会抑制其内置表头/表体。需要表格时用 `html` 字段输出 `<table>`（可用 v-table 类名获得原生外观）。
- 本结论已用 Vue 3.5.13 + Vuetify 3.7.3 CDN + 忠实复制的 PageRender 组件在本地复现验证（空表），替换为 html 表格后截图确认表头 6 列、数据行正常、路径截断生效。

## 本轮验证

- `py_compile` v2/v3 通过，两文件 MD5 一致（A689150337A1340A4A52831A860629A9）。
- 隔离冒烟测试 46 项全部通过（新增 html 表格结构、转义、空态、截断提示、101 行上限等断言）。
- 本地 Vuetify 环境截图比对：修复前空表（仅 footer），修复后完整表格。

## 本轮修改文件

- `plugins.v2/cloudstrmhelper/__init__.py`、`plugins.v3/cloudstrmhelper/__init__.py`（内容一致）
- `package.v2.json`、`package.v3.json`、`package.json`
- `DEVELOPMENT_PROGRESS.md`

---

# V1.3.0 OpenList + CD2 监控台（2026-08-12）

## 背景

用户提供新版前端设计稿，并明确移动云盘没有官方 API：监控不采用网盘 API 轮询，固定使用 OpenList + CD2 挂载目录监控，STRM 内容由 OpenList 地址模板生成。本轮只改描述、配置页文案、任务页和仪表盘展示，不改变扫描、监控、STRM 生成和任务逻辑。

## 配置页（get_form）

- 表单顶部固定提示监控方式为 OpenList + CD2，并说明移动云盘无官方 API、不提供网盘 API 轮询。
- 目录配置帮助、折叠面板标题和实时监控开关文案全部改为 OpenList + CD2 口径。

## 任务中心页（get_page）

- 新增「OpenList + CD2 监控台」：监控状态、OpenList 配置状态、监控目录数、本地 STRM 总数，以及最近任务成功/跳过/失败统计。
- 新增「路径监控与 STRM 映射策略」卡片，用 HTML 表格展示分类、状态、本地 STRM 输出目录、移动云盘路径和 OpenList 模板。
- 新增「最近同步明细」卡片，取当前或最近任务的前 8 条记录。
- 监控台与路径映射/同步明细改为左右两栏布局，参考设计稿的左侧状态策略、右侧映射表格与实时日志结构。

## 仪表盘（get_dashboard）

- 状态 chip 改为显示 OpenList + CD2 监控状态。
- 新增监控目录数量 chip。

## 版本与发布元数据

- `plugin_version` 和三个 package JSON 统一升级为 `V1.3.0`。
- 插件描述和 README 首段补充无官方 API 的移动云盘、CD2 挂载监控和 OpenList STRM 说明。

## 本轮验证

- v2/v3 `py_compile` 通过，SHA256 一致。
- 三个 package JSON 解析通过，版本均为 `V1.3.0`。
- `git diff --check` 通过。

---

# V1.3.1 Dashboard 对齐 SVG 样板（2026-08-12）

## 重构目标

用户反馈 V1.3.0 页面与提供的前端设计稿差别较大，本轮按 SVG 样板重构任务页：保留 OpenList + CD2 监控语义，同时把平铺表格改为现代 Dashboard 布局。

## get_page 结构调整

- 顶部 Header：插件标题、OpenList + CD2 监控状态 Badge、OpenList 状态、查看日志与保存配置按钮。
- 中间改为 `md=4 / md=8` 双栏：左栏为账号与监控指标 + 策略控制面板，右栏为路径映射卡片 + 180px 实时日志终端。
- 指标卡片改为深色 Mini Card，展示本地 STRM 总数与已跳过/失败数量。
- 策略面板使用只读 Switch 展示启用插件、实时监控、入库通知、同步删除和字幕复制状态。
- 路径映射改为卡片数据行：分类 Badge、本地输出路径 → 网盘路径、悬停显示编辑/删除图标，不再使用 Data Table。
- 实时日志终端背景 `#0b0f19`，固定高度 180px，渲染最近同步明细并带颜色标签。

## 样式

- 深色主色 `#0b0f19` / `#111827`，卡片 `#1f2937`，边框 `#374151`。
- 成功 `#10b981`、主按钮/高亮 `#0284c7` / `#38bdf8`、失败/删除 `#f43f5e`。
- 映射行 hover 显示操作图标，终端使用等宽字体。

## 版本

- `plugin_version` 和三个 package JSON 升级为 `V1.3.1`，history 增加 V1.3.1 说明。
- v2/v3 保持逐字节一致。

---

# V1.4.0 交互式 Dashboard 与结构化映射规则（2026-08-12）

## 背景与重构目标

用户提供了新的 SVG 设计稿（深色“中国移动云盘 STRM 助手”Dashboard），要求任务页不仅外观对齐，操作逻辑也要真正可用。移动网盘仍无官方 API，后端维持 OpenList + CD2 方案不变，设计稿中的 Token 状态元素对应改造为 OpenList / CD2 状态展示。本轮只改 `MoviePilot-Plugins` 内维护的 v2/v3 插件，仓库根目录的旧版 `CloudStrmCompanion`（thsrite 分支 v1.3.5）保持不动。

## 暂存式配置模型（未保存更改）

- 新增 `self._staged_config`：任务页上的策略开关、删除映射、移除扩展名都只写入暂存层，不直接落盘。
- Header 出现橙色“有未保存更改”Badge；「保存配置」走 POST `/config/save`（`update_config` + `init_plugin` 生效），「放弃更改」走 `/config/discard` 丢弃暂存。
- 页面所有展示读取 生效值 = 已保存配置 ⊕ 暂存改动，被暂存覆盖的开关行显示“待保存”标签。
- `__effective_bool` 统一处理生效值解析，避免字符串 "false" 被当作 True 的老坑。

## 结构化映射规则配置

- 新增配置键 `rule_{i}_category/local/strm/cloud/format/monitor`，设置页表单改为规则卡片编辑器（至少 4 槽、当前条数 +2、上限 12 槽）。
- `__rules_to_monitor_confs` 把结构化规则合成为旧版 `monitor_confs` 文本，喂给保持不变的原解析器；同时回写旧文本，降级到旧版本插件不丢配置。
- 设置页移除旧的 `monitor_confs` 大文本框，避免两套编辑入口互相覆盖。

## 新增运行时能力

- 新增 `cron_enabled`（默认开）与 `scan_interval`（默认 30 分钟）：注册间隔任务 `__scheduled_scan`，周期巡检并在事件流中记录 POLL 事件。
- 新增 `_live_events` 队列（deque，150 条）与 `__log_event`，事件类型 STRM-GEN / FAIL / PRUNE / TASK / POLL，任务页日志终端实时渲染。
- 死链清理累计数 `_pruned_total` 持久化到 `stats.json`，重启不丢。

## 新增 API（均 bear 鉴权）

- POST `/config/toggle` {key}：白名单 `_config_toggle_keys` 内的策略键暂存取反（仅 `cron_enabled` 默认 True）。
- POST `/config/save` / `/config/discard`：保存或放弃暂存配置。
- POST `/mappings/delete` {index}：暂存删除第 i 条映射。
- POST `/mappings/edit` {index}：记录 `_page_editing_rule` 并提示前往设置页编辑（MoviePilot PageRender 无法在任务页内嵌表单）。
- POST `/extensions/remove` {ext}：暂存移除扩展名，拒绝删到只剩最后一个。

## get_page 重建

- Header：SVG Logo、插件标题、OpenList + CD2 状态 chip（修复了 `__monitor_status()` 自带前缀导致的“OpenList + CD2 OpenList + CD2 监控中”重复文案）、OpenList 状态 chip、查看日志 / 保存配置 / 放弃更改按钮。
- 左栏：KPI 指标卡（本地 STRM 总数、已跳过/失败、死链清理累计）、OpenList + CD2 状态、三个策略开关行（VSwitch 真实可点、暂存后显示“待保存”）、可移除扩展名 VChip 组、「立即全量同步」按钮。
- 右栏：映射规则数据行（分类 Badge、本地路径 → 网盘路径、悬停显示编辑/删除图标）+ 实时日志终端（`#0b0f19` 背景、column-reverse 最新事件置顶）。
- 任务中心移入折叠的 VExpansionPanels，保持可用但不占首屏。
- 删除 V1.3.1 遗留的死代码 `__mapping_rows_html`。

## 机制结论与排障记录

- 曾怀疑策略开关暂存后状态不刷新：复核为旧截图缓存，`__effective_bool` + Vuetify 3 `modelValue` 绑定行为正确，重新生成预览确认开关 1 灰（关）、2/3 蓝（开）。
- 真实宿主待验证项：VSwitch 点击事件、VChip `click:close` 事件、PageRender 内 VExpansionPanels 表现、结构化规则表单在真实宿主中的保存链路。

## 本轮验证

- v2 `py_compile` 通过；v2/v3 SHA256 完全一致（`B0DC6DD8…F388D113`）。
- 冒烟测试 52/52 全部通过（含 API 注册、暂存读改写、死链计数持久化、无重复函数定义）。
- headless Chrome 整页截图核对：Header chips、KPI 行、策略面板状态、映射行、事件终端、折叠任务中心均符合设计稿。
- `git diff --check` 通过；根目录旧版插件与 `requirements.txt` 未改动。

## 修改文件

- `plugins.v2/cloudstrmhelper/__init__.py`、`plugins.v3/cloudstrmhelper/__init__.py`（逐字节一致）。
- `package.json`、`package.v2.json`、`package.v3.json`：版本统一 `V1.4.0`。
