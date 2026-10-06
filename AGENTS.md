# MoviePilot-Plugins（yyhoworks 个人插件仓库）

MoviePilot v3 架构的第三方插件仓库，同时用作 MP 插件市场源：MP 订阅仓库地址 `https://github.com/yyhoworks/MoviePilot-Plugins`（不是 raw package.json URL），自动读取 `package.v3.json` + `plugins.v3/`。

**当前工作焦点：`linkedmediadel`（媒体联动删除，v1.0）——由 MediaSyncDel 2.0.2 模块化重构并更名而来。**

## 1. 项目核心结构

- 插件主逻辑：`plugins.v3/linkedmediadel/`（多文件包，`class LinkedMediaDel(_PluginBase)`）
- 插件清单定义：`package.v3.json`（key = 插件 ID，必须与 `plugins.v3/<插件ID>/` 目录名一致）
- 图标：`icons/linkedmediadel.png`，清单里用 raw.githubusercontent.com 完整 URL 引用
- 开发日志：`DEVELOPMENT_PROGRESS.md`，新条目加在文件顶部
- 仓库内另有 CloudStrmHelper、CloudStrmButler 两个插件——**改 linkedmediadel 时不得改动它们的条目与文件**

### LinkedMediaDel 模块地图

| 文件 | 职责 |
|---|---|
| `__init__.py` | 插件元数据、`init_plugin`、四个事件入口（瘦壳，委托模块） |
| `payloads.py` | webhook 字段归一化：`normalize_bool`（布尔防线）、路径/时间戳/媒体类型归一化、`DeleteRequest`、三种入口的请求构建 |
| `transfer_query.py` | `query_transfer_history`：电影/剧集/季/集四分支查询组装 + 季集编号校验 |
| `deleter.py` | `SyncDeleter`：排除路径转发、删除执行、通知、历史落盘、空目录回收 |
| `torrents.py` | `TorrentCleaner`：`handle`（删种/停种判断）、`_del_seed`（辅种）、`_del_collection`（合集） |
| `views.py` | `build_form` / `build_page` 纯字典构建（详情页删除按钮 API 为 `plugin/LinkedMediaDel/delete_history`） |
| `tests/` | stubs.py（`app.*` 桩件）+ pytest 用例 |

事件入口（均在 `__init__.py` 的 LinkedMediaDel 上，`@eventmanager.register` 不外移）：
- `sync_del_by_webhook` — `EventType.WebhookMessage`，Emby `library.deleted`（顺带兼容 Jellyfin `ItemDeleted`），守卫 `_sync_type == "webhook"`
- `sync_del_by_plugin` — `EventType.WebhookMessage`，Scripter X `media_del`，守卫 `_sync_type == "plugin"` + `normalize_bool(item_isvirtual)`
- `sync_del` — `EventType.PluginAction`（`media_sync_del` 联动），直送 `SyncDeleter.execute`（无排除/身份前置校验，与历史行为一致）
- `downloadfile_del_sync` — `EventType.DownloadFileDeleted`，**不检查插件启用状态**（既有语义，勿擅自加守卫）
- `handle_torrent` 为保留的公开兼容入口，委托 `TorrentCleaner.handle`

## 2. 历史坑点（Critical Context）

- **核心触发源**：接收 Emby / Scripter X 发送的删除 Webhook 事件。
- **经典 Bug / 类型陷阱**：Emby 传入的 `item_isvirtual` 字段为字符串（如 `"False"`），MP 核心 `app/modules/emby/emby.py` 不做类型强转直接赋值。
  - **强制规则**：所有 webhook 布尔字段必须经过 `payloads.normalize_bool()`，禁止 `if value:` 隐式真值判断（`"False"` 恒真，曾致删除事件全被静默跳过，上游 thsrite/MoviePilot-Plugins issue #359）。
  - 归一化契约：真值集 `("true","1","yes","y","on")`、假值集 `("false","0","no","n","off")`；**None 与无法识别的值都返回 None，调用方必须走保护逻辑**（自动停用插件防误删），不得默认放行删除。

## 3. 技术规范

- **开发语言**：Python 3.10+
- **插件规范**：完全兼容 MoviePilot v3 插件接口协议——继承 `_PluginBase`（`from app.plugins import _PluginBase`），四个 eventmanager 注册入口保留在插件类上；SDK 导入走 `app.sdk.*`；模块间用相对导入（`from .payloads import ...`，CloudStrmButler 已验证该模式）。
- **版本控制与清单同步**：修改插件代码时，必须同步更新：
  1. `plugins.v3/linkedmediadel/__init__.py` 的 `plugin_version`
  2. `package.v3.json` 中 LinkedMediaDel 的 `version`（与 1 完全一致，否则 MP 一直误报「有更新」）
  3. `package.v3.json` 的 `history` 新增 `vX.Y.Z` 条目（author 为 Felix Yang）
  4. `DEVELOPMENT_PROGRESS.md` 顶部追加一节（含「验证」小节）
- **防御性编程**：
  - 删除前校验数据库（`TransferHistoryOper` / `DownloadHistoryOper`）与文件系统真实状态；外部清理失败保留整理历史。
  - 转移记录标题与删除媒体不符 → 防误删跳过；转移路径仍存在 → 跳过；整理时间晚于删除事件 → 忽略。
  - 每个跳过分支都必须有日志（历史教训：静默 return 零日志）。

## 4. 构建与测试

- LinkedMediaDel 测试：`python3 -m venv .venv && .venv/bin/pip install pytest`（仓库根，.venv 已 gitignore），然后 `cd plugins.v3/linkedmediadel && ../../../.venv/bin/python -m pytest`（pytest.ini 已配 `pythonpath=. testpaths=tests`）。
- 无 pytest 时的最低检查：`python3 -m py_compile plugins.v3/linkedmediadel/*.py` + `git diff --check`。
- 功能验证（不碰真实文件）：伪造 `media_del` webhook（`item_isvirtual='False'`、不存在的 tmdb_id）→ 插件日志出现「未获取到可删除数据」即证明进入删除逻辑。**注意：MP v3 把插件日志路由到独立文件 `config/logs/plugins/linkedmediadel.log`，主日志 `moviepilot.log` 里看不到插件输出**（已实测确认，勿再误判为静默失败）。伪造方法：`POST /api/v1/webhook/?token=<API_TOKEN>`，表单字段 `data` 为 JSON：`{"Event":"media_del","item_isvirtual":"False","item_type":"Movie","item_name":"__自检__","item_path":"/nonexistent/x.mkv","tmdb_id":"999999999"}`。
- 本机环境实测：系统 `python3` 无 pytest；有 `node` 无 pnpm。CloudStrmButler 的测试与前端构建与本插件无关，勿动。

## 生产机约束与挂起事项（2026-09-10 记录）

- **更名影响**：LinkedMediaDel 与旧 MediaSyncDel 是两个插件身份——旧插件的配置、插件历史不迁移；生产机上应卸载旧 MediaSyncDel 后安装 LinkedMediaDel 并重新配置（sync_type、排除路径、路径映射）。
- 生产 MP 更新只认本仓库；**不要从 thsrite 源重装 MediaSyncDel、不要点「重置插件」**。上游仍是 2.0.1 单文件，与本仓库结构已分叉，跟进上游改动时按语义对照 diff。
- 生产 MP 配置里指向 thsrite/MoviePilot-Plugins-Private 的 `REPO_GITHUB_TOKEN` 已 401 失效，待处理。
- 24 条 `transfersettlementreceipt` 孤儿行待清理。
- 可择机给 jxxghp/MoviePilot 提反馈：核心 `app/modules/emby/emby.py` 应把 `item_isvirtual` 归一化为 bool（zspace 模块同样问题）。
