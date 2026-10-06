"""媒体联动删除插件入口：事件接收与配置管理，删除执行委托给 deleter/torrents 模块。"""

from typing import Any, Dict, List, Tuple

from app import schemas
from app.db.oper.downloadhistory import DownloadHistoryOper
from app.db.oper.transferhistory import TransferHistoryOper
from app.plugins import _PluginBase
from app.schemas.types import EventType
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.services import DownloaderHelper

from . import payloads, views
from .deleter import SyncDeleter
from .torrents import TorrentCleaner


class LinkedMediaDel(_PluginBase):
    """根据媒体服务器删除事件同步清理整理历史、源文件和下载任务。"""

    # 插件名称
    plugin_name = "媒体联动删除"
    # 插件描述
    plugin_desc = "同步删除历史记录、源文件和下载任务。"
    # 插件图标
    plugin_icon = "linkedmediadel.png"
    # 插件版本
    plugin_version = "1.1.2"
    # 插件作者
    plugin_author = "Felix Yang"
    # 作者主页
    author_url = "https://github.com/yyhoworks"
    # 插件配置项ID前缀
    plugin_config_prefix = "linkedmediadel_"
    # 加载顺序
    plugin_order = 9
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _sync_type: str = ""
    _notify = False
    _del_source = False
    _del_history = False
    _exclude_path = None
    _library_path = None
    _transferhis = None
    _downloadhis = None
    _default_downloader = None
    _torrents = None
    _deleter = None

    def init_plugin(self, config: dict = None):
        """读取配置并重建本次运行所需的宿主服务句柄。"""
        config = config or {}
        downloader_helper = DownloaderHelper()
        self._transferhis = TransferHistoryOper()
        self._downloadhis = DownloadHistoryOper()
        self._enabled = bool(config.get("enabled"))
        self._sync_type = config.get("sync_type") or "webhook"
        self._notify = bool(config.get("notify"))
        self._del_source = bool(config.get("del_source"))
        self._del_history = bool(config.get("del_history"))
        self._exclude_path = config.get("exclude_path") or ""
        self._library_path = config.get("library_path") or ""
        self._default_downloader = None

        # 获取默认下载器
        downloader_services = downloader_helper.get_services()
        for downloader_name, downloader_info in downloader_services.items():
            if downloader_info.config.default:
                self._default_downloader = downloader_name

        # 删除执行所需的协作对象
        self._torrents = TorrentCleaner(
            chain=self.chain,
            downloadhis=self._downloadhis,
            data_store=self,
            default_downloader=self._default_downloader,
        )
        self._deleter = SyncDeleter(
            transferhis=self._transferhis,
            torrents=self._torrents,
            chain=self.chain,
            eventmanager=self.eventmanager,
            data_store=self,
            post_message=self.post_message,
            del_source=self._del_source,
            notify=self._notify,
            library_path=self._library_path,
            exclude_path=self._exclude_path,
        )

        # 清理插件历史
        if self._del_history:
            self.del_data(key="history")
            self.update_config({
                "enabled": self._enabled,
                "sync_type": self._sync_type,
                "notify": self._notify,
                "del_source": self._del_source,
                "del_history": False,
                "exclude_path": self._exclude_path,
                "library_path": self._library_path
            })

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        :return: 命令关键字、事件、描述、附带数据
        """
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "删除订阅历史记录",
                "response_model": schemas.Response[None],
            }
        ]

    def delete_history(self, key: str) -> schemas.Response[None]:
        """删除详情页中指定的插件历史记录。"""
        # 历史记录
        historys = self.get_data('history')
        if not historys:
            return schemas.Response(success=False, message="未找到历史记录")
        # 删除指定记录
        historys = [h for h in historys if h.get("unique") != key]
        self.save_data('history', historys)
        return schemas.Response(success=True, message="删除成功")

    def get_service(self) -> List[Dict[str, Any]]:
        """注册插件公共服务，本插件无常驻调度资源。"""
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构"""
        return views.build_form()

    def get_page(self) -> List[dict]:
        """拼装插件详情页面，需要返回页面配置，同时附带数据"""
        return views.build_page(self.get_data('history'))

    @eventmanager.register(EventType.WebhookMessage)
    def sync_del_by_webhook(self, event: Event):
        """Emby 原生 Webhook 删除事件（library.deleted，兼容 Jellyfin ItemDeleted）。"""
        if not self._enabled or str(self._sync_type) != "webhook":
            return

        event_data: schemas.WebhookEventInfo = event.event_data
        event_type = event_data.event

        # Emby Webhook event_type = library.deleted
        if not event_type or str(event_type) not in ['library.deleted', 'ItemDeleted']:
            logger.debug(f"Webhook 事件 {event_type} 非媒体删除事件，跳过")
            return

        request = payloads.build_webhook_request(event_data)
        self._dispatch_request(request)

    @eventmanager.register(EventType.WebhookMessage)
    def sync_del_by_plugin(self, event: Event):
        """Scripter X 插件删除事件（media_del）。"""
        if not self._enabled or str(self._sync_type) != "plugin":
            return

        event_data: schemas.WebhookEventInfo = event.event_data
        event_type = event_data.event

        # Scripter X插件 event_type = media_del
        if not event_type or str(event_type) != 'media_del':
            logger.debug(f"Scripter X 事件 {event_type} 非媒体删除事件，跳过")
            return

        # Scripter X插件 需要是否虚拟标识。
        # 注意：webhook 传入的 item_isvirtual 是字符串（如 "False"），MP 核心不做类型强转，
        # 必须显式归一化；无法识别（None/未知值）时走保护逻辑，防止误删除（issue #359）。
        is_virtual = payloads.normalize_bool(event_data.item_isvirtual)
        if is_virtual is None:
            logger.error(
                f"Scripter X插件方式，item_isvirtual参数未配置或无法识别（{event_data.item_isvirtual!r}），"
                f"为防止误删除，暂停插件运行")
            # 保留全量现有配置键（如 del_history 挂起的清空请求），只改 enabled；
            # update_config 只持久化不重载，进程内状态需同步置停
            config = dict(self.get_config() or {})
            config["enabled"] = False
            self.update_config(config)
            self._enabled = False
            return

        # 如果是虚拟item，则直接return，不进行删除
        if is_virtual:
            logger.info(f"{event_data.item_name} 为虚拟item，跳过同步删除")
            return

        request = payloads.build_scripterx_request(event_data)
        self._dispatch_request(request)

    def _dispatch_request(self, request: payloads.DeleteRequest):
        """webhook 入口的公共分发：排除路径 → 媒体身份校验 → 执行删除。"""
        if self._deleter.handle_exclusion(request):
            return

        # 删除链路必须绑定唯一媒体身份，缺失时不能按标题、季号或路径猜测目标。
        if not request.media_source or not request.media_id:
            logger.error(f"{request.media_name} 同步删除失败，未获取到有效媒体身份，请检查媒体库媒体是否刮削")
            return

        self._deleter.execute(request)

    @eventmanager.register(EventType.PluginAction)
    def sync_del(self, event: Event = None):
        """响应其他插件的 media_sync_del 联动请求。"""
        if not self._enabled or not event:
            return

        event_data = event.event_data
        if not event_data or event_data.get("action") != "media_sync_del":
            return

        logger.info(f"收到媒体同步删除请求：{event_data}")
        self._deleter.execute(payloads.build_action_request(event_data))

    def handle_torrent(self, type: str, src: str, torrent_hash: str):
        """判断种子是否局部删除（保留的公开入口，兼容旧外部调用）。"""
        return self._torrents.handle(media_kind=type, src=src, torrent_hash=torrent_hash)

    @eventmanager.register(EventType.DownloadFileDeleted)
    def downloadfile_del_sync(self, event: Event):
        """下载文件删除处理事件：同步处理下载器中的下载任务。"""
        if not event:
            return
        event_data = event.event_data
        src = event_data.get("src")
        if not src:
            return
        # 查询下载hash
        download_hash = self._downloadhis.get_hash_by_fullpath(src)
        if download_hash:
            download_history = self._downloadhis.get_by_hash(download_hash)
            if not download_history:
                logger.warning(f"未查询到文件 {src} 对应的下载历史")
                return
            self._torrents.handle(media_kind=download_history.type, src=src, torrent_hash=download_hash)
        else:
            logger.warning(f"未查询到文件 {src} 对应的下载记录")

    def get_state(self):
        return self._enabled

    def stop_service(self):
        """插件没有常驻调度资源，无需额外清理。"""
        return None
