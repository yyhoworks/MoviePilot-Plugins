"""QB 自动清理插件：静默删除 qBittorrent 中已完成的任务（仅删任务、保留数据文件）。

规则与旧版独立脚本 qb_auto_clean.py 完全一致：
- 只处理「已完成」（progress >= 1）且「完成时间已超过 delay_minutes」的任务；
- mponly=true 时只处理带 MOVIEPILOT 标签的任务（MoviePilot 下载的）；
- 命中 exclude_tags（标签精确匹配）或 exclude_domains（Tracker 域名子串）的任务永不删除；
- skip_paused=true 时跳过暂停状态的任务；
- 删除只移除任务本身（delete_file=False），绝不删除数据文件；
- 单轮最多删除 max_delete_per_run 个（按完成时间从早到晚）；
- 静默运行：仅写入插件日志与最近一次运行摘要，不主动发送任何消息；
- 支持 /qbclean 命令手动执行（附加参数 dry 仅预览、run 强制执行）。
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.plugins import _PluginBase
from app.schemas.types import EventType
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.services import DownloaderHelper


class QBAutoClean(_PluginBase):
    """QB 自动清理插件。"""

    # 插件名称
    plugin_name = "QB 自动清理"
    # 插件描述
    plugin_desc = "自动删除 qBittorrent 中已完成的任务（仅删任务、保留数据文件），支持豁免标签、豁免站点与执行周期；静默运行不推送消息。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/yyhoworks/MoviePilot-Plugins/main/icons/qbautoclean.png"
    # 插件版本
    plugin_version = "1.0.0"
    # 插件作者
    plugin_author = "Felix Yang"
    # 作者主页
    author_url = "https://github.com/yyhoworks"
    # 插件配置项ID前缀
    plugin_config_prefix = "qbautoclean_"
    # 加载顺序
    plugin_order = 20
    # 可使用的用户级别
    auth_level = 1

    # 私有属性（运行时配置）
    _enabled = False
    _cron = "*/15 * * * *"
    _delay_minutes = 30
    _downloaders: List[str] = []
    _mponly = False
    _exclude_tags = "H&R,彩虹岛,保留"
    _exclude_domains = "ptchdbits.co,rainbowisland.co,chdbits.xyz"
    _skip_paused = True
    _max_delete_per_run = 50
    _dry_run = False

    def init_plugin(self, config: Optional[Dict[str, Any]] = None) -> None:
        """读取配置并初始化插件运行状态。"""
        self.stop_service()
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._cron = str(config.get("cron") or "*/15 * * * *")
        try:
            self._delay_minutes = max(0, int(config.get("delay_minutes") or 30))
        except (TypeError, ValueError):
            self._delay_minutes = 30
        raw_downloaders = config.get("downloaders")
        self._downloaders = (
            [str(item) for item in raw_downloaders if item]
            if isinstance(raw_downloaders, list)
            else []
        )
        self._mponly = bool(config.get("mponly"))
        self._exclude_tags = str(config.get("exclude_tags") or "")
        self._exclude_domains = str(config.get("exclude_domains") or "")
        self._skip_paused = bool(config.get("skip_paused", True))
        try:
            self._max_delete_per_run = max(1, int(config.get("max_delete_per_run") or 50))
        except (TypeError, ValueError):
            self._max_delete_per_run = 50
        self._dry_run = bool(config.get("dry_run"))
        if self._enabled:
            logger.info(
                f"QB 自动清理已启用：周期[{self._cron}] 等待{self._delay_minutes}分钟 "
                f"下载器{self._downloaders or '全部qBittorrent'} 演练模式[{self._dry_run}]"
            )

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> Optional[List[Dict[str, Any]]]:
        """注册 /qbclean 远程命令（dry=仅预览，run=强制执行）。"""
        return [
            {
                'cmd': '/qbclean',
                'event': EventType.PluginAction,
                'desc': 'QB 自动清理（dry=预览，run=强制执行）',
                'category': '',
                'data': {
                    'action': 'qbautoclean'
                }
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """本插件不注册 API。"""
        return []

    def get_service(self) -> Optional[List[Dict[str, Any]]]:
        """注册定时清理服务。"""
        if not self._enabled or not self._cron:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron)
        except Exception as err:
            logger.error(f"QB 自动清理执行周期无效：{self._cron}（{err}）")
            return []
        return [
            {
                'id': 'QBAutoClean',
                'name': 'QB 已完成任务清理',
                'trigger': trigger,
                'func': self.clean,
                'kwargs': {}
            }
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """拼装插件配置页面与默认配置。"""
        downloader_options: List[Dict[str, str]] = []
        try:
            downloader_options = [
                {'title': str(item.name), 'value': str(item.name)}
                for item in DownloaderHelper().get_configs().values()
            ]
        except Exception as err:
            logger.error(f"QB 自动清理获取下载器列表失败：{err}")
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enabled', 'label': '启用插件'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'mponly', 'label': '仅处理 MoviePilot 任务'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'skip_paused', 'label': '跳过暂停任务'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'dry_run', 'label': '演练模式'}
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '*/15 * * * *'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'delay_minutes',
                                            'label': '完成后等待（分钟）',
                                            'type': 'number'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'max_delete_per_run',
                                            'label': '单轮删除上限',
                                            'type': 'number'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'downloaders',
                                            'label': '下载器',
                                            'items': downloader_options,
                                            'multiple': True,
                                            'clearable': True,
                                            'hint': '留空 = 全部已启用的 qBittorrent 实例',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'exclude_tags',
                                            'label': '豁免标签（逗号分隔）',
                                            'placeholder': 'H&R,彩虹岛,保留'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'exclude_domains',
                                            'label': '豁免 Tracker 域名（逗号分隔）',
                                            'placeholder': 'ptchdbits.co,rainbowisland.co,chdbits.xyz'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '删除仅移除任务本身、保留全部数据文件（媒体库硬链接不受影响）。'
                                                    '给任意任务加「保留」标签即可豁免；命中豁免标签、豁免域名、暂停状态或未满足等待时间的任务不会被删除。'
                                                    '本插件静默运行，仅记录日志与详情页摘要。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "cron": "*/15 * * * *",
            "delay_minutes": 30,
            "downloaders": [],
            "mponly": False,
            "exclude_tags": "H&R,彩虹岛,保留",
            "exclude_domains": "ptchdbits.co,rainbowisland.co,chdbits.xyz",
            "skip_paused": True,
            "max_delete_per_run": 50,
            "dry_run": False
        }

    def get_page(self) -> Optional[List[dict]]:
        """拼装插件详情页面，展示最近一次运行摘要。"""
        if not self._enabled:
            return None
        last_run = self.get_data("last_run") or {}
        if not isinstance(last_run, dict):
            last_run = {}
        deleted = last_run.get("deleted") or []
        text = f"最近运行：{last_run.get('time') or '暂无'}"
        text += f"；结果：{last_run.get('summary') or '暂无'}"
        if isinstance(deleted, list) and deleted:
            text += "；最近删除：" + "、".join(str(name) for name in deleted[:10])
        return [
            {
                'component': 'VAlert',
                'props': {
                    'type': 'info',
                    'variant': 'tonal',
                    'text': text
                }
            }
        ]

    def clean(self) -> None:
        """定时服务入口：执行一次清理（静默，仅写日志与运行摘要）。"""
        try:
            summary, deleted = self._run_clean(dry_run=self._dry_run)
            logger.info(f"QB 自动清理执行完成：{summary}；删除 {len(deleted)} 个任务")
        except Exception as err:
            logger.error(f"QB 自动清理执行异常：{err}")

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event = None) -> None:
        """响应 /qbclean 命令：立即执行清理（dry=仅预览，run=强制执行）。"""
        if not event or not event.event_data:
            return
        event_data = event.event_data
        if event_data.get("action") != "qbautoclean":
            return
        arg = str(event_data.get("arg_str") or "").strip().lower()
        if arg == "dry":
            dry_run = True
        elif arg == "run":
            dry_run = False
        else:
            dry_run = self._dry_run
        try:
            summary, deleted = self._run_clean(dry_run=dry_run)
        except Exception as err:
            logger.error(f"QB 自动清理命令执行异常：{err}")
            summary, deleted = f"执行异常：{err}", []
        logger.info(
            f"QB 自动清理命令{'预览' if dry_run else '执行'}完成：{summary}；删除 {len(deleted)} 个任务"
        )
        channel = event_data.get("channel")
        userid = event_data.get("user")
        if channel or userid:
            result_text = summary if dry_run else f"{summary}；共删除 {len(deleted)} 个任务"
            self.post_message(
                channel=channel,
                title="QB 自动清理（预览）" if dry_run else "QB 自动清理",
                text=result_text,
                userid=userid
            )

    def _run_clean(self, dry_run: bool) -> Tuple[str, List[str]]:
        """执行清理：遍历目标下载器，返回（摘要文本, 本次删除的任务名列表）。"""
        try:
            helper = DownloaderHelper()
            configs = helper.get_configs() or {}
        except Exception as err:
            logger.error(f"QB 自动清理获取下载器配置失败：{err}")
            return f"获取下载器配置失败：{err}", []
        if self._downloaders:
            names = list(self._downloaders)
        else:
            names = [
                name for name, conf in configs.items()
                if str(getattr(conf, "type", "") or "").lower() == "qbittorrent"
            ]
        if not names:
            logger.warning("QB 自动清理未找到可用下载器，跳过本轮")
            return "未找到可用下载器", []
        parts: List[str] = []
        deleted: List[str] = []
        for name in names:
            try:
                part, removed = self._clean_one(helper, name, dry_run)
            except Exception as err:
                logger.error(f"QB 自动清理处理下载器 {name} 异常：{err}")
                part, removed = f"{name}：异常", []
            parts.append(part)
            deleted.extend(removed)
        summary = "；".join(parts)
        self.save_data("last_run", {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary": summary,
            "deleted": deleted,
            "dry_run": dry_run,
        })
        return summary, deleted

    def _clean_one(self, helper: DownloaderHelper, name: str, dry_run: bool) -> Tuple[str, List[str]]:
        """处理单个下载器：统计候选并按需删除（仅删任务、保留文件）。"""
        service = helper.get_service(name)
        if not service or not service.instance:
            logger.warning(f"QB 自动清理：下载器 {name} 未连接或实例不存在，跳过")
            return f"{name}：未连接", []
        if not helper.is_downloader(service_type="qbittorrent", service=service):
            logger.warning(f"QB 自动清理：下载器 {name} 类型为 {service.type}，仅支持 qBittorrent，跳过")
            return f"{name}：非 qBittorrent", []
        client = service.instance
        try:
            if client.is_inactive():
                client.reconnect()
            if client.is_inactive():
                logger.warning(f"QB 自动清理：下载器 {name} 连接不可用，跳过")
                return f"{name}：连接不可用", []
            torrents, error = client.get_torrents()
        except Exception as err:
            logger.error(f"QB 自动清理：读取下载器 {name} 任务列表失败：{err}")
            return f"{name}：读取失败", []
        if error or torrents is None:
            logger.error(f"QB 自动清理：读取下载器 {name} 任务列表失败")
            return f"{name}：读取失败", []
        candidates, stats = self._collect_candidates(torrents)
        logger.info(
            f"QB 自动清理[{name}]：总数{stats['total']} 未完成{stats['not_completed']} "
            f"未到时间{stats['too_new']} 豁免{stats['excluded']} 非MP{stats['non_mp']} "
            f"暂停{stats['paused']} 候选{stats['candidates']}"
        )
        if not candidates:
            return f"{name}：候选 0（总数 {stats['total']}）", []
        to_delete = candidates[: self._max_delete_per_run]
        name_list = "、".join(item["name"] for item in to_delete)
        if dry_run:
            logger.info(f"QB 自动清理[{name}]：演练模式，将删除 {len(to_delete)} 个任务（仅删任务、保留文件）：{name_list}")
            return f"{name}：演练候选 {len(to_delete)}", []
        ids = [item["hash"] for item in to_delete]
        try:
            ok = client.delete_torrents(delete_file=False, ids=ids)
        except Exception as err:
            logger.error(f"QB 自动清理[{name}]：删除任务失败：{err}")
            return f"{name}：删除失败", []
        if not ok:
            logger.error(f"QB 自动清理[{name}]：删除接口返回失败：{name_list}")
            return f"{name}：删除失败", []
        logger.info(f"QB 自动清理[{name}]：已删除 {len(ids)} 个任务（仅删任务、保留文件）：{name_list}")
        return f"{name}：删除 {len(ids)} 个", [item["name"] for item in to_delete]

    def _collect_candidates(self, torrents: List[Any]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """按配置过滤全量任务，返回（候选列表, 统计信息）。"""
        now = time.time()
        delay_seconds = max(0, int(self._delay_minutes)) * 60
        exclude_tags = {
            part.strip() for part in str(self._exclude_tags or "").split(",") if part.strip()
        }
        exclude_domains = [
            part.strip().lower() for part in str(self._exclude_domains or "").split(",") if part.strip()
        ]
        stats: Dict[str, int] = {
            "total": len(torrents),
            "not_completed": 0,
            "too_new": 0,
            "excluded": 0,
            "non_mp": 0,
            "paused": 0,
            "candidates": 0,
        }
        candidates: List[Dict[str, Any]] = []
        for torrent in torrents:
            try:
                progress = float(getattr(torrent, "progress", 0) or 0)
            except (TypeError, ValueError):
                progress = 0.0
            if progress < 1:
                stats["not_completed"] += 1
                continue
            try:
                completed_at = int(getattr(torrent, "completion_on", 0) or 0)
            except (TypeError, ValueError):
                completed_at = 0
            if completed_at <= 0 or (now - completed_at) < delay_seconds:
                stats["too_new"] += 1
                continue
            state = str(getattr(torrent, "state", "") or "")
            if self._skip_paused and "paused" in state.lower():
                stats["paused"] += 1
                continue
            tags = {
                part.strip()
                for part in str(getattr(torrent, "tags", "") or "").split(",")
                if part.strip()
            }
            tracker_url = str(getattr(torrent, "tracker", "") or "")
            if tags & exclude_tags:
                stats["excluded"] += 1
                continue
            if any(domain in tracker_url.lower() for domain in exclude_domains):
                stats["excluded"] += 1
                continue
            if self._mponly and "MOVIEPILOT" not in tags:
                stats["non_mp"] += 1
                continue
            candidates.append({
                "hash": str(getattr(torrent, "hash", "") or ""),
                "name": str(getattr(torrent, "name", "") or ""),
                "completed_at": completed_at,
            })
        candidates.sort(key=lambda item: item["completed_at"])
        stats["candidates"] = len(candidates)
        return candidates, stats

    def stop_service(self) -> None:
        """停止插件服务（定时服务由宿主调度器托管，无需额外清理）。"""
        pass