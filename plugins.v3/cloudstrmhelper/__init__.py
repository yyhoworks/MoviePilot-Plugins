import json
import os
import re
import shutil
import threading
import time
import traceback
import urllib.parse
import uuid
from copy import deepcopy
from collections import deque
from datetime import datetime, timedelta
from html import escape as html_escape
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import Body
from fastapi.responses import JSONResponse
from watchdog.observers.polling import PollingObserver

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.core.metainfo import MetaInfoPath
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaInfo
from app.schemas.types import EventType, NotificationType, MediaType
from app.utils.http import RequestUtils
from app.utils.string import StringUtils

from .manifest import GeneratedFileManifest
from .monitoring import (
    FileMonitorHandler,
    is_ignored_path as _is_ignored_path,
    wait_for_stable_file as _wait_for_stable_file,
)
from .paths import (
    format_content as _format_content,
    is_path_within as _is_path_within,
    join_remote_path as _join_remote_path,
    find_monitor_path as _find_monitor_path,
    map_library_path as _map_library_path,
    map_path as _map_path,
    normalize_extensions as _normalize_extensions,
    parse_mapping_line as _parse_mapping_line,
    path_key as _path_key,
    relative_path as _relative_path,
    shorten_path as _shorten_path,
)
from .rules import (
    category_list as _category_list,
    category_string as _category_string,
    count_rule_slots as _count_rule_slots,
    remove_rule_config_keys as _remove_rule_config_keys,
    rules_from_config as _rules_from_config,
    rules_from_monitor_confs as _rules_from_monitor_confs,
    rules_to_config_keys as _rules_to_config_keys,
    rules_to_monitor_confs as _rules_to_monitor_confs,
    validate_monitor_confs as _validate_monitor_confs,
)

lock = threading.Lock()


class CloudStrmHelper(_PluginBase):
    # 插件名称
    plugin_name = "CloudStrm"
    # 插件描述
    plugin_desc = "OpenList + CD2 实时监控，定时全量增量生成 STRM 文件。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/yyhoworks/MoviePilot-Plugins/main/icons/cloudstrm.png"
    # 插件版本
    plugin_version = "V1.6.0"
    # 插件作者
    plugin_author = "Felix Yang"
    # 作者主页
    author_url = "https://github.com/yyhoworks"
    # 插件配置项ID前缀
    plugin_config_prefix = "cloudstrmHelper_"
    # 加载顺序
    plugin_order = 26
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _monitor_confs = None
    _cover = False
    _monitor = False
    _onlyonce = False
    _copy_files = False
    _copy_subtitles = False
    _sync_delete = False
    _url = None
    _notify = False
    _refresh_emby = False
    _uriencode = False
    _strm_dir_conf = {}
    _cloud_dir_conf = {}
    _category_conf = {}
    _format_conf = {}
    _observer = []
    _medias = {}
    _rmt_mediaext = None
    _other_mediaext = None
    _interval: int = 10
    _mediaservers = None
    mediaserver_helper = None
    _emby_paths = {}
    _path_replacements = {}  # 新增：路径替换规则属性
    _headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36",
        "Cookie": "",
    }

    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None
    # 退出事件
    _event = threading.Event()
    _state_lock = threading.Lock()
    _event_timers = {}
    _event_timers_lock = threading.Lock()
    _processing_paths = set()
    _generated_files = set()
    _manifest_lock = threading.Lock()
    _manifest_pending = False
    _generated_files_json = "generated_files.json"
    _media_exts = set()
    _other_exts = set()
    _event_delay = 1.0
    _stable_checks = 2
    _stable_interval = 0.5
    _task_history_limit = 30
    _task_history = []
    _task_history_json = "task_history.json"
    _task_history_lock = threading.RLock()
    _task_active_id = None
    _task_stop_event = threading.Event()
    _task_context = threading.local()
    _task_thread = None
    _task_thread_task_id = None
    _task_thread_lock = threading.Lock()
    _task_stop_events = {}
    _task_generations = {}
    _task_generation = 0
    _page_filter_status = None
    _page_task_id = None
    _task_selected_items = {}
    _task_history_dirty = 0
    _task_history_last_saved = 0.0
    _task_history_save_interval = 0.5
    _task_history_save_batch = 20
    _config_errors = []
    # V1.4.3：任务明细保留策略，避免 task_history.json 因逐文件明细无限膨胀
    _task_history_detail_tasks = 1
    _task_history_item_limit = 10000
    _task_history_failed_items_limit = 2000
    # V1.4.3：远程挂载目录状态缓存，页面请求不再同步检查 is_dir
    _dir_status_lock = threading.Lock()
    _dir_status = {}
    _dir_status_thread = None
    _dir_status_generation = 0
    _dir_status_last_refresh = 0.0
    _dir_status_refresh_interval = 30.0
    _dir_check_timeout = 2.0
    # V1.4.0：定时增量扫描（对应设计稿「定时增量扫描」开关）
    _cron_enabled = True
    _scan_interval = 30
    # V1.4.0：实时事件环形缓冲，供页面 Live Log Feed 使用
    _live_events = deque(maxlen=150)
    _live_events_lock = threading.Lock()
    # V1.4.0：孤儿清理（死链）累计统计，持久化到 stats.json
    _pruned_total = 0
    _stats_json = "stats.json"
    _stats_lock = threading.Lock()
    # V1.4.0：页面暂存配置（开关/映射删除/扩展名移除），保存后生效
    _staged_config = None
    _staged_lock = threading.Lock()
    _page_editing_rule = None

    _default_rmt_mediaext = ".mp4, .mkv, .ts, .iso,.rmvb, .avi, .mov, .mpeg,.mpg, .wmv, .3gp, .asf, .m4v, .flv, .m2ts, .strm,.tp, .f4v"
    _default_other_mediaext = ".nfo, .jpg, .png, .json"

    # Shared visual vocabulary for the settings form and the task dashboard.
    # Keep these values here so the two entry points cannot quietly drift apart.
    _ui_tokens = {
        "page": "#0d1117",
        "card": "#161b22",
        "input": "#0d1117",
        "border": "#30363d",
        "divider": "#21262d",
        "primary": "#8957e5",
        "info": "#38bdf8",
        "chip_bg": "#1f293d",
        "text": "#e5e7eb",
        "muted": "#8b949e",
    }
    _ui_card_style = (
        f"background:{_ui_tokens['card']};border:1px solid {_ui_tokens['border']};"
        f"border-radius:10px;color:{_ui_tokens['text']};"
    )
    _ui_button_style = (
        f"height:34px;border-radius:6px;background:{_ui_tokens['primary']};color:#ffffff;"
    )
    _ui_switch_style = (
        "--v-switch-track-width:34px;--v-switch-track-height:18px;"
        "--v-selection-control-size:18px;"
    )
    _subtitle_exts = {
        ".srt", ".ass", ".ssa", ".sub", ".idx", ".sup", ".vtt",
        ".smi", ".sami", ".ttml", ".dfxp"
    }
    _ignored_dir_names = {"extrafanart", "@recycle", "#recycle", "@eadir"}

    @staticmethod
    def __parse_mapping_line(line: str) -> Optional[Tuple[str, str]]:
        return _parse_mapping_line(str(line or ""))
        """解析路径映射，优先使用 =>，同时兼容旧版冒号格式。"""

        if "=>" in line:
            source, target = line.split("=>", 1)
        else:
            # 兼容 C:\\source:C:\\target 这种 Windows 路径。
            windows_match = re.match(
                r"^(?P<source>[A-Za-z]:[\\/].*?)(?::)(?P<target>.+)$",
                line,
            )
            if windows_match:
                source = windows_match.group("source")
                target = windows_match.group("target")
            elif ":" in line:
                source, target = line.split(":", 1)
            else:
                return None

        source = source.strip()
        target = target.strip()
        if not source or not target:
            return None
        return source, target

    @staticmethod
    def __shorten_path(path: str, keep: int = 2) -> str:
        return _shorten_path(path, keep)
        """长路径尾部截断：保留最后 keep 段，显示为 …/父目录/文件名。"""
        if not path:
            return ""
        parts = [seg for seg in re.split(r"[\\/]+", str(path)) if seg]
        if len(parts) <= keep:
            return path
        return "…/" + "/".join(parts[-keep:])

    @staticmethod
    def __render_table_html(headers: List[Dict[str, str]], rows: List[Dict[str, Any]],
                            empty_text: str = "暂无数据") -> str:
        """把表头/行数据渲染成轻量 HTML 表格。

        MoviePilot 的 PageRender/DashboardRender 会给每个组件注入默认插槽内容，
        VDataTable 一旦拿到默认插槽就会跳过内置的表头与表体渲染（页面只剩分页条），
        因此表格统一改用 html 字段输出原生 <table>，样式复用 Vuetify 的 v-table 类。
        """
        def cell(value: Any) -> str:
            return html_escape("" if value is None else str(value))

        head_cells = "".join(f"<th class=\"text-left\">{cell(col.get('title'))}</th>"
                             for col in headers)
        if rows:
            body_rows = "".join(
                "<tr>" + "".join(f"<td>{cell(row.get(col.get('key')))}</td>"
                                 for col in headers) + "</tr>"
                for row in rows)
        else:
            body_rows = (f"<tr><td colspan=\"{max(1, len(headers))}\" "
                         f"class=\"text-medium-emphasis\">{cell(empty_text)}</td></tr>")
        return ("<div class=\"v-table v-table--density-compact v-table--hover rounded\">"
                "<div class=\"v-table__wrapper\"><table>"
                f"<thead><tr>{head_cells}</tr></thead><tbody>{body_rows}</tbody>"
                "</table></div></div>")

    @staticmethod
    def __path_key(path: str) -> str:
        return _path_key(path)
        """返回用于边界判断的标准本地路径，不解析软链接。"""
        return os.path.normcase(os.path.abspath(os.path.normpath(str(path))))

    @classmethod
    def __relative_path(cls, path: str, root: str) -> Optional[Path]:
        return _relative_path(path, root)
        """获取 path 相对于 root 的路径，path 不在 root 下时返回 None。"""
        # 边界判断使用 normcase，但相对路径必须基于原始路径计算，
        # 否则 Windows 下目标文件名会被意外转换为小写。
        path_value = os.path.abspath(os.path.normpath(str(path)))
        root_value = os.path.abspath(os.path.normpath(str(root)))
        try:
            relative = os.path.relpath(path_value, root_value)
        except ValueError:
            return None
        if relative == os.curdir:
            return Path()
        if relative == os.pardir or relative.startswith(os.pardir + os.sep):
            return None
        return Path(relative)

    @classmethod
    def __is_path_within(cls, path: str, root: str) -> bool:
        return _is_path_within(path, root)
        return cls.__relative_path(path, root) is not None

    @classmethod
    def __map_path(cls, path: str, source_root: str, target_root: str) -> Optional[str]:
        return _map_path(path, source_root, target_root)
        relative = cls.__relative_path(path, source_root)
        if relative is None:
            return None
        if str(relative) in ("", "."):
            return os.path.normpath(str(target_root))
        return os.path.normpath(os.path.join(str(target_root), str(relative)))

    @staticmethod
    def __normalise_extensions(value: Optional[str], default: str = "") -> set:
        return _normalize_extensions(value, default)
        result = set()
        for extension in str(value or default).split(","):
            extension = extension.strip().lower()
            if not extension:
                continue
            if not extension.startswith("."):
                extension = "." + extension
            result.add(extension)
        return result

    @staticmethod
    def __count_rule_slots(config: dict) -> int:
        return _count_rule_slots(config)
        """统计配置中结构化映射规则 rule_i_* 占用的槽位数。"""
        slots = 0
        for key in (config or {}).keys():
            match = re.match(r"^rule_(\d+)_(local|strm)$", str(key))
            if match:
                slots = max(slots, int(match.group(1)) + 1)
        return slots

    @staticmethod
    def __remove_rule_config_keys(config: dict):
        return _remove_rule_config_keys(config)
        """Remove the transient/structured rule fields before rewriting them."""
        for key in list((config or {}).keys()):
            if re.match(r"^rule_\d+_(category|local|strm|cloud|format|monitor|delete)$",
                        str(key)):
                config.pop(key, None)

    @staticmethod
    def __rules_from_monitor_confs(monitor_confs: str) -> List[dict]:
        return _rules_from_monitor_confs(monitor_confs)
        """把旧版 # 分隔文本解析为结构化映射规则列表。"""
        rules = []
        for raw_line in str(monitor_confs or "").splitlines():
            line = str(raw_line).strip()
            if not line or line.startswith("#"):
                continue
            monitor_flag = None
            if line.count("$") == 1:
                line, monitor_flag = line.split("$", 1)
                monitor_flag = monitor_flag.strip()
            category = None
            if line.count("@") == 1:
                line, category = line.split("@", 1)
                category = category.strip()
            if line.count("#") < 3:
                continue
            local_dir, strm_dir, cloud_dir, format_str = [part.strip() for part in line.split("#", 3)]
            rules.append({
                "category": category or "",
                "local": local_dir,
                "strm": strm_dir,
                "cloud": cloud_dir,
                "format": format_str,
                "monitor": monitor_flag != "0",
            })
        return rules

    @staticmethod
    def __category_list(value: Any) -> List[str]:
        return _category_list(value)
        """split category into tags"""
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        raw = str(value).strip()
        if not raw:
            return []
        parts = re.split(r"[,，、]+", raw)
        return [part.strip() for part in parts if part.strip()]

    @staticmethod
    def __category_string(value: Any) -> str:
        return _category_string(value)
        """join tags as comma string"""
        return ",".join(CloudStrmHelper.__category_list(value))

    def __rules_from_config(self, config: dict) -> List[dict]:
        return _rules_from_config(config)
        """优先读取结构化 rule_i_* 键，缺失时回退解析旧版 monitor_confs 文本。"""
        config = config or {}
        rules = []
        structured_slots = self.__count_rule_slots(config)
        for index in range(structured_slots):
            delete_value = config.get(f"rule_{index}_delete")
            if (isinstance(delete_value, str)
                    and delete_value.strip().lower() in {"1", "true", "yes", "on"}) \
                    or delete_value is True:
                continue
            local = str(config.get(f"rule_{index}_local") or "").strip()
            strm = str(config.get(f"rule_{index}_strm") or "").strip()
            if not local and not strm:
                continue
            monitor_value = config.get(f"rule_{index}_monitor")
            rules.append({
                "category": CloudStrmHelper.__category_string(
                    config.get(f"rule_{index}_category") or ""),
                "local": local,
                "strm": strm,
                "cloud": str(config.get(f"rule_{index}_cloud") or "").strip(),
                "format": str(config.get(f"rule_{index}_format") or "").strip(),
                "monitor": bool(monitor_value) if monitor_value is not None else True,
            })
        # An existing structured slot set is authoritative even when every
        # rule was deleted; otherwise an old monitor_confs mirror could revive
        # a rule that the user just removed.
        if rules or structured_slots:
            return rules
        return self.__rules_from_monitor_confs(config.get("monitor_confs") or "")

    @staticmethod
    def __rules_to_monitor_confs(rules: List[dict]) -> str:
        return _rules_to_monitor_confs(rules)
        """把结构化规则序列化回旧版文本格式，作为可移植镜像。"""
        lines = []
        for rule in rules or []:
            local = str(rule.get("local") or "").strip()
            strm = str(rule.get("strm") or "").strip()
            if not local and not strm:
                continue
            line = f"{local}#{strm}#{str(rule.get('cloud') or '').strip()}#{str(rule.get('format') or '').strip()}"
            category = CloudStrmHelper.__category_string(rule.get("category"))
            if category:
                line += f"@{category}"
            if not rule.get("monitor", True):
                line += "$0"
            lines.append(line)
        return "\n".join(lines)

    @staticmethod
    def __rules_to_config_keys(rules: List[dict], slots: int) -> Dict[str, Any]:
        return _rules_to_config_keys(rules, slots)
        """把规则列表展开为 rule_i_* 配置键，空槽位写空串保证旧键被清除。"""
        keys = {}
        for index in range(max(slots, len(rules or []))):
            rule = rules[index] if rules and index < len(rules) else {}
            keys[f"rule_{index}_category"] = CloudStrmHelper.__category_string(
                rule.get("category"))
            keys[f"rule_{index}_local"] = str(rule.get("local") or "")
            keys[f"rule_{index}_strm"] = str(rule.get("strm") or "")
            keys[f"rule_{index}_cloud"] = str(rule.get("cloud") or "")
            keys[f"rule_{index}_format"] = str(rule.get("format") or "")
            keys[f"rule_{index}_monitor"] = bool(rule.get("monitor", True)) if rule else True
        return keys

    def __log_event(self, tag: str, message: str):
        """追加一条实时事件，供页面 Live Log Feed 展示；任何异常都不影响主流程。"""
        try:
            with self._live_events_lock:
                self._live_events.append({
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "tag": str(tag or "EVENT"),
                    "message": str(message or ""),
                })
        except Exception:
            pass

    def __live_event_snapshot(self, limit: int = 40) -> List[dict]:
        with self._live_events_lock:
            events = list(self._live_events)
        return events[-limit:]

    def __load_stats(self):
        try:
            if Path(self._stats_json).is_file():
                with open(self._stats_json, "r", encoding="utf-8") as file:
                    data = json.load(file)
                self._pruned_total = int(data.get("pruned_total") or 0)
        except Exception as err:
            logger.warning(f"读取清理统计失败：{err}")
            self._pruned_total = 0

    def __save_stats(self):
        with self._stats_lock:
            try:
                tmp_path = f"{self._stats_json}.tmp"
                with open(tmp_path, "w", encoding="utf-8") as file:
                    json.dump({"pruned_total": int(self._pruned_total)}, file, ensure_ascii=False)
                os.replace(tmp_path, self._stats_json)
            except OSError as err:
                logger.warning(f"保存清理统计失败：{err}")

    def __bump_pruned(self, count: int):
        if not count or count <= 0:
            return
        with self._stats_lock:
            self._pruned_total += int(count)
        self.__save_stats()

    def __current_saved_config(self) -> dict:
        """读取宿主当前保存的插件配置，隔离环境下不可用时返回空字典。"""
        try:
            config = self.get_config()
            return dict(config) if isinstance(config, dict) else {}
        except Exception:
            return {}

    def __staged_snapshot(self) -> dict:
        with self._staged_lock:
            return dict(self._staged_config or {})

    @staticmethod
    def __effective_bool(key: str, saved: dict, staged: dict, default: bool = False) -> bool:
        if key in staged:
            return bool(staged[key])
        if key in saved:
            return bool(saved[key])
        return default

    def __effective_rules(self, saved: dict, staged: dict) -> List[dict]:
        if "_rules" in staged:
            return deepcopy(staged.get("_rules") or [])
        return self.__rules_from_config(saved)

    def __pending_deleted_rules(self) -> List[dict]:
        """Return saved mapping rules removed from the staged configuration.

        The dashboard keeps a removed row visible until its pending change is
        saved. Compare occurrence-by-occurrence so identical mapping rules
        are handled correctly instead of all being treated as deleted. The
        monitor toggle is deliberately excluded: changing it is an update,
        not a deletion.
        """
        saved = self.__current_saved_config()
        staged = self.__staged_snapshot()
        if "_rules" not in staged:
            return []

        def identity(rule: dict) -> tuple:
            return (
                CloudStrmHelper.__category_string(rule.get("category")),
                str(rule.get("local") or "").strip(),
                str(rule.get("strm") or "").strip(),
                str(rule.get("cloud") or "").strip(),
                str(rule.get("format") or "").strip(),
            )

        remaining = self.__effective_rules(saved, staged)
        pending = []
        for saved_rule in self.__rules_from_config(saved):
            for index, remaining_rule in enumerate(remaining):
                if identity(saved_rule) == identity(remaining_rule):
                    remaining.pop(index)
                    break
            else:
                pending.append(deepcopy(saved_rule))
        return pending

    def __validate_monitor_confs(self, monitor_confs: str) -> List[str]:
        return _validate_monitor_confs(monitor_confs)
        """Validate directory configuration without starting monitors or scheduling work."""
        errors = []
        for original_conf in str(monitor_confs or "").splitlines():
            monitor_conf = original_conf.strip()
            if not monitor_conf or monitor_conf.startswith("#"):
                continue
            if monitor_conf.count("$") == 1:
                monitor_conf = monitor_conf.split("$", 1)[0]
            if monitor_conf.count("@") == 1:
                monitor_conf = monitor_conf.split("@", 1)[0]
            if monitor_conf.count("#") < 3:
                errors.append(f"目录配置格式错误：{original_conf.strip()}")
                continue
            local_dir, strm_dir, _cloud_dir, format_str = monitor_conf.split("#", 3)
            local_dir = local_dir.strip()
            strm_dir = strm_dir.strip()
            format_str = format_str.strip()
            if not local_dir or not strm_dir:
                errors.append(f"目录配置格式错误：{original_conf.strip()}")
                continue
            try:
                if (self.__is_path_within(strm_dir, local_dir)
                        or self.__is_path_within(local_dir, strm_dir)):
                    errors.append(f"目录配置存在包含关系：{local_dir} 与 {strm_dir}")
                    continue
            except Exception:
                errors.append(f"目录配置路径无效：{original_conf.strip()}")
                continue
            if not any(token in format_str for token in ("{local_file}", "{cloud_file}")):
                errors.append(f"{local_dir} 格式化模板缺少 {{local_file}} 或 {{cloud_file}}")
        return errors

    def __probe_dir(self, path: str) -> dict:
        path = str(path or "").strip()
        if not path:
            return {"state": "unavailable", "detail": "未配置本地目录", "checked_at": time.time()}
        result = {"ok": False, "error": ""}

        def probe():
            try:
                result["ok"] = Path(path).is_dir()
            except Exception as err:
                result["error"] = str(err)

        thread = threading.Thread(target=probe, daemon=True)
        thread.start()
        thread.join(timeout=self._dir_check_timeout)
        if thread.is_alive():
            return {"state": "unavailable", "detail": "检查超时", "checked_at": time.time()}
        if result["ok"]:
            return {"state": "available", "detail": "", "checked_at": time.time()}
        return {"state": "unavailable", "detail": result.get("error") or "目录不存在或不可访问",
                "checked_at": time.time()}

    def __refresh_dir_statuses(self):
        generation = self._dir_status_generation
        paths = []
        for rule in self.__rules_from_monitor_confs(self._monitor_confs):
            local_dir = str(rule.get("local") or "").strip()
            if local_dir and local_dir not in paths:
                paths.append(local_dir)
        results = {}
        threads = []
        for path in paths:
            results[path] = {"state": "checking", "detail": "", "checked_at": time.time()}

            def probe(path=path):
                results[path] = self.__probe_dir(path)

            thread = threading.Thread(target=probe, daemon=True)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join(timeout=self._dir_check_timeout + 0.5)
        with self._dir_status_lock:
            if generation != self._dir_status_generation:
                if self._dir_status_thread is threading.current_thread():
                    self._dir_status_thread = None
                return
            for path in paths:
                self._dir_status[path] = results.get(path) or {
                    "state": "checking", "detail": "", "checked_at": time.time()}
            self._dir_status_last_refresh = time.time()
            if self._dir_status_thread is threading.current_thread():
                self._dir_status_thread = None

    def __ensure_dir_status_refresh(self, force: bool = False):
        with self._dir_status_lock:
            thread = self._dir_status_thread
            if thread and thread.is_alive():
                return
            if not force and time.time() - self._dir_status_last_refresh < self._dir_status_refresh_interval:
                return
            self._dir_status_thread = threading.Thread(
                target=self.__refresh_dir_statuses, daemon=True)
            self._dir_status_thread.start()

    def __dir_status(self, path: str) -> Tuple[str, str, str, str]:
        path = str(path or "").strip()
        if not path:
            return "unavailable", "未配置", "#f43f5e", "未配置本地目录"
        with self._dir_status_lock:
            info = self._dir_status.get(path) or {}
        state = info.get("state") or "checking"
        detail = info.get("detail") or ""
        if state == "available":
            text, color = "可访问", "#10b981"
        elif state == "unavailable":
            text, color = "不可访问", "#f43f5e"
        else:
            text, color = "检查中", "#94a3b8"
        return state, text, color, detail

    def __monitor_config_rows(self) -> List[Dict[str, Any]]:
        """把目录配置解析成页面可展示的映射行，不依赖插件是否已启用。"""
        rows: List[Dict[str, Any]] = []
        seen = set()
        for original_conf in str(self._monitor_confs or "").splitlines():
            monitor_conf = original_conf.strip()
            if not monitor_conf or monitor_conf.startswith("#"):
                continue
            line_monitor = None
            if monitor_conf.count("$") == 1:
                line_monitor = str(monitor_conf.split("$", 1)[1]).strip()
                monitor_conf = monitor_conf.split("$", 1)[0].strip()
            category = None
            if monitor_conf.count("@") == 1:
                category = str(monitor_conf.split("@", 1)[1]).strip()
                monitor_conf = monitor_conf.split("@", 1)[0].strip()
            if monitor_conf.count("#") < 3:
                continue
            local_dir, strm_dir, cloud_dir, format_str = monitor_conf.split("#", 3)
            local_dir = local_dir.strip()
            strm_dir = strm_dir.strip()
            cloud_dir = cloud_dir.strip()
            format_str = format_str.strip()
            if not local_dir or not strm_dir:
                continue
            key = (self.__path_key(local_dir), self.__path_key(strm_dir))
            if key in seen:
                continue
            seen.add(key)
            if line_monitor == "1":
                monitor_state = "监控中"
            elif line_monitor == "0":
                monitor_state = "已停用"
            elif self._enabled and self._monitor:
                monitor_state = "监控中"
            else:
                monitor_state = "已配置"
            dir_state, dir_state_text, dir_state_color, dir_detail = self.__dir_status(local_dir)
            rows.append({
                "category": category or "-",
                "state": monitor_state,
                "local_dir": local_dir,
                "strm_dir": strm_dir,
                "cloud_dir": cloud_dir or "-",
                "format_str": format_str or "-",
                "mounted": dir_state == "available",
                "dir_state": dir_state,
                "dir_state_text": dir_state_text,
                "dir_state_color": dir_state_color,
                "dir_detail": dir_detail,
            })
        return rows

    def __monitor_status(self) -> Tuple[str, str, str]:
        """返回监控状态、颜色和说明。"""
        rows = self.__monitor_config_rows()
        if not self._enabled:
            return "插件已停用", "default", "启用插件并保存配置后开始监控"
        if not rows:
            return "未配置目录", "warning", "请在“路径监控与 STRM 映射策略”中配置目录"
        dir_states = [row.get("dir_state") for row in rows]
        mounted = dir_states.count("available")
        checking = dir_states.count("checking")
        if not mounted and not checking:
            return "CD2 挂载异常", "error", f"{len(rows)} 个监控目录均不可访问"
        if not mounted and checking:
            return "CD2 挂载检查中", "info", "目录状态正在后台检查，请稍候"
        if checking and mounted < len(rows):
            return "CD2 部分检查中", "info", f"可用 {mounted}/{len(rows)}，其余目录状态检查中"
        if mounted < len(rows):
            return "CD2 部分异常", "warning", f"可用 {mounted}/{len(rows)}，{len(rows) - mounted} 个目录异常"
        if not self._monitor:
            return "已启用（未开启实时监控）", "info", "可开启 OpenList + CD2 实时监控"
        return "OpenList + CD2 监控中", "success", f"{len(rows)} 个 CD2 目录正常"

    def __openlist_status(self) -> Tuple[str, str, str]:
        """根据目录模板判断 OpenList 地址是否已配置。"""
        templates = [row.get("format_str", "") for row in self.__monitor_config_rows()]
        if not templates:
            return "OpenList 未配置", "warning", "目录模板中未发现地址"
        if any(("http://" in template or "https://" in template) for template in templates):
            return "OpenList 已配置", "success", f"{len(templates)} 个地址模板"
        return "OpenList 未配置", "warning", "目录模板应包含 http(s) 地址"



    @staticmethod
    def __task_now() -> str:
        return datetime.now().isoformat(timespec="seconds")

    @staticmethod
    def __task_result(source_file: str, target_file: str = "", status: str = "skipped",
                      action: str = "", stage: str = "", reason: str = "",
                      retryable: bool = False, monitor_path: str = "") -> dict:
        return {
            "id": uuid.uuid4().hex[:12],
            "source_file": str(source_file or ""),
            "target_file": str(target_file or ""),
            "action": action,
            "status": status,
            "stage": stage,
            "reason": reason,
            "retryable": bool(retryable),
            "monitor_path": str(monitor_path or ""),
            "created_at": CloudStrmHelper.__task_now(),
        }

    def __save_task_history(self, force: bool = False):
        """节流保存任务历史，同时保持临时文件替换的原子性。"""
        with self._task_history_lock:
            elapsed = time.monotonic() - self._task_history_last_saved
            if (not force and self._task_history_dirty < self._task_history_save_batch
                    and elapsed < self._task_history_save_interval):
                return
            snapshot = deepcopy(self._task_history)
            path = Path(self._task_history_json)
            temp_file = path.with_name(f".{path.name}.tmp")
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(temp_file, "w", encoding="utf-8") as file:
                    json.dump(snapshot, file, ensure_ascii=False, indent=2)
                os.replace(str(temp_file), str(path))
                self._task_history_dirty = 0
                self._task_history_last_saved = time.monotonic()
            except OSError as err:
                try:
                    if temp_file.exists():
                        temp_file.unlink()
                except OSError:
                    pass
                logger.warning(f"保存任务历史失败：{err}")

    def __load_task_history(self):
        changed = False
        try:
            path = Path(self._task_history_json)
            if path.is_file():
                with open(path, "r", encoding="utf-8") as file:
                    data = json.load(file)
                if isinstance(data, list):
                    tasks = [item for item in data if isinstance(item, dict)]
                    if len(tasks) > self._task_history_limit:
                        changed = True
                    self._task_history = tasks[-self._task_history_limit:]
        except (OSError, ValueError) as err:
            logger.warning(f"读取任务历史失败：{err}")
            self._task_history = []

        now = self.__task_now()
        with self._task_history_lock:
            for task in self._task_history:
                if not isinstance(task, dict):
                    continue
                if task.get("status") == "running":
                    task["status"] = "interrupted"
                    task["finished_at"] = now
                    task["error"] = "插件重启时任务未完成"
                    task["duration_seconds"] = self.__task_duration(task)
                    changed = True
            running = next((task for task in reversed(self._task_history)
                            if task.get("status") == "running"), None)
            self._task_active_id = running.get("id") if running else None
            if self.__trim_task_history():
                changed = True
        if changed:
            try:
                self.__save_task_history(force=True)
            except OSError as err:
                logger.warning(f"保存中断任务历史失败：{err}")

    def __task_snapshot(self, task: dict, include_items: bool = True) -> dict:
        if not include_items:
            return {key: deepcopy(value) for key, value in task.items() if key != "items"}
        return deepcopy(task)

    def __trim_task_history(self):
        trimmed = False
        with self._task_history_lock:
            before = sum(len(task.get("items") or []) for task in self._task_history)
            ended = [item for item in self._task_history if item.get("status") != "running"]
            running = [item for item in self._task_history if item.get("status") == "running"]
            ended = ended[-self._task_history_limit:] if ended else ended
            newest_first = list(reversed(ended))
            for offset, task in enumerate(newest_first):
                items = task.get("items") or []
                if not items:
                    continue
                if offset < self._task_history_detail_tasks:
                    if len(items) > self._task_history_item_limit:
                        task["items"] = items[-self._task_history_item_limit:]
                else:
                    failed = [item for item in items if item.get("status") == "failed"]
                    if len(failed) > self._task_history_failed_items_limit:
                        failed = failed[-self._task_history_failed_items_limit:]
                    task["items"] = failed
            for task in running:
                items = task.get("items") or []
                if len(items) > self._task_history_item_limit:
                    task["items"] = items[-self._task_history_item_limit:]
            self._task_history = ended + running
            trimmed = sum(len(task.get("items") or []) for task in self._task_history) < before
        return trimmed

    def __task_summary(self, task: dict) -> dict:
        summary = self.__task_snapshot(task, include_items=False)
        summary["duration_seconds"] = self.__task_duration(summary)
        return summary

    def __current_task(self) -> Optional[dict]:
        with self._task_history_lock:
            for task in reversed(self._task_history):
                if task.get("status") == "running":
                    return self.__task_summary(task)
        return None

    def __create_task(self, kind: str, scope: dict) -> Tuple[Optional[dict], Optional[dict]]:
        with self._task_history_lock:
            active = next((task for task in reversed(self._task_history)
                           if task.get("status") == "running"), None)
            if active:
                self._task_active_id = active.get("id")
                return None, self.__task_summary(active)
            if self._task_thread and self._task_thread.is_alive():
                stopping_id = self._task_thread_task_id or self._task_active_id
                stopping = next((task for task in reversed(self._task_history)
                                 if task.get("id") == stopping_id), None)
                if stopping:
                    return None, self.__task_summary(stopping)
                return None, {"id": stopping_id, "status": "running"}

            now = self.__task_now()
            task = {
                "id": uuid.uuid4().hex[:12],
                "kind": kind,
                "status": "running",
                "created_at": now,
                "started_at": now,
                "finished_at": None,
                "updated_at": now,
                "scope": scope or {},
                "stats": {
                    "discovered": 0,
                    "processed": 0,
                    "success": 0,
                    "skipped": 0,
                    "failed": 0,
                },
                "items": [],
            }
            self._task_history.append(task)
            self._task_active_id = task["id"]
            self._task_stop_event.clear()
            self._task_stop_events[task["id"]] = threading.Event()
            self._task_generations[task["id"]] = self._task_generation
            self._task_history_dirty += 1
            self.__save_task_history(force=True)
            return self.__task_snapshot(task), None

    def __task_discover(self, task_id: str):
        with self._task_history_lock:
            task = next((item for item in self._task_history if item.get("id") == task_id), None)
            if not task or task.get("status") != "running":
                return
            task["stats"]["discovered"] += 1
            task["updated_at"] = self.__task_now()
            self._task_history_dirty += 1
        self.__save_task_history()

    def __task_record_result(self, task_id: str, result: dict):
        result = deepcopy(result or {})
        result.setdefault("id", uuid.uuid4().hex[:12])
        result.setdefault("status", "failed")
        result.setdefault("created_at", self.__task_now())
        trim_now = False
        with self._task_history_lock:
            task = next((item for item in self._task_history if item.get("id") == task_id), None)
            if not task or task.get("status") != "running":
                return
            duplicate = next((item for item in task["items"]
                              if item.get("source_file") == result.get("source_file")
                              and item.get("target_file") == result.get("target_file")
                              and item.get("action") == result.get("action")
                              and item.get("stage") == result.get("stage")), None)
            if duplicate:
                return
            task["items"].append(result)
            trim_now = len(task["items"]) % 500 == 0
            task["stats"]["processed"] += 1
            status = result.get("status")
            if status not in {"success", "skipped", "failed"}:
                status = "failed"
                result["status"] = status
            task["stats"].setdefault(status, 0)
            task["stats"][status] += 1
            task["updated_at"] = self.__task_now()
            self._task_history_dirty += 1
        self.__save_task_history()
        if trim_now:
            self.__trim_task_history()

    def __task_is_stopped(self, task_id: str) -> bool:
        # Each worker keeps its own event so a plugin reload cannot clear the
        # stop signal before the old daemon thread has exited.
        event = getattr(self._task_context, "stop_event", None)
        if event is None:
            event = self._task_stop_events.get(task_id)
        generation = getattr(self._task_context, "task_generation", None)
        if generation is not None and generation != self._task_generation:
            return True
        return bool(event and event.is_set())

    def __task_claim_source(self, task_id: str, source_file: str) -> bool:
        if not task_id:
            return True
        seen_sources = getattr(self._task_context, "seen_sources", None)
        if seen_sources is None:
            seen_sources = set()
            self._task_context.seen_sources = seen_sources
        source_key = self.__path_key(source_file)
        if source_key in seen_sources:
            return False
        seen_sources.add(source_key)
        return True

    def __task_claim_sidecar(self, source_file: str) -> bool:
        handled_sidecars = getattr(self._task_context, "handled_sidecars", None)
        if handled_sidecars is None:
            handled_sidecars = set()
            self._task_context.handled_sidecars = handled_sidecars
        source_key = self.__path_key(source_file)
        if source_key in handled_sidecars:
            return False
        handled_sidecars.add(source_key)
        return True

    def __finish_task(self, task_id: str, error: str = "", channel=None, userid=None):
        with self._task_history_lock:
            task = next((item for item in self._task_history if item.get("id") == task_id), None)
            if not task or task.get("status") != "running":
                return
            stats = task.get("stats") or {}
            if error:
                if not any(item.get("action") == "task" for item in task.get("items") or []):
                    failure = self.__task_result("", status="failed", action="task",
                                                  stage="execution", reason=error, retryable=False)
                    task.setdefault("items", []).append(failure)
                    stats["processed"] = stats.get("processed", 0) + 1
                    stats["failed"] = stats.get("failed", 0) + 1
                status = ("partial" if stats.get("failed", 0)
                          and stats.get("success", 0) + stats.get("skipped", 0)
                          else "failed")
                task["error"] = error
            elif self.__task_is_stopped(task_id):
                status = "interrupted"
                task["error"] = "任务被插件停止"
            elif stats.get("failed", 0) and stats.get("success", 0) + stats.get("skipped", 0):
                status = "partial"
            elif stats.get("failed", 0):
                status = "failed"
            else:
                status = "success"
            task["status"] = status
            task["finished_at"] = self.__task_now()
            task["updated_at"] = task["finished_at"]
            try:
                started = datetime.fromisoformat(str(task.get("started_at")))
                finished = datetime.fromisoformat(str(task["finished_at"]))
                task["duration_seconds"] = max(0.0, round((finished - started).total_seconds(), 3))
            except (TypeError, ValueError):
                task["duration_seconds"] = None
            if self._task_active_id == task_id:
                self._task_active_id = None
            self._task_stop_events.pop(task_id, None)
            self._task_generations.pop(task_id, None)
            self._task_history_dirty += 1
            finished = self.__task_snapshot(task, include_items=False)
            self.__trim_task_history()
        self.__save_task_history(force=True)
        self.__notify_task(finished, channel=channel, userid=userid)
        # V1.4.0：任务结束写入实时事件流，供页面 Live Log Feed 展示
        finished_stats = (finished or {}).get("stats") or {}
        kind_names = {"full_scan": "全量扫描", "targeted": "定向同步", "retry": "失败重试"}
        status_names = {"success": "成功", "partial": "部分成功", "failed": "失败", "interrupted": "已中断"}
        task_duration = self.__task_duration(finished or {}) or 0
        logger.info(
            f"{kind_names.get((finished or {}).get('kind'), '任务')}"
            f"{status_names.get((finished or {}).get('status'), '结束')}："
            f"新增 {finished_stats.get('success', 0)}，跳过 {finished_stats.get('skipped', 0)}，"
            f"失败 {finished_stats.get('failed', 0)}，总耗时 {task_duration:.1f} 秒")
        self.__log_event(
            "TASK",
            f"{kind_names.get((finished or {}).get('kind'), '任务')}"
            f"{status_names.get((finished or {}).get('status'), '结束')}："
            f"成功 {finished_stats.get('success', 0)}，跳过 {finished_stats.get('skipped', 0)}，"
            f"失败 {finished_stats.get('failed', 0)}")

    def __run_task(self, task_id: str, worker, channel=None, userid=None, generation=None):
        stop_event = self._task_stop_events.get(task_id)
        try:
            self._task_context.task_id = task_id
            self._task_context.stop_event = stop_event
            self._task_context.task_generation = generation
            self._task_context.seen_sources = set()
            self._task_context.allowed_sources = None
            self._task_context.handled_sidecars = set()
            worker(task_id)
        except Exception as err:
            logger.error(f"CloudStrm 任务执行失败：{err} - {traceback.format_exc()}")
            self.__finish_task(task_id, error=str(err), channel=channel, userid=userid)
        else:
            self.__finish_task(task_id, channel=channel, userid=userid)
        finally:
            self._task_context.task_id = None
            self._task_context.stop_event = None
            self._task_context.task_generation = None
            self._task_context.seen_sources = None
            self._task_context.allowed_sources = None
            self._task_context.handled_sidecars = None
            with self._task_history_lock:
                if self._task_stop_events.get(task_id) is stop_event:
                    self._task_stop_events.pop(task_id, None)
                if self._task_thread is threading.current_thread():
                    self._task_thread = None
                    self._task_thread_task_id = None
                if self._task_active_id == task_id:
                    self._task_active_id = None

    def __start_task(self, kind: str, scope: dict, worker, channel=None, userid=None) -> dict:
        with self._task_thread_lock:
            task, active = self.__create_task(kind, scope)
            if active:
                return {"accepted": False, "task_id": active.get("id"), "task": active,
                        "message": "已有任务正在运行"}
            thread = threading.Thread(
                target=self.__run_task,
                args=(task["id"], worker, channel, userid,
                      self._task_generations.get(task["id"], self._task_generation)),
                name=f"cloudstrm-task-{task['id']}", daemon=True,
            )
            self._task_thread = thread
            self._task_thread_task_id = task["id"]
            thread.start()
        return {"accepted": True, "task_id": task["id"], "task": task}

    def __get_task(self, task_id: str) -> Optional[dict]:
        with self._task_history_lock:
            task = next((item for item in self._task_history if item.get("id") == task_id), None)
            return self.__task_snapshot(task) if task else None

    def __task_page_link(self) -> str:
        return settings.MP_DOMAIN(f"#/plugins?tab=installed&id={self.__class__.__name__}")

    def __notify_task(self, task: dict, channel=None, userid=None):
        if not self._notify or not task:
            return
        stats = task.get("stats") or {}
        scope = task.get("scope") or {}
        kind_names = {"full_scan": "全量扫描", "targeted": "定向同步", "retry": "失败重试"}
        status_names = {"running": "运行中", "success": "成功", "partial": "部分成功",
                        "failed": "失败", "interrupted": "已中断"}
        status_label = status_names.get(task.get("status"), task.get("status") or "完成")
        title = f"CloudStrm 任务{status_label}：成功 {stats.get('success', 0)}，跳过 {stats.get('skipped', 0)}，失败 {stats.get('failed', 0)}"
        text = (
            f"类型：{kind_names.get(task.get('kind'), task.get('kind', '任务'))}\n"
            f"范围：{scope.get('path') or '全部监控目录'}\n"
            f"总数：{stats.get('discovered', 0)}，已处理：{stats.get('processed', 0)}\n"
            f"任务状态：{task.get('status')}\n"
            f"失败项：{stats.get('failed', 0)}\n"
            f"总耗时：{self.__task_duration(task) or 0:.1f} 秒"
        )
        message = {"mtype": NotificationType.Plugin, "title": title, "text": text,
                   "link": self.__task_page_link()}
        if channel:
            message["channel"] = channel
        if userid:
            message["userid"] = userid
        try:
            self.post_message(**message)
        except Exception as err:
            logger.warning(f"发送 CloudStrm 任务摘要通知失败：{err}")

    def __api_error(self, status_code: int, message: str, **extra) -> JSONResponse:
        payload = {"error": message, "message": message}
        payload.update(extra)
        return JSONResponse(status_code=status_code, content=payload)

    @staticmethod
    def __task_duration(task: dict) -> Optional[float]:
        if task.get("duration_seconds") is not None:
            return task.get("duration_seconds")
        try:
            started = datetime.fromisoformat(str(task.get("started_at")))
            end_value = task.get("finished_at") or datetime.now().isoformat(timespec="seconds")
            finished = datetime.fromisoformat(str(end_value))
            return max(0.0, round((finished - started).total_seconds(), 3))
        except (TypeError, ValueError):
            return None

    def __task_page_snapshot(self, task_id: str, status: str = None, limit: int = 100) -> Optional[dict]:
        with self._task_history_lock:
            task = next((item for item in self._task_history if item.get("id") == task_id), None)
            if not task:
                return None
            summary = self.__task_summary(task)
            items = task.get("items") or []
            if status:
                items = [item for item in items if item.get("status") == status]
            summary["items"] = deepcopy(items[:limit])
            summary["item_total"] = len(items)
        return summary

    def __api_tasks(self, status: str = None):
        summaries = []
        with self._task_history_lock:
            tasks = list(reversed(self._task_history))
            if status:
                tasks = [task for task in tasks if task.get("status") == status]
            summaries = [self.__task_summary(task) for task in tasks[:self._task_history_limit]]
        current = self.__current_task()
        if status and current and current.get("status") != status:
            current = None
        return {"current": current, "tasks": summaries, "total": len(summaries)}

    def __api_task_detail(self, task_id: str, status: str = None, page: int = 1,
                          page_size: int = 50):
        try:
            page = max(1, int(page))
            page_size = min(200, max(1, int(page_size)))
        except (TypeError, ValueError):
            return self.__api_error(400, "page 和 page_size 必须是数字")
        if status and status not in {"success", "skipped", "failed"}:
            return self.__api_error(400, "明细状态只能是 success、skipped 或 failed")
        with self._task_history_lock:
            task = next((item for item in self._task_history if item.get("id") == task_id), None)
            if not task:
                return self.__api_error(404, f"任务不存在：{task_id}")
            detail = self.__task_summary(task)
            items = task.get("items") or []
            if status:
                items = [item for item in items if item.get("status") == status]
            total = len(items)
            start = (page - 1) * page_size
            detail["items"] = deepcopy(items[start:start + page_size])
            detail["pagination"] = {
                "page": page, "page_size": page_size, "total": total,
                "pages": (total + page_size - 1) // page_size if total else 0,
            }
        return detail

    def __api_start_task(self, payload: Optional[dict] = Body(default=None)):
        payload = payload or {}
        kind = str(payload.get("kind") or payload.get("mode") or "full_scan").strip()
        if kind not in {"full_scan", "targeted"}:
            return self.__api_error(400, "任务模式只能是 full_scan 或 targeted")
        path = str(payload.get("path") or "").strip()
        mon_path = self.__find_monitor_path(path) if path else None
        if path:
            path_obj = Path(path)
            if not path_obj.exists():
                return self.__api_error(400, "目标路径不存在")
            if not mon_path or not self.__is_path_within(path, mon_path):
                return self.__api_error(400, "目标路径不属于已配置的监控目录")
            if kind == "full_scan" and not path_obj.is_dir():
                return self.__api_error(400, "全量扫描路径必须是目录")
        if kind == "targeted" and not path:
            return self.__api_error(400, "定向同步必须提供 path")
        if kind == "full_scan":
            worker = lambda task_id: self.scan(scan_path=path or None, mon_path=mon_path,
                                               task_id=task_id, record_task=False)
        else:
            worker = lambda task_id: self.__run_targeted_paths(
                task_id, [{"path": path, "monitor_path": mon_path}]
            )
        result = self.__start_task(kind, {"path": path, "monitor_path": mon_path or ""}, worker)
        if not result.get("accepted"):
            return self.__api_error(409, result.get("message", "已有任务正在运行"),
                                    task_id=result.get("task_id"), current=result.get("task"))
        self._page_task_id = result["task_id"]
        return JSONResponse(status_code=202, content={"task_id": result["task_id"],
                                                       "task": result["task"]})

    def __api_retry_task(self, task_id: str, payload: Optional[dict] = Body(default=None)):
        original = self.__get_task(task_id)
        if not original:
            return self.__api_error(404, f"任务不存在：{task_id}")
        payload = payload or {}
        requested_ids = payload.get("item_ids")
        if requested_ids is not None and not isinstance(requested_ids, list):
            return self.__api_error(400, "item_ids 必须是数组")
        selected = []
        for item in original.get("items") or []:
            if requested_ids is not None and item.get("id") not in requested_ids:
                continue
            if item.get("status") == "failed" and item.get("retryable"):
                selected.append(deepcopy(item))
        if not selected:
            return self.__api_error(400, "没有可重试的失败项")
        source_ids = [item.get("id") for item in selected]

        def worker(retry_id):
            self._task_context.allowed_sources = {
                self.__path_key(item.get("source_file")) for item in selected
                if item.get("source_file")
            }
            for item in selected:
                if self.__task_is_stopped(retry_id):
                    break
                source_file = item.get("source_file")
                monitor_path = item.get("monitor_path") or self.__find_monitor_path(source_file)
                if not monitor_path:
                    result = self.__task_result(source_file, item.get("target_file"), status="failed",
                                                action="retry", stage="mapping",
                                                reason="找不到对应的监控目录", retryable=False)
                    self.__task_discover(retry_id)
                    self.__task_record_result(retry_id, result)
                    continue
                self.__handle_file(source_file, monitor_path,
                                   force=item.get("stage") in {"format", "generate", "copy", "source"},
                                   retry_stage=item.get("stage") if item.get("stage") in {"push", "emby"} else "")

        result = self.__start_task("retry", {"source_task_id": task_id,
                                                  "item_ids": source_ids}, worker)
        if not result.get("accepted"):
            return self.__api_error(409, result.get("message", "已有任务正在运行"),
                                    task_id=result.get("task_id"), current=result.get("task"))
        self._page_task_id = result["task_id"]
        self._task_selected_items.pop(task_id, None)
        return JSONResponse(status_code=202, content={"task_id": result["task_id"],
                                                       "source_task_id": task_id,
                                                       "task": result["task"]})

    def __api_toggle_task_item(self, task_id: str,
                               payload: Optional[dict] = Body(default=None)):
        """Toggle a retryable failure selection used by the native task page."""
        task = self.__get_task(task_id)
        if not task:
            return self.__api_error(404, f"任务不存在：{task_id}")
        payload = payload or {}
        item_id = str(payload.get("item_id") or "").strip()
        if not item_id:
            return self.__api_error(400, "item_id 不能为空")
        item = next((item for item in task.get("items") or []
                     if item.get("id") == item_id), None)
        if not item:
            return self.__api_error(404, f"任务明细不存在：{item_id}")
        if item.get("status") != "failed" or not item.get("retryable"):
            return self.__api_error(400, "只有可重试的失败项可以加入批量重试")
        with self._task_history_lock:
            selected = self._task_selected_items.setdefault(task_id, set())
            if item_id in selected:
                selected.remove(item_id)
                checked = False
            else:
                selected.add(item_id)
                checked = True
            selected_ids = sorted(selected)
        return {"task_id": task_id, "item_id": item_id,
                "selected": checked, "selected_item_ids": selected_ids}

    # V1.4.0：页面可直接切换的策略开关白名单及其默认值
    _config_toggle_keys = {
        "monitor": "OpenList + CD2 实时监控",
        "cron_enabled": "定时增量扫描",
        "sync_delete": "云端同步删除",
        "cover": "覆盖已存在文件",
        "copy_files": "复制旁车文件",
        "copy_subtitles": "复制字幕文件",
        "notify": "任务与入库通知",
    }
    _config_toggle_defaults = {"cron_enabled": True}

    def __api_config_toggle(self, payload: Optional[dict] = Body(default=None)):
        """页面开关：服务端翻转指定布尔配置并写入暂存，保存后生效。"""
        key = str((payload or {}).get("key") or "").strip()
        if key not in self._config_toggle_keys:
            return self.__api_error(400, f"不支持切换的配置项：{key}")
        saved = self.__current_saved_config()
        with self._staged_lock:
            staged = dict(self._staged_config or {})
            if key in staged:
                current = bool(staged[key])
            elif key in saved:
                current = bool(saved[key])
            else:
                current = bool(self._config_toggle_defaults.get(key, False))
            staged[key] = not current
            self._staged_config = staged
            new_value = staged[key]
        label = self._config_toggle_keys.get(key, key)
        self.__log_event("CONFIG", f"{label} 已{'开启' if new_value else '关闭'}（待保存）")
        return {"key": key, "value": new_value, "staged": True}

    def __api_config_save(self, payload: Optional[dict] = Body(default=None)):
        """把页面暂存的开关、映射删除和扩展名变更落盘并重新初始化插件。"""
        with self._staged_lock:
            staged = dict(self._staged_config or {})
        if not staged:
            return {"saved": False, "message": "没有待保存的更改"}
        saved = self.__current_saved_config()
        new_config = dict(saved)
        rules = staged.pop("_rules", None)
        for key, value in staged.items():
            if key in self._config_toggle_keys or key == "rmt_mediaext":
                new_config[key] = value
        if rules is not None:
            self.__remove_rule_config_keys(new_config)
            new_config.update(self.__rules_to_config_keys(rules, len(rules)))
            new_config["monitor_confs"] = self.__rules_to_monitor_confs(rules)
        try:
            self.update_config(new_config)
        except Exception as err:
            return self.__api_error(500, f"保存配置失败：{err}")
        with self._staged_lock:
            self._staged_config = None
        self._page_editing_rule = None
        self.__log_event("CONFIG", "配置已保存，OpenList + CD2 监控服务重新加载")
        try:
            self.init_plugin(new_config)
        except Exception as err:
            logger.error(f"保存配置后重新初始化失败：{err} - {traceback.format_exc()}")
        return {"saved": True}

    def __api_config_discard(self, payload: Optional[dict] = Body(default=None)):
        with self._staged_lock:
            had_changes = bool(self._staged_config)
            self._staged_config = None
        self._page_editing_rule = None
        return {"discarded": had_changes}

    def __api_mapping_delete(self, payload: Optional[dict] = Body(default=None)):
        """删除一条映射规则，变更写入暂存，点击「保存配置」后生效。"""
        try:
            index = int((payload or {}).get("index"))
        except (TypeError, ValueError):
            return self.__api_error(400, "index 必须是数字")
        saved = self.__current_saved_config()
        with self._staged_lock:
            staged = dict(self._staged_config or {})
            rules = deepcopy(staged["_rules"]) if "_rules" in staged \
                else self.__rules_from_config(saved)
            if index < 0 or index >= len(rules):
                return self.__api_error(404, f"映射规则不存在：第 {index + 1} 条")
            removed = rules.pop(index)
            staged["_rules"] = rules
            self._staged_config = staged
        removed_name = removed.get("category") or removed.get("local") or f"第 {index + 1} 条"
        self.__log_event("CONFIG", f"映射规则「{removed_name}」已标记删除（待保存）")
        return {"deleted": index, "remaining": len(rules), "staged": True}

    def __api_mapping_edit(self, payload: Optional[dict] = Body(default=None)):
        """标记正在编辑的规则行，页面展开「前往设置编辑」提示；再次点击取消。"""
        try:
            index = int((payload or {}).get("index", -1))
        except (TypeError, ValueError):
            return self.__api_error(400, "index 必须是数字")
        if self._page_editing_rule == index:
            self._page_editing_rule = None
            editing = None
        else:
            self._page_editing_rule = index
            editing = index
        return {"editing": editing,
                "message": "映射规则的新增与编辑请在插件「设置 → 路径监控与 STRM 映射策略」中完成"}

    def __api_mapping_toggle_monitor(self, index: int, payload: Optional[dict] = Body(default=None)):
        try:
            index = int(index)
        except (TypeError, ValueError):
            return self.__api_error(400, "index 必须是数字")
        saved = self.__current_saved_config()
        with self._staged_lock:
            staged = dict(self._staged_config or {})
            rules = deepcopy(staged["_rules"]) if "_rules" in staged \
                else self.__rules_from_config(saved)
            if index < 0 or index >= len(rules):
                return self.__api_error(404, f"映射规则不存在：第 {index + 1} 条")
            rules[index]["monitor"] = not bool(rules[index].get("monitor", True))
            staged["_rules"] = rules
            self._staged_config = staged
        rule = rules[index]
        rule_monitor = bool(rule.get("monitor", True))
        label = rule.get("category") or rule.get("local") or f"第 {index + 1} 条"
        self.__log_event("CONFIG",
                        f"映射规则「{label}」实时监控已{'开启' if rule_monitor else '关闭'}（待保存）")
        return {"index": index, "monitor": rule_monitor, "staged": True}

    def __api_extension_remove(self, payload: Optional[dict] = Body(default=None)):
        """从监控扩展名列表移除一个格式，变更写入暂存。"""
        ext = str((payload or {}).get("ext") or "").strip().lower()
        if not ext:
            return self.__api_error(400, "ext 不能为空")
        if not ext.startswith("."):
            ext = "." + ext
        saved = self.__current_saved_config()
        with self._staged_lock:
            staged = dict(self._staged_config or {})
            current_value = (staged.get("rmt_mediaext") or saved.get("rmt_mediaext")
                             or self._default_rmt_mediaext)
            extensions = sorted(self.__normalise_extensions(current_value,
                                                            self._default_rmt_mediaext))
            if ext not in extensions:
                return self.__api_error(404, f"扩展名不存在：{ext}")
            if len(extensions) <= 1:
                return self.__api_error(400, "至少保留一种视频格式")
            extensions.remove(ext)
            staged["rmt_mediaext"] = ", ".join(extensions)
            self._staged_config = staged
        self.__log_event("CONFIG", f"监控扩展名 {ext} 已移除（待保存）")
        return {"removed": ext, "extensions": extensions, "staged": True}

    def __api_get_tasks(self, status: str = None):
        if status and status not in {"running", "success", "partial", "failed", "interrupted"}:
            return self.__api_error(400, "无效的任务状态")
        return self.__api_tasks(status=status)

    def __api_get_task(self, task_id: str, status: str = None, page: int = 1, page_size: int = 50):
        if status and status not in {"success", "skipped", "failed"}:
            return self.__api_error(400, "明细状态只能是 success、skipped 或 failed")
        self._page_task_id = task_id
        self._page_filter_status = status or None
        return self.__api_task_detail(task_id, status, page, page_size)

    def init_plugin(self, config: dict = None):
        # 清空配置
        self.stop_service()
        self._enabled = False
        self._onlyonce = False
        self._interval = 10
        self._monitor = False
        self._cover = False
        self._copy_files = False
        self._copy_subtitles = False
        self._sync_delete = False
        self._refresh_emby = False
        self._notify = False
        self._uriencode = False
        self._monitor_confs = ""
        self._url = ""
        self._mediaservers = []
        self._rmt_mediaext = self._default_rmt_mediaext
        self._other_mediaext = self._default_other_mediaext
        self._strm_dir_conf = {}
        self._cloud_dir_conf = {}
        self._format_conf = {}
        self._category_conf = {}
        self._emby_paths = {}
        self._medias = {}
        self._processing_paths = set()
        self._event_timers = {}
        self._generated_files = set()
        self._manifest_pending = False
        self._path_replacements = {}  # 新增：清空路径替换规则
        self._generated_files_json = os.path.join(self.get_data_path(), "generated_files.json")
        self._task_history_json = os.path.join(self.get_data_path(), "task_history.json")
        self._task_history = []
        self._task_active_id = None
        self._task_stop_event = threading.Event()
        self._task_stop_events = {}
        with self._task_thread_lock:
            if not self._task_thread or not self._task_thread.is_alive():
                self._task_thread = None
                self._task_thread_task_id = None
        self._task_generations = {}
        self._task_history_dirty = 0
        self._task_history_last_saved = 0.0
        self._page_filter_status = None
        self._page_task_id = None
        self._task_selected_items = {}
        self._config_errors = []
        with self._dir_status_lock:
            self._dir_status = {}
            self._dir_status_last_refresh = 0.0
            self._dir_status_generation += 1
        self.mediaserver_helper = MediaServerHelper()
        self.__load_generated_files()
        self.__load_task_history()
        # V1.4.0：定时增量扫描与页面暂存状态
        self._cron_enabled = True
        self._scan_interval = 30
        with self._staged_lock:
            self._staged_config = None
        self._page_editing_rule = None
        self._stats_json = os.path.join(self.get_data_path(), "stats.json")
        self._pruned_total = 0
        self.__load_stats()

        if config:
            self._enabled = bool(config.get("enabled"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._interval = config.get("interval") or 10
            self._monitor = bool(config.get("monitor"))
            # V1.4.0：定时增量扫描开关与周期（分钟）
            self._cron_enabled = bool(config.get("cron_enabled", True))
            try:
                self._scan_interval = max(5, int(config.get("scan_interval") or 30))
            except (TypeError, ValueError):
                self._scan_interval = 30
            self._cover = bool(config.get("cover"))
            # The UI exposes one sidecar switch.  Existing configurations that
            # predate it keep their split values until they are saved again.
            if "copy_sidecars" in config:
                copy_sidecars = bool(config.get("copy_sidecars"))
                self._copy_files = copy_sidecars
                self._copy_subtitles = copy_sidecars
            else:
                self._copy_files = bool(config.get("copy_files"))
                self._copy_subtitles = bool(config.get("copy_subtitles"))
            self._sync_delete = bool(config.get("sync_delete"))
            self._refresh_emby = bool(config.get("refresh_emby"))
            self._notify = bool(config.get("notify"))
            self._uriencode = bool(config.get("uriencode"))
            self._monitor_confs = config.get("monitor_confs") or ""
            self._url = config.get("url") or ""
            self._mediaservers = config.get("mediaservers") or []
            self._other_mediaext = config.get("other_mediaext") or self._default_other_mediaext
            # 新增：读取路径替换规则
            if config.get("path_replacements"):
                for replacement in str(config.get("path_replacements")).split("\n"):
                    mapping = self.__parse_mapping_line(replacement)
                    if mapping:
                        source, target = mapping
                        self._path_replacements[source] = target
            self._rmt_mediaext = config.get(
                "rmt_mediaext") or self._default_rmt_mediaext
            # V1.4.0：存在结构化映射规则时以其为准，序列化为旧版文本走原有解析
            if self.__count_rule_slots(config):
                rules = self.__rules_from_config(config)
                self._monitor_confs = self.__rules_to_monitor_confs(rules)
                # 表单删除项只在提交时短暂存在，保存后清理旧槽位，避免删除的
                # 规则在下一次打开配置页时又被空槽位重新占住。
                normalized_config = dict(config)
                self.__remove_rule_config_keys(normalized_config)
                normalized_config.update(self.__rules_to_config_keys(rules, len(rules)))
                normalized_config["monitor_confs"] = self._monitor_confs
                if normalized_config != config:
                    try:
                        self.update_config(normalized_config)
                    except Exception:
                        pass
            if config.get("emby_path"):
                for path in str(config.get("emby_path")).split(","):
                    mapping = self.__parse_mapping_line(path)
                    if mapping:
                        source, target = mapping
                        self._emby_paths[source] = target
        else:
            self._monitor_confs = ""

        self._media_exts = self.__normalise_extensions(self._rmt_mediaext, self._default_rmt_mediaext)
        self._other_exts = self.__normalise_extensions(self._other_mediaext, self._default_other_mediaext)

        self.__ensure_dir_status_refresh(force=True)

        # Saving a disabled plugin still reinitializes it. Validate here so the
        # form can show actionable feedback before a task is ever started.
        if not self._enabled and not self._onlyonce:
            self._config_errors = self.__validate_monitor_confs(self._monitor_confs)

        if self._enabled or self._onlyonce:
            # 定时服务
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._notify:
                # 追加入库消息统一发送服务
                self._scheduler.add_job(self.send_msg, trigger='interval', seconds=15)

            # 读取目录配置
            monitor_confs = str(self._monitor_confs or "").splitlines()
            if not monitor_confs:
                return
            for monitor_conf in monitor_confs:
                # 格式 MoviePilot中云盘挂载本地的路径#MoviePilot中strm生成路径#alist/cd2上115路径#strm格式化
                monitor_conf = str(monitor_conf).strip()
                if not monitor_conf:
                    continue
                # 注释
                if str(monitor_conf).startswith("#"):
                    continue

                monitor = None
                if monitor_conf.count("$") == 1:
                    monitor = str(monitor_conf.split("$")[1])
                    monitor_conf = monitor_conf.split("$")[0]
                category = None
                if monitor_conf.count("@") == 1:
                    category = str(monitor_conf.split("@")[1])
                    monitor_conf = monitor_conf.split("@")[0]
                if str(monitor_conf).count("#") >= 3:
                    local_dir, strm_dir, cloud_dir, format_str = monitor_conf.split("#", 3)
                    local_dir = local_dir.strip()
                    strm_dir = strm_dir.strip()
                    cloud_dir = cloud_dir.strip()
                    format_str = format_str.strip()
                else:
                    logger.error(f"{monitor_conf} 格式错误")
                    self._config_errors.append(f"目录配置格式错误：{monitor_conf}")
                    continue
                # 检查媒体库目录是不是下载目录的子目录
                try:
                    if (strm_dir and (
                            self.__is_path_within(strm_dir, local_dir)
                            or self.__is_path_within(local_dir, strm_dir))):
                        logger.warning(f"{strm_dir} 与 {local_dir} 存在包含关系，无法监控")
                        self.systemmessage.put(f"{strm_dir} 与 {local_dir} 存在包含关系，无法监控")
                        self._config_errors.append(f"目录配置存在包含关系：{local_dir} 与 {strm_dir}")
                        continue
                except Exception as e:
                    logger.debug(str(e))

                if not format_str or not any(token in format_str for token in ("{local_file}", "{cloud_file}")):
                    logger.error(f"{monitor_conf} 格式化模板缺少 {{local_file}} 或 {{cloud_file}}")
                    self.systemmessage.put(f"{local_dir} 格式化模板无效，缺少文件路径占位符")
                    self._config_errors.append(f"{local_dir} 格式化模板缺少 {{local_file}} 或 {{cloud_file}}")
                    continue

                # 存储目录监控配置
                self._strm_dir_conf[local_dir] = strm_dir
                self._cloud_dir_conf[local_dir] = cloud_dir
                self._format_conf[local_dir] = format_str
                self._category_conf[local_dir] = category

                if not monitor:
                    try:
                        if self._monitor:
                            # 兼容模式，目录同步性能降低且NAS不能休眠，但可以兼容挂载的远程共享目录如SMB
                            dir_state, _, _, dir_detail = self.__dir_status(local_dir)
                            if dir_state == "unavailable":
                                logger.warning(
                                    f"{local_dir} 当前不可访问，已跳过实时监控："
                                    f"{dir_detail or '目录不存在或不可访问'}")
                                self.__log_event("MONITOR",
                                                f"{local_dir} 不可访问，实时监控已跳过")
                                continue
                            observer = PollingObserver(timeout=10)
                            self._observer.append(observer)
                            observer.schedule(FileMonitorHandler(local_dir, self), path=local_dir, recursive=True)
                            observer.daemon = True
                            observer.start()
                            logger.info(f"{local_dir} 的Strm生成实时监控服务启动")
                    except Exception as e:
                        err_msg = str(e)
                        if "inotify" in err_msg and "reached" in err_msg:
                            logger.warning(
                                f"云盘实时监控服务启动出现异常：{err_msg}，请在宿主机上（不是docker容器内）执行以下命令并重启："
                                + """
                                                            echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf
                                                            echo fs.inotify.max_user_instances=524288 | sudo tee -a /etc/sysctl.conf
                                                            sudo sysctl -p
                                                            """)
                        else:
                            logger.error(f"{local_dir} 启动x实时监控失败：{err_msg}")
                        self.systemmessage.put(f"{local_dir} 启动实时监控失败：{err_msg}")

            # 运行一次定时服务
            if self._onlyonce:
                logger.info("云盘Strm助手全量执行服务启动，立即运行一次")
                self._scheduler.add_job(func=self.scan, trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="云盘Strm助手全量执行服")
                # 关闭一次性开关
                self._onlyonce = False
                # 保存配置
                self.__update_config()

            # V1.4.0：定时增量扫描，对应设计稿「定时增量扫描」开关
            if self._enabled and self._cron_enabled and self._strm_dir_conf:
                self._scheduler.add_job(
                    func=self.__scheduled_scan, trigger='interval',
                    minutes=self._scan_interval,
                    name="CloudStrm定时增量扫描")
                logger.info(f"定时增量扫描已启动，周期 {self._scan_interval} 分钟")

            # 启动任务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __scheduled_scan(self):
        """定时增量扫描入口：记录轮询事件并复用统一任务执行器。"""
        self.__log_event("POLL", f"定时增量扫描启动（周期 {self._scan_interval} 分钟），轮询 CD2 挂载目录")
        try:
            result = self.scan()
        except Exception as err:
            self.__log_event("FAIL", f"定时增量扫描异常：{err}")
            return
        if isinstance(result, dict) and not result.get("accepted", True):
            self.__log_event("POLL", "已有任务运行中，本次定时扫描跳过")

    def scan(self, scan_path: str = None, mon_path: str = None,
             task_id: str = None, record_task: bool = True, channel=None, userid=None):
        """
        全量执行
        """
        if record_task and not task_id:
            scope = {"path": scan_path or "", "monitor_path": mon_path or ""}
            result = self.__start_task(
                "full_scan", scope,
                lambda _task_id: self.scan(scan_path=scan_path, mon_path=mon_path,
                                           task_id=_task_id, record_task=False),
                channel=channel, userid=userid,
            )
            if isinstance(result, dict) and not result.get("accepted", True):
                logger.info("全量扫描请求被拒绝：已有任务正在运行，跳过本次")
            return result
        logger.info("开始全量执行")
        monitor_paths = [mon_path] if mon_path else list(self._strm_dir_conf.keys())
        for current_mon_path in monitor_paths:
            if current_mon_path not in self._strm_dir_conf:
                continue
            root_path = scan_path if scan_path and self.__is_path_within(scan_path, current_mon_path) else current_mon_path
            # 遍历目录下所有文件
            for root, dirs, files in os.walk(root_path):
                dirs[:] = [directory for directory in dirs
                           if directory.lower() not in self._ignored_dir_names
                           and not directory.startswith(".")]
                # 处理文件
                for file in files:
                    if task_id and self.__task_is_stopped(task_id):
                        return self.__get_task(task_id)
                    source_file = os.path.join(root, file)
                    if self.__is_ignored_path(source_file):
                        continue

                    self.__handle_file(event_path=source_file, mon_path=current_mon_path)
        logger.info("全量执行完成")
        return self.__get_task(task_id) if task_id else None

    @eventmanager.register(EventType.PluginAction)
    def strm_one(self, event: Event = None):
        if event:
            event_data = event.event_data
            if not event_data:
                return
            if event_data.get("action") == "CloudStrmHelper":
                self.scan(channel=event_data.get("channel"), userid=event_data.get("user"))
                return
            if event_data.get("action") != "cloudstrm_file":
                return
            file_path = event_data.get("file_path")
            if not file_path:
                logger.error(f"缺少参数：{event_data}")
                return

            # 遍历所有监控目录
            mon_path = self.__find_monitor_path(file_path)

            if not mon_path:
                logger.error(f"未找到文件 {file_path} 对应的监控目录")
                return

            # 处理单文件
            self.__handle_file(event_path=file_path, mon_path=mon_path)

    def __find_monitor_path(self, file_path: str) -> Optional[str]:
        return _find_monitor_path(file_path, self._strm_dir_conf)
        candidates = [
            mon_path for mon_path in self._strm_dir_conf.keys()
            if self.__is_path_within(file_path, mon_path)
        ]
        return max(candidates, key=lambda path: len(self.__path_key(path))) if candidates else None

    @staticmethod
    def __join_remote_path(root: str, relative: Path) -> str:
        return _join_remote_path(root, relative)
        root = str(root or "")
        relative_text = str(relative).replace("\\", "/")
        if not relative_text or relative_text == ".":
            return root
        if not root:
            return "/" + relative_text.lstrip("/")
        if root.endswith("/"):
            return root + relative_text
        return root + "/" + relative_text

    def __schedule_file(self, event_path: str, mon_path: str):
        key = (mon_path, self.__path_key(event_path))
        with self._event_timers_lock:
            previous = self._event_timers.pop(key, None)
            if previous:
                previous.cancel()
            timer = threading.Timer(self._event_delay, self.__process_scheduled_file,
                                    args=(event_path, mon_path, key))
            timer.daemon = True
            self._event_timers[key] = timer
            timer.start()

    def __process_scheduled_file(self, event_path: str, mon_path: str, key):
        with self._event_timers_lock:
            self._event_timers.pop(key, None)
        if not self.__wait_for_stable_file(event_path):
            return
        # V1.4.0：实时监控事件写入 Live Log Feed
        self.__log_event("MONITOR", f"发现文件变更 -> {event_path}")
        result = self.__handle_file(event_path=event_path, mon_path=mon_path)
        if not result:
            return
        target_file = result.get("target_file") or ""
        if result.get("status") == "success" and str(target_file).lower().endswith(".strm"):
            self.__log_event("STRM-GEN", f"生成文件 -> {target_file}")
        elif result.get("status") == "failed":
            self.__log_event("FAIL", f"{event_path} 处理失败：{result.get('reason') or '未知原因'}")

    def __schedule_directory(self, event_path: str, mon_path: str):
        key = ("directory", mon_path, self.__path_key(event_path))
        with self._event_timers_lock:
            previous = self._event_timers.pop(key, None)
            if previous:
                previous.cancel()
            timer = threading.Timer(self._event_delay, self.__process_scheduled_directory,
                                    args=(event_path, mon_path, key))
            timer.daemon = True
            self._event_timers[key] = timer
            timer.start()

    def __process_scheduled_directory(self, event_path: str, mon_path: str, key):
        with self._event_timers_lock:
            self._event_timers.pop(key, None)
        if Path(event_path).is_dir():
            self.__log_event("POLL", f"目录变更，增量扫描 -> {event_path}")
            self.scan(scan_path=event_path, mon_path=mon_path, record_task=False)

    def __wait_for_stable_file(self, event_path: str) -> bool:
        return _wait_for_stable_file(
            event_path,
            stable_checks=self._stable_checks,
            stable_interval=self._stable_interval,
        )
        """等待挂载文件大小稳定，避免复制到尚未完成的旁车文件。"""
        previous = None
        stable_count = 0
        for _ in range(10):
            try:
                stat = os.stat(event_path)
            except OSError:
                return False
            current = (stat.st_size, getattr(stat, "st_mtime_ns", stat.st_mtime))
            if current == previous:
                stable_count += 1
                if stable_count >= self._stable_checks:
                    return True
            else:
                previous = current
                stable_count = 0
            time.sleep(self._stable_interval)
        logger.warning(f"文件 {event_path} 长时间未稳定，继续尝试同步")
        return Path(event_path).is_file()

    def event_handler(self, event, mon_path: str, text: str, event_path: str,
                      action: str = "created", old_event_path: str = None):
        """
        处理文件变化
        :param event: 事件
        :param mon_path: 监控目录
        :param text: 事件描述
        :param event_path: 事件文件路径
        """
        if '.fuse_hidden' in str(event_path):
            return
        if self.__is_ignored_path(event_path):
            return

        logger.debug(f"监控到文件{text}：{event_path}")
        if action == "deleted":
            if not self._sync_delete:
                return
            # CD2/FUSE 短暂掉线时不要把整个 STRM 媒体库误判为已删除。
            if not Path(mon_path).is_dir():
                logger.warning(f"监控目录 {mon_path} 当前不可访问，跳过删除清理")
                return
            self.__cleanup_source(event_path=event_path, mon_path=mon_path,
                                  is_directory=bool(event.is_directory))
            return

        if action == "moved" and old_event_path and self._sync_delete:
            self.__cleanup_source(event_path=old_event_path, mon_path=mon_path,
                                  is_directory=bool(event.is_directory))

        if event.is_directory:
            if action in {"created", "moved"}:
                self.__schedule_directory(event_path=event_path, mon_path=mon_path)
            return
        self.__schedule_file(event_path=event_path, mon_path=mon_path)

    def __is_ignored_path(self, event_path: str) -> bool:
        return _is_ignored_path(event_path, self._ignored_dir_names)
        path = Path(str(event_path))
        for part in path.parts:
            lower_part = part.lower()
            if lower_part in self._ignored_dir_names or lower_part.startswith("."):
                return True
        return path.name.lower().startswith(".fuse_hidden")

    def __handle_file(self, event_path: str, mon_path: str, force: bool = False,
                      retry_stage: str = "") -> dict:
        task_id = getattr(self._task_context, "task_id", None)
        if task_id and self.__task_is_stopped(task_id):
            result = self.__task_result(event_path, status="skipped", action="stopped",
                                        stage="queue", reason="任务已停止", monitor_path=mon_path)
            self.__task_discover(task_id)
            self.__task_record_result(task_id, result)
            return result
        if task_id and not self.__task_claim_source(task_id, event_path):
            return None
        if task_id:
            self.__task_discover(task_id)
        result = self.__process_file(
            event_path=event_path, mon_path=mon_path, force=force, retry_stage=retry_stage
        )
        if task_id and result:
            self.__task_record_result(task_id, result)
        return result

    def __process_file(self, event_path: str, mon_path: str, force: bool = False,
                       retry_stage: str = "") -> dict:
        """
        同步一个文件
        :param event_path: 事件文件路径
        :param mon_path: 监控目录
        """
        source_path = os.path.normpath(str(event_path))
        processing_key = (mon_path, self.__path_key(source_path))
        with self._state_lock:
            if processing_key in self._processing_paths:
                return self.__task_result(source_path, status="skipped", action="busy",
                                          stage="queue", reason="文件正在处理中",
                                          monitor_path=mon_path)
            self._processing_paths.add(processing_key)
        try:
            source = Path(source_path)
            if not source.is_file():
                return self.__task_result(source_path, status="failed", action="read",
                                          stage="source", reason="源文件不存在或不可读取",
                                          retryable=True, monitor_path=mon_path)
            if self.__is_ignored_path(source_path):
                return self.__task_result(source_path, status="skipped", action="ignore",
                                          stage="filter", reason="文件属于忽略路径",
                                          monitor_path=mon_path)

            cloud_dir = self._cloud_dir_conf.get(mon_path)
            strm_dir = self._strm_dir_conf.get(mon_path)
            format_str = self._format_conf.get(mon_path)
            relative = self.__relative_path(source_path, mon_path)
            if relative is None or not strm_dir:
                logger.warning(f"文件 {source_path} 不在监控目录 {mon_path} 内，跳过")
                return self.__task_result(source_path, status="failed", action="map",
                                          stage="mapping", reason="文件不在有效监控目录内",
                                          retryable=False, monitor_path=mon_path)

            target_file = self.__map_path(source_path, mon_path, strm_dir)
            cloud_file = self.__join_remote_path(cloud_dir, relative)
            suffix = source.suffix.lower()

            if suffix in self._media_exts:
                strm_path = os.path.splitext(target_file)[0] + ".strm"
                if retry_stage in {"push", "emby"}:
                    if retry_stage == "push":
                        push_result = self.__push_strm_file(strm_path, source_path)
                        if not push_result.get("ok"):
                            return self.__task_result(
                                source_path, strm_path, status="failed", action="push",
                                stage="push", reason=push_result.get("reason", "任务推送失败"),
                                retryable=True, monitor_path=mon_path,
                            )
                        return self.__task_result(source_path, strm_path, status="success",
                                                  action="push", stage="push",
                                                  monitor_path=mon_path)
                    refresh_result = self.__refresh_emby_file(strm_path, update_type="Modified")
                    if not refresh_result.get("ok"):
                        return self.__task_result(
                            source_path, strm_path, status="failed", action="refresh",
                            stage="emby", reason=refresh_result.get("reason", "媒体库刷新失败"),
                            retryable=True, monitor_path=mon_path,
                        )
                    return self.__task_result(source_path, strm_path, status="success",
                                              action="refresh", stage="emby",
                                              monitor_path=mon_path)
                strm_content = self.__format_content(
                    format_str=format_str,
                    local_file=source_path,
                    cloud_file=cloud_file,
                    uriencode=self._uriencode,
                )
                if strm_content is None:
                    logger.error(f"{source_path} 未生成 STRM：格式化模板无效")
                    return self.__task_result(
                        source_path, strm_path, status="failed", action="format",
                        stage="format", reason="格式化模板缺少有效占位符",
                        retryable=True, monitor_path=mon_path,
                    )
                strm_existed = Path(strm_path).is_file()
                strm_result = self.__create_strm_file(
                    strm_file=target_file,
                    strm_content=strm_content,
                    source_file=source_path,
                    force=force,
                )

                related_results = []
                for related_file in self.__find_related_files(source):
                    allowed_sources = getattr(self._task_context, "allowed_sources", None)
                    if allowed_sources is not None and self.__path_key(str(related_file)) not in allowed_sources:
                        continue
                    related_target = self.__map_path(str(related_file), mon_path, strm_dir)
                    if related_target:
                        task_id = getattr(self._task_context, "task_id", None)
                        if task_id and not self.__task_claim_source(task_id, str(related_file)):
                            continue
                        if task_id and not self.__task_claim_sidecar(str(related_file)):
                            continue
                        related_result = self.__handle_other_files(
                            event_path=str(related_file), target_file=related_target, force=force
                        )
                        related_results.append(related_result)
                        if task_id:
                            sidecar_task_result = self.__task_result(
                                str(related_file), related_target,
                                status=related_result.get("status", "failed"),
                                action=related_result.get("action", "copy"),
                                stage=related_result.get("stage", "copy"),
                                reason=related_result.get("reason", ""),
                                retryable=bool(related_result.get("retryable")),
                                monitor_path=mon_path,
                            )
                            self.__task_discover(task_id)
                            self.__task_record_result(task_id, sidecar_task_result)

                changed = strm_result.get("status") == "success" or any(
                    item.get("status") == "success" for item in related_results
                )
                if changed and self._refresh_emby and self._mediaservers:
                    update_type = "Modified" if strm_existed else "Created"
                    refresh_result = self.__refresh_emby_file(strm_path, update_type=update_type)
                    if not refresh_result.get("ok"):
                        return self.__task_result(
                            source_path, strm_path, status="failed", action="refresh",
                            stage="emby", reason=refresh_result.get("reason", "媒体库刷新失败"),
                            retryable=True, monitor_path=mon_path,
                        )
                if strm_result.get("status") == "failed":
                    return self.__task_result(
                        source_path, strm_path, status="failed", action=strm_result.get("action", "generate"),
                        stage=strm_result.get("stage", "generate"),
                        reason=strm_result.get("reason", "STRM 文件生成失败"),
                        retryable=bool(strm_result.get("retryable")), monitor_path=mon_path,
                    )
                if strm_result.get("status") == "success" or any(
                    item.get("status") == "success" for item in related_results
                ):
                    action = strm_result.get("action") or "generate"
                    if any(item.get("status") == "success" for item in related_results):
                        action = f"{action},sidecar"
                    return self.__task_result(source_path, strm_path, status="success", action=action,
                                              stage=strm_result.get("stage", "generate"),
                                              monitor_path=mon_path)
                return self.__task_result(
                    source_path, strm_path, status="skipped", action=strm_result.get("action", "skip"),
                    stage=strm_result.get("stage", "generate"),
                    reason=strm_result.get("reason", "目标文件无需更新"), monitor_path=mon_path,
                )
            else:
                if retry_stage == "emby":
                    refresh_result = self.__refresh_emby_file(
                        self.__related_strm_path(target_file), update_type="Modified"
                    )
                    if not refresh_result.get("ok"):
                        return self.__task_result(
                            source_path, target_file, status="failed", action="refresh",
                            stage="emby", reason=refresh_result.get("reason", "媒体库刷新失败"),
                            retryable=True, monitor_path=mon_path,
                        )
                    return self.__task_result(
                        source_path, target_file, status="success", action="refresh",
                        stage="emby", monitor_path=mon_path,
                    )
                copy_result = self.__handle_other_files(event_path=source_path, target_file=target_file,
                                                        force=force)
                if copy_result.get("status") == "success" and self._refresh_emby and self._mediaservers:
                    refresh_result = self.__refresh_emby_file(
                        self.__related_strm_path(target_file), update_type="Modified"
                    )
                    if not refresh_result.get("ok"):
                        return self.__task_result(
                            source_path, target_file, status="failed", action="refresh",
                            stage="emby", reason=refresh_result.get("reason", "媒体库刷新失败"),
                            retryable=True, monitor_path=mon_path,
                        )
                return self.__task_result(
                    source_path, target_file, status=copy_result.get("status", "failed"),
                    action=copy_result.get("action", "copy"),
                    stage=copy_result.get("stage", "copy"),
                    reason=copy_result.get("reason", ""),
                    retryable=bool(copy_result.get("retryable")), monitor_path=mon_path,
                )
        except Exception as e:
            logger.error("目录监控发生错误：%s - %s" % (str(e), traceback.format_exc()))
            return self.__task_result(
                source_path, status="failed", action="process", stage="process",
                reason=str(e), retryable=True, monitor_path=mon_path,
            )
        finally:
            with self._state_lock:
                self._processing_paths.discard(processing_key)

    def __find_related_files(self, media_file: Path) -> List[Path]:
        related = []
        prefix = media_file.stem
        try:
            entries = media_file.parent.iterdir()
            for file in entries:
                if not file.is_file() or file == media_file:
                    continue
                if file.suffix.lower() in self._media_exts:
                    continue
                if file.name.startswith((prefix + ".", prefix + "-", prefix + "_", prefix + " ")):
                    related.append(file)
        except OSError as err:
            logger.warning(f"读取 {media_file.parent} 旁车文件失败：{err}")
        return related

    def __related_strm_path(self, target_file: str) -> str:
        """将旁车文件路径转换为对应 STRM 路径，找不到时返回原路径。"""
        target = Path(target_file)
        if target.suffix.lower() == ".strm":
            return str(target)
        if target.suffix.lower() in self._media_exts:
            return os.path.splitext(str(target))[0] + ".strm"
        try:
            candidates = [
                file for file in target.parent.glob("*.strm")
                if file.is_file()
                and (target.stem == file.stem
                     or target.stem.startswith((file.stem + ".", file.stem + "-",
                                                 file.stem + "_", file.stem + " ")))
            ]
            if candidates:
                return str(max(candidates, key=lambda file: len(file.stem)))
        except OSError:
            pass
        return str(target)

    def __handle_other_files(self, event_path: str, target_file: str, force: bool = False) -> dict:
        """
        处理非媒体文件
        :param event_path: 事件文件路径
        """
        source = Path(event_path)
        suffix = source.suffix.lower()
        should_copy = (
            (self._copy_files and suffix in self._other_exts)
            or (self._copy_subtitles and suffix in self._subtitle_exts)
        )
        if not should_copy:
            return {"status": "skipped", "action": "disabled", "stage": "copy",
                    "reason": "未启用该文件类型的复制"}
        if not source.is_file():
            return {"status": "failed", "action": "copy", "stage": "source",
                    "reason": "旁车文件不存在", "retryable": True}
        try:
            target = Path(target_file)
            if self.__path_key(str(source)) == self.__path_key(str(target)):
                return {"status": "skipped", "action": "same_path", "stage": "copy",
                        "reason": "源文件与目标文件相同"}
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.is_file() and not force:
                source_stat = source.stat()
                target_stat = target.stat()
                if (source_stat.st_size == target_stat.st_size
                        and source_stat.st_mtime_ns == target_stat.st_mtime_ns):
                    return {"status": "skipped", "action": "unchanged", "stage": "copy",
                            "reason": "目标文件内容未变化"}
            temp_file = target.with_name(f".{target.name}.tmp")
            with lock:
                shutil.copy2(str(source), str(temp_file))
                os.replace(str(temp_file), str(target))
            self.__mark_generated_file(str(target))
            logger.debug(f"复制旁车文件 {source} 到 {target}")
            return {"status": "success", "action": "copy", "stage": "copy"}
        except Exception as err:
            logger.error(f"复制旁车文件失败 {source} -> {target_file}：{err}")
            return {"status": "failed", "action": "copy", "stage": "copy",
                    "reason": str(err), "retryable": True}

    def __load_generated_files(self):
        self._generated_files = GeneratedFileManifest.load(self._generated_files_json)

    def __save_generated_files(self):
        with self._manifest_lock:
            with self._state_lock:
                generated_files = set(self._generated_files)
            GeneratedFileManifest.save(self._generated_files_json, generated_files)

    def __mark_generated_file(self, path: str):
        path_key = self.__path_key(path)
        with self._state_lock:
            if path_key in self._generated_files:
                return
            self._generated_files.add(path_key)
            self._manifest_pending = True

        key = ("generated-files-manifest",)
        with self._event_timers_lock:
            previous = self._event_timers.pop(key, None)
            if previous:
                previous.cancel()
            timer = threading.Timer(2.0, self.__flush_generated_files, args=(key,))
            timer.daemon = True
            self._event_timers[key] = timer
            timer.start()

    def __flush_generated_files(self, key=("generated-files-manifest",)):
        with self._event_timers_lock:
            self._event_timers.pop(key, None)
        with self._state_lock:
            pending = self._manifest_pending
            self._manifest_pending = False
        if pending:
            self.__save_generated_files()

    def __remove_file_if_generated(self, path: str):
        path_key = self.__path_key(path)
        with self._state_lock:
            was_generated = path_key in self._generated_files
        if not was_generated:
            return False
        try:
            target = Path(path)
            if target.is_file():
                target.unlink()
            with self._state_lock:
                self._generated_files.discard(path_key)
            logger.debug(f"清理已生成文件 {target}")
            return True
        except OSError as err:
            logger.warning(f"清理已生成文件失败 {path}：{err}")
            return False

    def __cleanup_source(self, event_path: str, mon_path: str, is_directory: bool = False):
        """按源路径清理 STRM 及插件复制的旁车文件。"""
        strm_dir = self._strm_dir_conf.get(mon_path)
        if not strm_dir:
            return
        relative = self.__relative_path(event_path, mon_path)
        if relative is None:
            return
        target = Path(self.__map_path(event_path, mon_path, strm_dir))
        candidates = []
        if is_directory:
            root = target
            if root.exists():
                candidates = [path for path in root.rglob("*") if path.is_file()]
            else:
                prefix = self.__path_key(str(root)) + os.sep
                with self._state_lock:
                    generated_files = list(self._generated_files)
                candidates = [path for path_key in generated_files
                              if path_key.startswith(prefix) for path in [Path(path_key)]]
        else:
            if Path(event_path).suffix.lower() in self._media_exts:
                candidates.append(Path(os.path.splitext(str(target))[0] + ".strm"))
                try:
                    candidates.extend(
                        file for file in target.parent.iterdir()
                        if file.is_file()
                        and (file.name.startswith(target.stem + ".")
                             or file.name.startswith(target.stem + "-")
                             or file.name.startswith(target.stem + "_")
                             or file.name.startswith(target.stem + " "))
                    )
                except OSError:
                    pass
            if target.suffix.lower() in self._subtitle_exts or target.suffix.lower() in self._other_exts:
                candidates.append(target)
        removed_count = 0
        for candidate in candidates:
            if self.__remove_file_if_generated(str(candidate)):
                removed_count += 1
        removed = removed_count > 0
        # V1.4.0：死链清理计数与实时事件
        if removed_count:
            self.__bump_pruned(removed_count)
            self.__log_event("PRUNE", f"云端源文件已删除，同步清理 {removed_count} 个本地文件 <- {event_path}")
        else:
            self.__log_event("PRUNE", f"未发现死链，跳过清理动作 <- {event_path}")
        if removed:
            with self._state_lock:
                self._manifest_pending = False
            self.__save_generated_files()
            if self._refresh_emby and self._mediaservers:
                refresh_path = str(target)
                if Path(event_path).suffix.lower() in self._media_exts:
                    refresh_path = os.path.splitext(refresh_path)[0] + ".strm"
                elif not is_directory:
                    refresh_path = self.__related_strm_path(refresh_path)
                self.__refresh_emby_file(refresh_path, update_type="Deleted")
    @staticmethod
    def __format_content(format_str: str, local_file: str, cloud_file: str, uriencode: bool):
        return _format_content(format_str, local_file, cloud_file, uriencode)
        """
        格式化strm内容
        """
        format_str = str(format_str or "")
        if not any(token in format_str for token in ("{local_file}", "{cloud_file}")):
            return None
        if "{cloud_file}" in format_str:
            if uriencode:
                # 对盘符之后的所有内容进行url转码
                cloud_file = urllib.parse.quote(cloud_file, safe='')
            else:
                # 替换路径中的\为/
                cloud_file = cloud_file.replace("\\", "/")
            format_str = format_str.replace("{cloud_file}", cloud_file)
        if "{local_file}" in format_str:
            format_str = format_str.replace("{local_file}", local_file)
        if "{local_file}" in format_str or "{cloud_file}" in format_str:
            return None
        return format_str

    def __push_strm_file(self, strm_file: str, source_file: str = None) -> dict:
        if not self._url:
            return {"ok": True}
        try:
            content = Path(strm_file).read_text(encoding="utf-8")
            response = RequestUtils(content_type="application/json").post(
                url=self._url, json={"path": content, "type": "add"}
            )
            if response is None:
                return {"ok": False, "reason": "任务推送未收到响应"}
            if getattr(response, "status_code", 0) not in range(200, 300):
                return {"ok": False, "reason": f"任务推送返回 HTTP {response.status_code}"}
            return {"ok": True}
        except Exception as err:
            logger.warning(f"STRM 任务推送失败 {strm_file}：{err}")
            return {"ok": False, "reason": str(err)}

    def __create_strm_file(self, strm_file: str, strm_content: str, source_file: str = None,
                           force: bool = False) -> dict:

        """
        生成strm文件
        :param library_dir:
        :param dest_dir:
        :param dest_file:
        """
        try:
            # 文件
            if not Path(strm_file).parent.exists():
                logger.debug(f"创建目标文件夹 {Path(strm_file).parent}")
                os.makedirs(Path(strm_file).parent, exist_ok=True)

            # 构造.strm文件路径
            strm_file = os.path.join(Path(strm_file).parent, f"{os.path.splitext(Path(strm_file).name)[0]}.strm")

            # 媒体文件
            if Path(strm_file).exists() and not (self._cover or force):
                logger.debug(f"目标文件 {strm_file} 已存在")
                return {"status": "skipped", "action": "exists", "stage": "generate",
                        "reason": "目标文件已存在且未开启覆盖"}
            # 新增：应用自定义路径替换规则
            for source, target in self._path_replacements.items():
                if source in strm_content:
                    strm_content = strm_content.replace(source, target)
                    logger.debug(f"应用路径替换规则: {source} -> {target}")

            # 原子写入，避免媒体服务器读到半截 STRM。
            temp_file = Path(strm_file).with_name(f".{Path(strm_file).name}.tmp")
            with open(temp_file, 'w', encoding='utf-8', newline="") as f:
                f.write(strm_content)
            os.replace(str(temp_file), strm_file)
            self.__mark_generated_file(strm_file)

            logger.debug(f"创建strm文件成功 {strm_file} -> {strm_content}")
            if self._url and source_file and Path(source_file).suffix.lower() in self._media_exts:
                push_result = self.__push_strm_file(strm_file, source_file)
                if not push_result.get("ok"):
                    return {"status": "failed", "action": "push", "stage": "push",
                            "reason": push_result.get("reason", "任务推送失败"),
                            "retryable": True}

            if self._notify and source_file and Path(source_file).suffix.lower() in self._media_exts:
                # 发送消息汇总
                file_meta = MetaInfoPath(Path(strm_file))

                pattern = r'tmdbid=(\d+)'
                # 提取 tmdbid
                match = re.search(pattern, strm_file)
                if match:
                    tmdbid = match.group(1)
                    file_meta.tmdbid = tmdbid

                key = f"{file_meta.cn_name} ({file_meta.year}){f' {file_meta.season}' if file_meta.season else ''}"
                with self._state_lock:
                    media_list = self._medias.get(key) or {}
                    episodes = media_list.get("episodes") or []
                    if file_meta.begin_episode:
                        if int(file_meta.begin_episode) not in episodes:
                            episodes.append(int(file_meta.begin_episode))
                    self._medias[key] = {
                        "episodes": episodes,
                        "file_meta": file_meta,
                        "type": "tv" if file_meta.season else "movie",
                        "time": datetime.now()
                    }

            return {"status": "success", "action": "overwrite" if force else "generate",
                    "stage": "generate"}
        except Exception as e:
            logger.error(f"创建strm文件失败 {strm_file} -> {str(e)}")
            return {"status": "failed", "action": "generate", "stage": "generate",
                    "reason": str(e), "retryable": True}
        return {"status": "failed", "action": "generate", "stage": "generate",
                "reason": "STRM 文件生成失败", "retryable": True}

    def __refresh_emby_file(self, strm_file: str, update_type: str = "Created"):
        """
        通知emby刷新文件
        """
        try:
            emby_servers = self.mediaserver_helper.get_services(
                name_filters=self._mediaservers, type_filter="emby"
            )
            if not emby_servers:
                logger.error("未配置Emby媒体服务器")
                return {"ok": False, "reason": "未配置可用的 Emby 媒体服务器"}

            mapped_file = self.__get_path(paths=self._emby_paths, file_path=strm_file)
            failures = []
            for emby_name, emby_server in emby_servers.items():
                try:
                    res = emby_server.instance.post_data(
                        url=f'[HOST]emby/Library/Media/Updated?api_key=[APIKEY]&reqformat=json',
                        data=json.dumps({
                            "Updates": [{"Path": mapped_file, "UpdateType": update_type}]
                        }),
                        headers={"Content-Type": "application/json"}
                    )
                    if res and res.status_code in [200, 204]:
                        logger.debug(f"媒体服务器 {emby_name} 已刷新 {mapped_file}")
                    else:
                        status_code = res.status_code if res else "无响应"
                        failures.append(f"{emby_name}: HTTP {status_code}")
                        logger.error(f"通知媒体服务器 {emby_name} 刷新文件 {mapped_file} 失败，错误码：{status_code}")
                except Exception as err:
                    failures.append(f"{emby_name}: {err}")
                    logger.error(f"通知媒体服务器 {emby_name} 刷新新增文件失败：{err}")
            return {"ok": not failures,
                    "reason": "；".join(failures) if failures else ""}
        except Exception as err:
            logger.error(f"获取 Emby 服务或刷新文件失败：{err}")
            return {"ok": False, "reason": str(err)}

    def __get_path(self, paths, file_path: str):
        return _map_library_path(paths, file_path)
        """
        路径转换
        """
        if paths:
            matches = [path for path in paths.keys()
                       if self.__is_path_within(file_path, path)]
            if matches:
                library_path = max(matches, key=lambda path: len(self.__path_key(path)))
                relative = self.__relative_path(file_path, library_path)
                target_root = str(paths.get(library_path) or "")
                if relative is not None:
                    relative_text = str(relative)
                    if target_root.startswith("/"):
                        return target_root.rstrip("/") + "/" + relative_text.replace("\\", "/")
                    return os.path.normpath(os.path.join(target_root, relative_text))
        # 未匹配到路径，返回原路径
        return file_path
    def __iter_task_files(self, path: str):
        source = Path(path)
        if source.is_file():
            yield str(source)
            return
        if not source.is_dir():
            return
        for root, dirs, files in os.walk(str(source)):
            dirs[:] = [directory for directory in dirs
                       if directory.lower() not in self._ignored_dir_names
                       and not directory.startswith(".")]
            for file_name in files:
                file_path = os.path.join(root, file_name)
                if not self.__is_ignored_path(file_path):
                    yield file_path

    def __run_targeted_paths(self, task_id: str, targets: list):
        for target in targets:
            if self.__task_is_stopped(task_id):
                return
            path = target.get("path")
            mon_path = target.get("monitor_path")
            limit = target.get("limit")
            if limit is not None and Path(path).is_dir():
                sub_paths = [Path(path) / item for item in os.listdir(path)
                             if (Path(path) / item).is_dir()]
                sub_paths.sort(key=lambda item: item.stat().st_mtime, reverse=True)
                paths = []
                for sub_path in sub_paths[:int(limit)]:
                    paths.extend(self.__iter_task_files(str(sub_path)))
            else:
                paths = self.__iter_task_files(path)
            for source_file in paths:
                if self.__task_is_stopped(task_id):
                    return
                self.__handle_file(event_path=source_file, mon_path=mon_path)

    def __resolve_command_targets(self, args: str) -> Tuple[list, str]:
        args = str(args or "").strip()
        if not args:
            return [], "缺少同步路径或分类参数"

        direct_path = Path(args)
        direct_mon = self.__find_monitor_path(args)
        if direct_mon and direct_path.exists():
            return [{"path": str(direct_path), "monitor_path": direct_mon}], ""

        args_arr = args.split(maxsplit=1)
        category = args_arr[0] if len(args_arr) == 2 else None
        remainder = args_arr[1] if len(args_arr) == 2 else args
        limit = int(remainder) if category and remainder.isdigit() else None
        if category and Path(category).is_dir() and limit is not None:
            mon_path = self.__find_monitor_path(category)
            if not mon_path:
                return [], f"未找到 {category} 对应的监控目录"
            return [{"path": category, "monitor_path": mon_path, "limit": limit}], ""

        targets = []
        for mon_path, mon_category in self._category_conf.items():
            if not mon_category:
                continue
            mon_categories = CloudStrmHelper.__category_list(mon_category)
            requested_categories = CloudStrmHelper.__category_list(category) if category else []
            category_matched = bool(
                requested_categories and any(tag in mon_categories for tag in requested_categories)
            ) or bool(category and str(category) in str(mon_category))
            if category and category_matched:
                parent_path = os.path.join(mon_path, category)
                if limit is not None:
                    if Path(parent_path).is_dir():
                        targets.append({"path": parent_path, "monitor_path": mon_path,
                                        "limit": limit})
                else:
                    requested = os.path.join(parent_path, remainder)
                    related_paths = self.__find_related_paths(requested)
                    targets.extend({"path": path, "monitor_path": mon_path}
                                    for path in related_paths)
            elif not category and any(tag == str(args).strip() for tag in mon_categories):
                parent_path = os.path.join(mon_path, args)
                if Path(parent_path).exists():
                    targets.append({"path": parent_path, "monitor_path": mon_path})

        if targets:
            return targets, ""
        return [], f"未检索到 {args}，请检查输入是否正确"

    @eventmanager.register(EventType.PluginAction)
    def remote_sync_one(self, event: Event = None):
        if not event:
            return
        event_data = event.event_data or {}
        if event_data.get("action") != "strm_one":
            return
        args = event_data.get("arg_str")
        targets, error = self.__resolve_command_targets(args)
        if error:
            logger.error(error)
            if event_data.get("user"):
                self.post_message(channel=event_data.get("channel"), title=error,
                                  userid=event_data.get("user"))
            return
        result = self.__start_task(
            "targeted", {"path": args or "", "targets": targets},
            lambda task_id: self.__run_targeted_paths(task_id, targets),
            channel=event_data.get("channel"), userid=event_data.get("user"),
        )
        if not result.get("accepted") and event_data.get("user"):
            self.post_message(channel=event_data.get("channel"),
                              title=f"已有任务正在运行：{result.get('task_id')}",
                              userid=event_data.get("user"))

    @staticmethod
    def __find_related_paths(base_path):
        related_paths = []
        base_dir = os.path.dirname(base_path)
        base_name = os.path.basename(base_path)

        try:
            entries = os.listdir(base_dir)
        except OSError:
            return related_paths
        for entry in entries:
            if entry.startswith(base_name):
                full_path = os.path.join(base_dir, entry)
                if os.path.isdir(full_path):
                    related_paths.append(full_path)

        # 按照修改时间倒序排列
        related_paths.sort(key=lambda path: os.path.getmtime(path), reverse=True)

        return related_paths
    def send_msg(self):
        """Check completed media groups and send aggregated notifications."""
        with self._state_lock:
            pending_medias = list(self._medias.items())
        for key, media_list in pending_medias:
            if not media_list or not media_list.get("time"):
                continue
            if (datetime.now() - media_list["time"]).total_seconds() <= int(self._interval) \
                    and str(media_list.get("type")) != "movie":
                continue
            try:
                if self._notify:
                    file_meta = media_list.get("file_meta")
                    episodes = media_list.get("episodes") or []
                    file_count = len(episodes) if episodes else 1
                    if str(media_list.get("type")) == "tv":
                        season_episode = f"{key} {StringUtils.format_ep(episodes)}"
                        media_type = MediaType.TV
                    else:
                        season_episode = key
                        media_type = MediaType.MOVIE
                    mediainfo = self.chain.recognize_media(
                        meta=file_meta, mtype=media_type, tmdbid=file_meta.tmdbid)
                    image = None
                    if mediainfo:
                        image = mediainfo.backdrop_path or mediainfo.poster_path
                    self.send_transfer_message(
                        msg_title=season_episode, file_count=file_count, image=image)
            except Exception as err:
                logger.warning("Send media notification failed for %s: %s", key, err)
            with self._state_lock:
                if self._medias.get(key) is media_list:
                    self._medias.pop(key, None)

    def send_transfer_message(self, msg_title, file_count, image):
        """
        发送消息
        """
        # 发送
        self.post_message(
            mtype=NotificationType.Plugin,
            title=f"{msg_title} Strm已生成", text=f"共{file_count}个文件",
            image=image,
            link=settings.MP_DOMAIN('#/history'))

    def __update_config(self):
        """
        更新配置
        """
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cover": self._cover,
            "notify": self._notify,
            "monitor": self._monitor,
            "interval": self._interval,
            "copy_files": self._copy_files,
            "copy_subtitles": self._copy_subtitles,
            "copy_sidecars": self._copy_files and self._copy_subtitles,
            "sync_delete": self._sync_delete,
            "refresh_emby": self._refresh_emby,
            "uriencode": self._uriencode,
            "url": self._url,
            "monitor_confs": self._monitor_confs,
            "rmt_mediaext": self._rmt_mediaext,
            "other_mediaext": self._other_mediaext,
            "mediaservers": self._mediaservers,
            "emby_path": ",".join([f"{source}=>{target}" for source, target in self._emby_paths.items()]),
            # 新增：路径替换规则
            "path_replacements": "\n".join([f"{source}=>{target}" for source, target in
                                            self._path_replacements.items()]) if self._path_replacements else "",
            "cron_enabled": self._cron_enabled,
            "scan_interval": self._scan_interval,
        })

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        :return: 命令关键字、事件、描述、附带数据
        """
        return [
            {
                "cmd": "/cloud_strm_companion",
                "event": EventType.PluginAction,
                "desc": "云盘Strm助手同步",
                "category": "",
                "data": {
                    "action": "CloudStrmHelper"
                }
            },
            {
                "cmd": "/strm",
                "event": EventType.PluginAction,
                "desc": "定向云盘Strm同步",
                "category": "",
                "data": {
                    "action": "strm_one"
                }
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/tasks",
                "endpoint": self.__api_get_tasks,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "读取 CloudStrm 任务列表",
            },
            {
                "path": "/tasks/{task_id}",
                "endpoint": self.__api_get_task,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "读取 CloudStrm 任务详情",
            },
            {
                "path": "/tasks",
                "endpoint": self.__api_start_task,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "启动 CloudStrm 扫描任务",
            },
            {
                "path": "/tasks/{task_id}/retry",
                "endpoint": self.__api_retry_task,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "重试 CloudStrm 失败项",
            },
            {
                "path": "/tasks/{task_id}/selection",
                "endpoint": self.__api_toggle_task_item,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "选择 CloudStrm 失败项",
            },
            {
                "path": "/config/toggle",
                "endpoint": self.__api_config_toggle,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "切换 CloudStrm 策略开关（暂存）",
            },
            {
                "path": "/config/save",
                "endpoint": self.__api_config_save,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "保存 CloudStrm 页面暂存配置",
            },
            {
                "path": "/config/discard",
                "endpoint": self.__api_config_discard,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "放弃 CloudStrm 页面暂存配置",
            },
            {
                "path": "/mappings/delete",
                "endpoint": self.__api_mapping_delete,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "删除 CloudStrm 映射规则（暂存）",
            },
            {
                "path": "/mappings/edit",
                "endpoint": self.__api_mapping_edit,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "标记 CloudStrm 映射规则编辑入口",
            },
            {
                "path": "/mappings/{index}/monitor",
                "endpoint": self.__api_mapping_toggle_monitor,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "切换 CloudStrm 单条映射规则实时监控（暂存）",
            },
            {
                "path": "/extensions/remove",
                "endpoint": self.__api_extension_remove,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "移除 CloudStrm 监控扩展名（暂存）",
            },
        ]
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """Render settings with the same visual tokens as the task dashboard."""
        card_style = self._ui_card_style
        field_style = (
            "--v-input-control-height:34px;--v-field-padding-start:10px;"
            "--v-field-padding-end:12px;font-size:14px;line-height:1.4;"
            "background:#0d1117;border-radius:6px;"
        )

        def switch(model, label, color="primary"):
            return {
                "component": "VSwitch",
                "props": {
                    "model": model, "label": label, "color": color,
                    "inset": True, "density": "compact",
                    "hideDetails": True, "class": "cloudstrm-config-switch",
                    "style": self._ui_switch_style,
                },
            }

        def field(component, model, label, **props):
            values = {
                "model": model, "label": label, "density": "compact",
                "variant": "outlined", "hideDetails": True,
                "class": "cloudstrm-config-field", "style": field_style,
            }
            values.update(props)
            return {"component": component, "props": values}

        def col(content, md=12, class_name="pa-1"):
            return {
                "component": "VCol",
                "props": {"cols": 12, "md": md, "class": class_name},
                "content": content if isinstance(content, list) else [content],
            }

        def row(*columns, class_name="ma-0 mb-2"):
            return {
                "component": "VRow",
                "props": {"noGutters": True, "class": class_name},
                "content": list(columns),
            }

        def icon(icon_name, size="small"):
            return {"component": "VIcon", "props": {"icon": icon_name, "size": size}}

        def section(title, icon_name, content):
            return {
                "component": "VCard",
                "props": {
                    "variant": "flat", "class": "cloudstrm-config-section mb-4",
                    "style": card_style,
                },
                "content": [
                    {
                        "component": "div",
                        "props": {
                            "style": ("display:flex;align-items:center;gap:8px;min-height:34px;"
                                      "padding:16px 16px 12px;border-bottom:1px solid #21262d;margin-bottom:12px;"),
                        },
                        "content": [
                            icon(icon_name),
                            {"component": "span", "props": {"style": "font-size:15px;font-weight:600;"},
                             "text": title},
                        ],
                    },
                    {"component": "VCardText", "props": {"style": "padding:0 16px 16px!important;"},
                     "content": content},
                ],
            }

        def banner(text, tone="info"):
            colors = {
                "info": ("#38bdf8", "rgba(56,189,248,.10)", "#bae6fd", "i"),
                "warning": ("#d29922", "rgba(210,153,34,.12)", "#f0d98a", "!"),
                "error": ("#f85149", "rgba(248,81,73,.12)", "#fecaca", "!"),
            }
            border, background, foreground, mark = colors.get(tone, colors["info"])
            return {
                "component": "div",
                "html": (
                    f'<div style="display:flex;align-items:center;gap:12px;min-height:42px;'
                    f'padding:8px 12px;margin:0 0 16px;border:1px solid {border};'
                    f'border-radius:6px;background:{background};color:{foreground};font-size:12px;">'
                    f'<span style="width:22px;height:22px;display:inline-flex;align-items:center;'
                    f'justify-content:center;border-radius:50%;background:{border};color:#fff;'
                    f'font-weight:700;flex:0 0 auto;">{mark}</span>'
                    f'<span>{html_escape(text)}</span></div>'
                ),
            }

        emby_configs = self.mediaserver_helper.get_configs() if self.mediaserver_helper else {}
        emby_items = [
            {"title": config.name, "value": config.name}
            for config in emby_configs.values() if getattr(config, "type", "") == "emby"
        ]
        saved_config = self.__current_saved_config()
        form_rules = self.__rules_from_config(saved_config) if saved_config else \
            self.__rules_from_monitor_confs(self._monitor_confs)
        form_status_color = "#3fb950" if self._enabled else "#8b949e"
        form_status_text = "已启用" if self._enabled else "未启用"
        form_header = {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-4", "style": card_style},
            "content": [{
                "component": "VCardText",
                "props": {"style": "padding:16px!important;"},
                "content": [{
                    "component": "VRow",
                    "props": {"align": "center", "noGutters": True},
                    "content": [
                        {
                            "component": "VCol",
                            "props": {"cols": 12, "md": 7},
                            "content": [{
                                "component": "div",
                                "html": (
                                    "<div style=\"color:#f0f6fc;font-size:15px;font-weight:600;\">插件配置</div>"
                                    "<div style=\"color:#8b949e;font-size:12px;margin-top:3px;\">"
                                    "OpenList + CD2 监控与 STRM 生成规则</div>"
                                ),
                            }],
                        },
                        {
                            "component": "VCol",
                            "props": {
                                "cols": 12,
                                "md": 5,
                                "class": "d-flex flex-wrap align-center justify-end mt-2 mt-md-0",
                                "style": "gap:8px;",
                            },
                            "content": [
                                {
                                    "component": "div",
                                    "html": (
                                        f"<span style='display:inline-flex;align-items:center;gap:6px;background:#1f293d;"
                                        f"border:1px solid #30363d;border-radius:4px;padding:4px 8px;color:{form_status_color};font-size:11px;'>"
                                        f"<span style='width:6px;height:6px;border-radius:50%;background:{form_status_color};'></span>"
                                        f"{form_status_text}</span>"
                                    ),
                                },
                                {
                                    "component": "VBtn",
                                    "props": {
                                        "prependIcon": "mdi-text-box-search-outline",
                                        "size": "small",
                                        "variant": "flat",
                                        "style": "height:34px;border-radius:6px;background:#21262d;color:#c9d1d9;border:1px solid #30363d;",
                                    },
                                    "text": "查看任务",
                                    "events": {
                                        "click": {
                                            "api": f"plugin/{self.__class__.__name__}/tasks",
                                            "method": "GET",
                                            "params": {},
                                        }
                                    },
                                },
                            ],
                        },
                    ],
                }],
            }],
        }
        rule_slot_count = len(form_rules) + 1

        # 规则摘要仅用于快速查看，编辑与删除在下方规则卡片中完成。
        summary_rows = []
        for rule_index, rule in enumerate(form_rules):
            tags = self.__category_list(rule.get("category")) or ["未分类"]
            tag_html = "".join(
                f'<span style="display:inline-block;margin:0 3px 3px 0;padding:3px 8px;'
                f'border-radius:4px;background:#1f293d;color:#38bdf8;font-size:11px;">'
                f"{html_escape(tag)}</span>" for tag in tags
            )
            local = html_escape(rule.get("local") or "-")
            strm = html_escape(rule.get("strm") or "-")
            cloud = html_escape(rule.get("cloud") or "-")
            format_str = html_escape(rule.get("format") or "-")
            summary_rows.append(
                '<div style="display:grid;grid-template-columns:17% 39% 31% 13%;'
                'align-items:center;min-height:68px;padding:8px 12px;border-top:1px solid #21262d;">'
                f'<div style="min-width:0;">{tag_html}</div>'
                f'<div style="min-width:0;font-family:Consolas,monospace;font-size:12px;line-height:1.8;'
                f'color:#e2e8f0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'
                f'title="{local}">CD2: {local}<br><span style="color:#94a3b8;">STRM: {strm}</span></div>'
                f'<div style="min-width:0;font-family:Consolas,monospace;font-size:12px;line-height:1.8;'
                f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap;title="{format_str}">'
                f'{cloud}<br><span style="color:#64748b;">{format_str}</span></div>'
                f'<div style="text-align:center;color:#94a3b8;font-size:11px;">规则 {rule_index + 1}</div></div>'
            )
        if not summary_rows:
            summary_rows.append(
                '<div style="padding:18px;text-align:center;color:#94a3b8;font-size:13px;">'
                '暂无映射规则，请在下方新增规则编辑区填写。</div>'
            )
        summary_table = {
            "component": "div",
            "html": (
                '<div style="border:1px solid #30363d;border-radius:10px;overflow:hidden;background:#0d1117;">'
                '<div style="display:grid;grid-template-columns:17% 39% 31% 13%;padding:12px;'
                'background:#161b22;color:#8b949e;font-size:12px;font-weight:600;">'
                '<span>分类标签</span><span>CD2 挂载目录 / STRM 生成目录</span>'
                '<span>OpenList 云盘目录 &amp; 格式化模板</span><span style="text-align:center">规则编号</span>'
                '</div>' + ''.join(summary_rows) + '</div>'
            ),
        }

        existing_rule_forms = []
        new_rule_form = None
        for rule_index in range(rule_slot_count):
            is_new_rule = rule_index >= len(form_rules)
            rule = form_rules[rule_index] if rule_index < len(form_rules) else {}
            rule_content = [
                row(
                    col({
                        "component": "VCombobox",
                        "props": {
                            "model": f"rule_{rule_index}_category", "label": "分类标签",
                            "multiple": True, "chips": True, "smallChips": True,
                            "closableChips": True, "items": [],
                            "placeholder": "选择或输入...", "density": "compact",
                            "variant": "outlined", "hideDetails": True,
                            "class": "cloudstrm-config-field", "style": field_style,
                        },
                    }, 4),
                    col(field("VTextField", f"rule_{rule_index}_local",
                              "CD2 挂载目录（MoviePilot 中路径）",
                              placeholder="/CloudNAS/CloudDrive/WebDrive/电影"), 6),
                    col(switch(f"rule_{rule_index}_monitor", "实时监控"), 2),
                ),
                row(
                    col(field("VTextField", f"rule_{rule_index}_strm", "STRM 生成目录",
                              placeholder="/CloudNAS/云盘Strm/media/电影"), 4),
                    col(field("VTextField", f"rule_{rule_index}_cloud", "OpenList 云盘目录",
                              placeholder="/media/电影"), 4),
                    col(field("VTextField", f"rule_{rule_index}_format",
                              "STRM 格式化模板（必须包含 {cloud_file}）",
                              placeholder="http://192.168.1.10:5244/d{cloud_file}"), 4),
                ),
            ]
            if is_new_rule:
                rule_content.append(banner("填写完整后保存配置，即可新增这条映射规则。"))
            else:
                rule_content.append({
                    "component": "VCheckbox",
                    "props": {
                        "model": f"rule_{rule_index}_delete",
                        "label": "删除此映射规则（保存后生效）",
                        "color": "error", "density": "compact",
                        "hideDetails": True,
                    },
                })
            rule_form = {
                "component": "div",
                "props": {
                    "style": ("margin-top:16px;padding:14px 16px 12px;background:#161b22;"
                              "border:1px solid #30363d;border-radius:10px;"),
                },
                "content": [
                    {"component": "div",
                     "props": {"style": ("padding-bottom:10px;margin-bottom:4px;color:#e2e8f0;"
                                          "font-size:14px;font-weight:600;border-bottom:1px solid #21262d;")},
                     "text": "新增映射规则" if is_new_rule else f"映射规则 {rule_index + 1}"},
                    *rule_content,
                ],
            }
            if is_new_rule:
                new_rule_form = rule_form
            else:
                existing_rule_forms.append(rule_form)

        mapping_content = [summary_table]
        mapping_content.extend(existing_rule_forms)
        mapping_content.append(new_rule_form)

        form_content = [form_header]
        if self._config_errors:
            form_content.append(banner("；".join(self._config_errors), "error"))
        form_content.extend([
            banner("监控方式：OpenList + CD2。通过 CD2 挂载目录感知变化，STRM 内容采用 OpenList 模板渲染。"),
            section("基础与文件设置", "mdi-cog", [
                row(
                    col(switch("enabled", "启用插件"), 3),
                    col(switch("monitor", "OpenList + CD2 实时监控"), 3),
                    col(switch("cron_enabled", "定时增量扫描"), 3),
                    col(switch("notify", "任务入库通知"), 3),
                ),
                row(
                    col(field("VTextField", "interval", "入库通知延迟（秒）",
                              type="number", min=1, placeholder="10"), 3),
                    col(field("VTextField", "scan_interval", "定时扫描周期（分钟）",
                              type="number", min=5, placeholder="30"), 3),
                    col(switch("cover", "覆盖已存在文件"), 3),
                    col(switch("copy_sidecars", "复制旁车/字幕文件"), 3),
                ),
            ]),
            section("路径监控与 STRM 映射策略", "mdi-folder-sync", mapping_content),
            section("媒体与高级设置", "mdi-movie-open", [
                row(
                    col(field("VTextarea", "rmt_mediaext", "视频格式扩展名", rows=2,
                              placeholder=self._default_rmt_mediaext), 6),
                    col(field("VTextarea", "other_mediaext", "非媒体格式 / 忽略文件", rows=2,
                              placeholder=self._default_other_mediaext), 6),
                ),
                row(
                    col({"component": "VSelect", "props": {
                        "multiple": True, "chips": True, "clearable": True,
                        "model": "mediaservers", "label": "媒体服务器类型",
                        "items": emby_items, "density": "compact", "variant": "outlined",
                        "hideDetails": True, "class": "cloudstrm-config-field",
                        "style": field_style,
                    }}, 4),
                    col(switch("refresh_emby", "生成后刷新 Emby"), 4),
                    col(switch("uriencode", "云盘路径 URL 编码"), 4),
                ),
                row(col(field("VTextField", "url", "任务推送 Webhook URL",
                             placeholder="http://127.0.0.1:5001/api/v1/webhook"), 12)),
                {
                    "component": "VExpansionPanels",
                    "props": {"variant": "accordion", "class": "mt-4",
                              "style": "border:1px solid #30363d;border-radius:6px;overflow:hidden;"},
                    "content": [{
                        "component": "VExpansionPanel",
                        "props": {"elevation": 0, "style": "background:#0d1117;color:#c9d1d9;"},
                        "content": [
                            {"component": "VExpansionPanelTitle", "text": "路径兼容设置（可选）"},
                            {"component": "VExpansionPanelText", "content": [row(
                                col(field("VTextarea", "emby_path", "媒体库路径映射", rows=2,
                                          placeholder="本地路径=>Emby路径，多组用英文逗号分隔"), 6),
                                col(field("VTextarea", "path_replacements", "STRM 路径替换规则", rows=2,
                                          placeholder="源路径=>目标路径，每行一条；兼容旧版冒号格式"), 6),
                            )]},
                        ],
                    }],
                },
                {
                    "component": "VExpansionPanels",
                    "props": {"variant": "accordion", "class": "mt-3",
                              "style": "border:1px solid #30363d;border-radius:6px;overflow:hidden;"},
                    "content": [{
                        "component": "VExpansionPanel",
                        "props": {"elevation": 0, "style": "background:#0d1117;color:#c9d1d9;"},
                        "content": [
                            {"component": "VExpansionPanelTitle", "text": "执行与清理设置（可选）"},
                            {"component": "VExpansionPanelText", "content": [row(
                                col(switch("onlyonce", "保存后立即执行一次"), 6),
                                col(switch("sync_delete", "同步删除生成文件"), 6),
                            )]},
                        ],
                    }],
                },
            ]),
        ])

        form = [{
            "component": "VForm",
            "props": {
                "class": "cloudstrm-config-form",
                "style": "background:#0d1117;color:#e5e7eb;padding:0 0 16px;",
            },
            "content": form_content,
        }]
        model = {
            "enabled": False, "notify": False, "monitor": False, "cover": False,
            "onlyonce": False, "copy_files": self._copy_files,
            "copy_sidecars": self._copy_files and self._copy_subtitles,
            "uriencode": False, "copy_subtitles": self._copy_subtitles,
            "sync_delete": False, "refresh_emby": False,
            "mediaservers": [], "emby_path": "",
            "interval": 10, "url": "",
            "other_mediaext": self._default_other_mediaext,
            "rmt_mediaext": self._default_rmt_mediaext, "path_replacements": "",
            "cron_enabled": True, "scan_interval": 30,
        }
        for rule_index in range(rule_slot_count):
            rule = form_rules[rule_index] if rule_index < len(form_rules) else {}
            model[f"rule_{rule_index}_category"] = self.__category_list(
                rule.get("category", ""))
            model[f"rule_{rule_index}_local"] = rule.get("local", "")
            model[f"rule_{rule_index}_strm"] = rule.get("strm", "")
            model[f"rule_{rule_index}_cloud"] = rule.get("cloud", "")
            model[f"rule_{rule_index}_format"] = rule.get("format", "")
            model[f"rule_{rule_index}_monitor"] = rule.get("monitor", True)
            model[f"rule_{rule_index}_delete"] = False
        return form, model

    def get_page(self) -> List[dict]:
        data = self.__api_tasks()
        current = data.get("current") or {}
        current_stats = current.get("stats") or {}
        summaries = data.get("tasks") or []
        retry_relations = {}
        for related_task in summaries:
            related_scope = related_task.get("scope") or {}
            source_task_id = related_scope.get("source_task_id")
            if related_task.get("kind") == "retry" and source_task_id:
                retry_relations.setdefault(source_task_id, []).append(related_task.get("id"))
        status_names = {
            "running": "运行中", "success": "成功", "partial": "部分成功",
            "failed": "失败", "interrupted": "已中断",
        }
        kind_names = {"full_scan": "全量扫描", "targeted": "定向同步", "retry": "失败重试"}

        status_colors = {
            "running": "info", "success": "success", "partial": "warning",
            "failed": "error", "interrupted": "grey",
        }

        def chip(text, color="default"):
            return {"component": "VChip", "props": {"size": "small", "color": color,
                                                       "variant": "tonal"}, "text": text}

        # 最近任务：富列表呈现，状态带颜色，行内直接打开详情，最多展示 10 条
        task_item_views = []
        for task in summaries[:10]:
            task_id_value = task.get("id")
            if not task_id_value:
                continue
            task_status = task.get("status") or ""
            related_retries = retry_relations.get(task_id_value) or []
            task_time = str(task.get('finished_at') or task.get('created_at') or '-')
            task_duration = task.get('duration_seconds', 0) or 0
            task_item_views.append({
                "component": "VRow",
                "props": {"align": "center", "noGutters": True,
                          "class": "py-1 task-row",
                          "style": "border-bottom:1px solid rgba(55,65,81,.25);"},
                "content": [
                    {"component": "VCol", "props": {"cols": 2, "class": "text-left text-caption"},
                     "content": [{"component": "span",
                                  "text": kind_names.get(task.get('kind'), task.get('kind'))}]},
                    {"component": "VCol", "props": {"cols": 3, "class": "text-left text-caption"},
                     "content": [{"component": "span",
                                  "props": {"class": "text-truncate", "title": task_time},
                                  "text": task_time}]},
                    {"component": "VCol", "props": {"cols": 2, "class": "text-left"},
                     "content": [chip(status_names.get(task_status, task_status),
                                      status_colors.get(task_status, "default"))]},
                    {"component": "VCol", "props": {"cols": 1, "class": "text-left text-caption"},
                     "content": [{"component": "span",
                                  "text": f"{len(related_retries)}" + ("重试" if related_retries else "")}]},
                    {"component": "VCol", "props": {"cols": 1, "class": "text-left text-caption"},
                     "content": [{"component": "span", "text": f"{task_duration:.1f}s"}]},
                    {"component": "VCol", "props": {"cols": 2, "class": "text-left text-caption"},
                     "content": [{"component": "span",
                                  "props": {"class": "text-truncate", "title": task_id_value},
                                  "text": task_id_value}]},
                    {"component": "VCol", "props": {"cols": 1, "class": "text-right"},
                     "content": [{
                         "component": "VBtn",
                         "props": {"prependIcon": "mdi-file-document-outline", "size": "x-small",
                                   "variant": "tonal"},
                         "text": "详情",
                         "events": {"click": {"api": f"plugin/{self.__class__.__name__}/tasks/{task_id_value}",
                                              "method": "GET", "params": {}}},
                     }]},
                ],
            })
        if task_item_views:
            recent_body = [{
                "component": "div",
                "props": {"class": "task-list", "style": "width:100%;"},
                "content": task_item_views,
            }]
        else:
            recent_body = [{
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal",
                          "text": "暂无任务记录，点击上方「立即全量扫描」开始第一个任务。"},
            }]
        # 顶部状态区：运行中展示实时进度与当前统计，空闲时展示最近一次任务结果
        is_running = bool(current)
        current_label = status_names.get(current.get("status"), "空闲") if is_running else "空闲"
        current_color = status_colors.get(current.get("status"), "default") if is_running else "default"
        current_total = max(1, current_stats.get("discovered", 0))
        current_progress = min(100, round(current_stats.get("processed", 0) * 100 / current_total)) if is_running else 0
        last_finished = None if is_running else next(
            (task for task in summaries if task.get("status") != "running"), None)
        overview_stats = current_stats if is_running else ((last_finished or {}).get("stats") or {})
        overview_rows = [{
            "component": "VRow",
            "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 3},
                 "content": [chip(f"当前任务：{current_label}", current_color)]},
            ] + ([{
                "component": "VCol", "props": {"cols": 12, "md": 9},
                "content": [{"component": "VProgressLinear", "props": {
                    "modelValue": current_progress, "height": 8, "rounded": True,
                    "color": "primary",
                    "indeterminate": not bool(current_stats.get("discovered"))}}]},
            ] if is_running else []),
        }]
        def stat_box(label, value, value_color="primary"):
            return {
                "component": "VCol", "props": {"cols": 6, "sm": 4, "md": 2, "class": "pa-1"},
                "content": [{"component": "div", "html": (
                    "<div style=\"background:#1f2937;border:1px solid #374151;border-radius:8px;"
                    "padding:8px 10px;text-align:center;height:100%;box-sizing:border-box;\">"
                    f"<div style=\"color:#9ca3af;font-size:10px;white-space:nowrap;overflow:hidden;"
                    f"text-overflow:ellipsis;\">{html_escape(label)}</div>"
                    f"<div style=\"color:{value_color};font-size:16px;font-weight:700;line-height:1.4;"
                    f"white-space:nowrap;\">{html_escape(str(value))}</div></div>"
                )}],
            }

        if is_running or last_finished:
            overview_rows.append({
                "component": "VRow",
                "content": ([{
                  "component": "VCol", "props": {"cols": 6, "md": 2},
                    "content": [{"component": "div",
                                  "props": {"class": "text-caption text-medium-emphasis text-center pa-2"},
                                  "text": "最近任务结果"}]
                }] if not is_running else []) + [
                    stat_box("总数", overview_stats.get('discovered', 0), "#38bdf8"),
                    stat_box("已处理", overview_stats.get('processed', 0), "#38bdf8"),
                    stat_box("成功", overview_stats.get('success', 0), "#10b981"),
                    stat_box("跳过", overview_stats.get('skipped', 0), "#f59e0b"),
                    stat_box("失败", overview_stats.get('failed', 0), "#f43f5e"),
                ],
            })
        latest_id = self._page_task_id or (current.get("id") if current else (summaries[0].get("id") if summaries else ""))
        latest_task = self.__task_page_snapshot(
            latest_id, self._page_filter_status, 100) if latest_id else None
        latest_items = (latest_task or {}).get("items") or []
        items_total = (latest_task or {}).get("item_total") or len(latest_items)
        item_rows = []
        retryable_failed_ids = []
        for item in latest_items:
            item_status = item.get("status") or ""
            if item_status == "failed" and item.get("retryable"):
                retryable_failed_ids.append(item.get("id"))
            item_rows.append({
                "来源": self.__shorten_path(item.get("source_file") or ""),
                "目标": self.__shorten_path(item.get("target_file") or ""),
                "动作": item.get("action") or "",
                "状态": status_names.get(item_status, item_status),
                "阶段": item.get("stage") or "",
                "原因": item.get("reason") or "",
            })
        item_headers = [
            {"title": "来源", "key": "来源"},
            {"title": "目标", "key": "目标"},
            {"title": "动作", "key": "动作"},
            {"title": "状态", "key": "状态"},
            {"title": "阶段", "key": "阶段"},
            {"title": "原因", "key": "原因"},
        ]
        task_id = latest_id
        detail_url = f"plugin/{self.__class__.__name__}/tasks/{task_id}" if task_id else ""
        retry_url = f"plugin/{self.__class__.__name__}/tasks/{task_id}/retry" if task_id else ""
        selection_url = f"plugin/{self.__class__.__name__}/tasks/{task_id}/selection" if task_id else ""
        with self._task_history_lock:
            selected_retryable_ids = set(self._task_selected_items.get(task_id, set())) if task_id else set()
        selected_retryable_ids = [item_id for item_id in retryable_failed_ids
                                  if item_id in selected_retryable_ids]
        latest_scope = (latest_task or {}).get("scope") or {}
        relation_text = ""
        if latest_task and latest_task.get("kind") == "retry":
            relation_text = f"来源任务：{latest_scope.get('source_task_id') or '未知'}"
        elif latest_task and retry_relations.get(latest_task.get("id")):
            relation_text = f"关联重试任务：{', '.join(retry_relations[latest_task.get('id')])}"
        detail_actions = []
        if retry_url and retryable_failed_ids:
            detail_actions.append({
                "component": "VBtn",
                "props": {"prependIcon": "mdi-replay", "color": "error", "variant": "tonal"},
                "text": f"批量重试全部失败项（{len(retryable_failed_ids)}）",
                "events": {"click": {"api": retry_url, "method": "POST",
                                             "params": {"item_ids": retryable_failed_ids}}},
            })
        if retry_url and selected_retryable_ids:
            detail_actions.append({
                "component": "VBtn",
                "props": {"prependIcon": "mdi-checkbox-marked-outline",
                           "color": "primary", "variant": "flat"},
                "text": f"重试已选失败项（{len(selected_retryable_ids)}）",
                "events": {"click": {"api": retry_url, "method": "POST",
                                             "params": {"item_ids": selected_retryable_ids}}},
            })
        if relation_text:
            detail_actions.append({"component": "VChip", "props": {"size": "small", "variant": "tonal"},
                                   "text": relation_text})
        failed_item_views = []
        for item in latest_items:
            if item.get("status") != "failed" or not item.get("retryable") or not retry_url:
                continue
            failed_item_views.append({
                "component": "VListItem",
                "props": {
                    "title": item.get("source_file") or "未知源文件",
                    "subtitle": f"阶段：{item.get('stage') or '未知'}；原因：{item.get('reason') or '未提供'}",
                    "lines": "three",
                },
                "content": [
                    {
                        "component": "VCheckbox",
                        "props": {"modelValue": item.get("id") in selected_retryable_ids,
                                   "label": "选择", "density": "compact",
                                   "hideDetails": True, "color": "primary"},
                        "events": {"click": {"api": selection_url, "method": "POST",
                                                     "params": {"item_id": item.get("id")}}},
                    },
                    {
                        "component": "VBtn",
                        "props": {"prependIcon": "mdi-replay", "size": "small",
                                   "color": "error", "variant": "tonal"},
                        "text": "重试此项",
                        "events": {"click": {"api": retry_url, "method": "POST",
                                                     "params": {"item_ids": [item.get("id")]}}},
                    },
                ],
            })
        if failed_item_views:
            failed_block = {
                "component": "VList", "props": {"density": "compact", "lines": "three"},
                "content": failed_item_views,
            }
        else:
            failed_block = {
                "component": "VAlert",
                "props": {"type": "success", "variant": "tonal",
                          "text": "本次任务没有可重试的失败项。"},
            }
        # 筛选按钮选中态由服务端状态驱动：当前筛选高亮，其余为文字按钮
        detail_filters = []
        active_filter = self._page_filter_status or ""
        for filter_status, filter_label in (("", "全部"), ("success", "成功"),
                                             ("skipped", "跳过"), ("failed", "失败")):
            is_active_filter = filter_status == active_filter
            filter_props = {"size": "small", "disabled": not bool(detail_url),
                            "variant": "flat" if is_active_filter else "text",
                            "prependIcon": "mdi-filter-variant" if filter_status else "mdi-filter-off-outline"}
            if is_active_filter:
                filter_props["color"] = "primary"
            detail_filters.append({
                "component": "VBtn",
                "props": filter_props,
                "text": filter_label,
                "events": {"click": {"api": detail_url, "method": "GET",
                                             "params": ({"status": filter_status} if filter_status else {})}},
             })
        monitor_rows = self.__monitor_config_rows()
        self.__ensure_dir_status_refresh()
        monitor_status, monitor_color, monitor_detail = self.__monitor_status()
        openlist_status, openlist_color, openlist_detail = self.__openlist_status()
        strm_total = len(self._generated_files)
        failed_total = overview_stats.get("failed", 0)
        skipped_total = overview_stats.get("skipped", 0)
        tasks_url = f"plugin/{self.__class__.__name__}/tasks"

        # ---------- V1.4.0：参考设计稿的深色监控台（OpenList + CD2 口径） ----------
        toggle_url = f"plugin/{self.__class__.__name__}/config/toggle"
        save_url = f"plugin/{self.__class__.__name__}/config/save"
        discard_url = f"plugin/{self.__class__.__name__}/config/discard"
        mapping_delete_url = f"plugin/{self.__class__.__name__}/mappings/delete"
        mapping_edit_url = f"plugin/{self.__class__.__name__}/mappings/edit"
        extension_remove_url = f"plugin/{self.__class__.__name__}/extensions/remove"
        saved_config = self.__current_saved_config()
        staged_config = self.__staged_snapshot()
        staged_dirty = bool(staged_config)
        status_dot_colors = {"success": "#10b981", "warning": "#f59e0b", "error": "#f43f5e",
                             "info": "#38bdf8", "default": "#6b7280"}
        # GitHub dark theme - matches 仪表盘.svg design
        dark_card_style = self._ui_card_style

        def panel_title(title, subtitle=""):
            subtitle_html = (f"<div style=\"color:#8b949e;font-size:12px;margin-top:2px;\">"
                             f"{html_escape(subtitle)}</div>" if subtitle else "")
            return (f"<div style=\"color:#f0f6fc;font-size:15px;font-weight:600;\">"
                    f"{html_escape(title)}</div>" + subtitle_html)
        def hint_box(html_text):
            return {"component": "div", "html": (
                "<div style=\"border:1px solid rgba(2,132,199,.45);background:rgba(2,132,199,.12);"
                f"color:#7dd3fc;border-radius:8px;padding:8px 12px;font-size:12px;\">{html_text}</div>"
            )}

        def strategy_row(label, subtitle, key):
            effective = self.__effective_bool(key, saved_config, staged_config,
                                              self._config_toggle_defaults.get(key, False))
            pending_tag = (
                "<span style=\"color:#d29922;font-size:10px;margin-left:6px;border:1px solid #d29922;"
                "border-radius:4px;padding:0 4px;\">待保存</span>" if key in staged_config else "")
            label_html = (
                f"<div style=\"color:#c9d1d9;font-size:13px;font-weight:500;\">{html_escape(label)}{pending_tag}</div>"
                f"<div style=\"color:#8b949e;font-size:11px;margin-top:2px;\">{html_escape(subtitle)}</div>"
            )
            return {
                "component": "VRow",
                "props": {"align": "center", "noGutters": True, "class": "py-2"},
                "content": [
                    {"component": "VCol", "props": {"cols": 9},
                     "content": [{"component": "div", "html": label_html}]},
                    {"component": "VCol", "props": {"cols": 3, "class": "d-flex justify-end"},
                     "content": [{
                         "component": "VSwitch",
                         "props": {"modelValue": effective, "color": "primary", "inset": True,
                                   "hideDetails": True, "density": "compact",
                                   "style": self._ui_switch_style},
                         "events": {"click": {"api": toggle_url, "method": "POST",
                                              "params": {"key": key}}},
                     }]},
                ],
            }

        def row_divider():
            return {"component": "div",
                    "html": "<div style=\"border-top:1px solid #21262d;\"></div>"}

        def rule_state(rule):
            local_dir = rule.get("local") or ""
            format_str = rule.get("format") or ""
            if local_dir:
                dir_state, dir_state_text, dir_state_color, dir_detail = self.__dir_status(local_dir)
                if dir_state == "unavailable":
                    return "目录不可访问", "#f85149", dir_detail or "目录不存在或不可访问"
                if dir_state == "checking":
                    return "目录检查中", "#8b949e", ""
            if format_str and not any(token in format_str
                                      for token in ("{local_file}", "{cloud_file}")):
                return "模板缺占位符", "#d29922", ""
            if not rule.get("monitor", True):
                return "实时已停用", "#8b949e", ""
            if self._enabled and self._monitor:
                return "监控中", "#3fb950", ""
            return "已配置", "#38bdf8", ""

        def mapping_table_cells(rule, state_text="", state_color=""):
            category_tags = CloudStrmHelper.__category_list(rule.get("category")) or ["未分类"]
            category_badges = "".join(
                f"<span style=\"background:#1f293d;color:#38bdf8;border-radius:4px;padding:2px 8px;"
                f"font-size:11px;font-weight:600;white-space:nowrap;margin:0 2px 2px 0;"
                f"display:inline-block;\">{html_escape(tag)}</span>"
                for tag in category_tags
            )
            local_dir = html_escape(rule.get("local") or "-")
            strm_dir = html_escape(rule.get("strm") or "-")
            cloud_dir = html_escape(rule.get("cloud") or "-")
            format_str = html_escape(rule.get("format") or "-")
            col1 = f"<div style=\"display:flex;flex-wrap:wrap;gap:2px;align-items:center;\">{category_badges}</div>"
            col2 = (
                f"<div style=\"font-family:Consolas,Monaco,monospace;min-width:0;\">"
                f"<div style=\"color:#c9d1d9;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;\" title=\"{local_dir}\">CD2: {local_dir}</div>"
                f"<div style=\"color:#8b949e;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;\" title=\"{strm_dir}\">STRM: {strm_dir}</div></div>"
            )
            col3 = (
                f"<div style=\"font-family:Consolas,Monaco,monospace;min-width:0;\">"
                f"<div style=\"color:#c9d1d9;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;\" title=\"{cloud_dir}\">{cloud_dir}</div>"
                f"<div style=\"color:#8b949e;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;\" title=\"{format_str}\">{format_str}</div></div>"
            )
            return col1, col2, col3


        # 头部 Banner - 匹配 仪表盘.svg 设计
        app_icon_svg = (
            "<svg width=\"24\" height=\"24\" viewBox=\"0 0 24 24\" fill=\"none\">"
            "<rect x=\"2\" y=\"2\" width=\"20\" height=\"20\" rx=\"6\" fill=\"#1f6feb\"/>"
            "<path d=\"M6 10 C6 6, 18 6, 18 10 C18 14, 6 14, 6 10 Z M6 14 C6 10, 18 10, 18 14"
            " C18 18, 6 18, 6 14 Z\" fill=\"none\" stroke=\"#ffffff\" stroke-width=\"2\"/>"
            "<circle cx=\"16\" cy=\"16\" r=\"3\" fill=\"#3fb950\"/></svg>"
        )
        header_title_html = (
            "<div style=\"display:flex;align-items:center;gap:12px;\">"
            f"<div style=\"width:48px;height:48px;border-radius:12px;background:#1f6feb;"
            f"display:flex;align-items:center;justify-content:center;flex:0 0 auto;\">{app_icon_svg}</div>"
            "<div style=\"min-width:0;\">"
            "<div style=\"color:#f0f6fc;font-size:17px;font-weight:600;line-height:1.35;\">"
            "中国移动云盘 STRM 助手</div>"
            "<div style=\"color:#8b949e;font-size:12px;\">"
            "精简高性能 · 自动化STRM生成与MP联动 · OpenList + CD2</div>"
            "</div></div>"
        )
        # 状态徽章
        header_chips = [
            chip(f"OpenList + CD2 · {monitor_status.replace('OpenList + CD2 ', '', 1)}",
                 monitor_color),
            chip(openlist_status, openlist_color),
        ]
        if staged_dirty:
            header_chips.append(chip("有未保存修改", "warning"))
        for header_chip in header_chips:
            header_chip["props"]["class"] = "ma-1"

        # 操作按钮
        header_button_cols = [
            {"component": "VCol", "props": {"cols": "auto"},
             "content": [{
                 "component": "VBtn",
                 "props": {"prependIcon": "mdi-refresh", "size": "small",
                           "variant": "flat",
                           "style": "background:#21262d;color:#c9d1d9;border:1px solid #30363d;"},
                 "text": "查看日志",
                 "events": {"click": {"api": tasks_url, "method": "GET", "params": {}}},
             }]},
            {"component": "VCol", "props": {"cols": "auto"},
             "content": [{
                 "component": "VBtn",
                 "props": {"prependIcon": "mdi-content-save", "size": "small", "variant": "flat",
                           "style": "background:#8957e5;color:#ffffff;",
                           "disabled": not staged_dirty},
                 "text": "保存配置",
                 "events": {"click": {"api": save_url, "method": "POST", "params": {}}},
             }]},
        ]
        if staged_dirty:
            header_button_cols.append({
                "component": "VCol", "props": {"cols": "auto"},
                "content": [{
                    "component": "VBtn",
                    "props": {"prependIcon": "mdi-undo-variant", "size": "small", "variant": "text",
                              "style": "color:#8b949e;"},
                    "text": "放弃修改",
                    "events": {"click": {"api": discard_url, "method": "POST", "params": {}}},
                }],
            })
        header_card = {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-4",
                      "style": dark_card_style},
            "content": [
                {"component": "VCardText", "content": [
                    {"component": "VRow", "props": {"align": "center", "noGutters": True},
                     "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6},
                         "content": [{"component": "div", "html": header_title_html}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6,
                                          "class": "d-flex flex-wrap align-center justify-end mt-2 mt-md-0"},
                         "content": header_chips},
                     ]},
                    {"component": "VRow", "props": {"align": "center", "noGutters": True, "class": "mt-2"},
                     "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6},
                         "content": [
                             {"component": "div", "html": (
                                 f'<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;color:#8b949e;font-size:11px;">'
                                 f'<span>{html_escape(monitor_detail or monitor_status)}</span>'
                                 f'<span>{html_escape(openlist_detail or openlist_status)}</span></div>'
                             )},
                         ]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6,
                                          "class": "d-flex flex-wrap align-center justify-end mt-2 mt-md-0"},
                         "content": header_button_cols},
                     ]},
                ]},
            ],
        }

        # 左侧：账号与运行指标 (KPI Card)
        kpi_card = {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-4",
                      "style": dark_card_style},
            "content": [
                {"component": "VCardText", "content": [
                    {"component": "div", "html": panel_title("账号与运行指标")},
                    {"component": "VRow", "props": {"noGutters": True, "class": "mt-2"},
                     "content": [
                         {"component": "VCol", "props": {"cols": 6, "class": "pr-1"},
                          "content": [{"component": "div", "html": (
                              '<div style="background:#0d1117;border:1px solid #21262d;border-radius:6px;'
                              'padding:8px 10px;">'
                              '<div style="color:#8b949e;font-size:10px;">本地 STRM 总数</div>'
                              f'<div style="color:#38bdf8;font-size:18px;font-weight:700;">'
                              f'{strm_total} <span style="color:#8b949e;font-size:11px;font-weight:400;">个</span></div>'
                              '</div>'
                          )}]},
                         {"component": "VCol", "props": {"cols": 6, "class": "pl-1"},
                          "content": [{"component": "div", "html": (
                              '<div style="background:#0d1117;border:1px solid #21262d;border-radius:6px;'
                              'padding:8px 10px;">'
                              '<div style="color:#8b949e;font-size:10px;">孤儿清理 (死链)</div>'
                              f'<div style="color:#f85149;font-size:18px;font-weight:700;">'
                              f'{self._pruned_total} <span style="color:#8b949e;font-size:11px;font-weight:400;">'
                              f'个已剥离</span></div>'
                              '</div>'
                          )}]},
                     ]},
                    {"component": "div", "class": "mt-2",
                     "html": (
                         '<div style="background:#0d1117;border:1px solid #21262d;border-radius:6px;'
                         'padding:8px 10px;">'
                         '<div style="color:#c9d1d9;font-size:11px;">OpenList + CD2 运行状态</div>'
                         f'<div style="color:{status_dot_colors.get(monitor_color, "#8b949e")};'
                         f'font-size:13px;font-weight:600;margin-top:2px;">'
                         f'{html_escape(monitor_status)} · {html_escape(openlist_status)}</div>'
                         f'<div style="color:#8b949e;font-size:10px;margin-top:2px;line-height:1.5;">'
                         f'{html_escape(monitor_detail or "")}；{html_escape(openlist_detail or "")}</div>'
                         '</div>'
                     )},
                    {"component": "div", "class": "mt-1",
                     "html": (
                         '<div style="color:#6e7681;font-size:9px;line-height:1.5;">'
                         '移动云盘无官方API：文件变化通过 CD2 挂载目录监控，STRM 内容使用 OpenList 地址模板。'
                         '</div>'
                     )},
                ]},
            ],
        }

        strategy_card = {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-4",
                      "style": dark_card_style},
            "content": [
                {"component": "VCardText", "content": [
                    {"component": "div", "html": panel_title("自动化与清理策略")},
                    {"component": "div", "class": "mt-1",
                     "content": [
                         strategy_row("监控 MoviePilot 整理入库",
                                      "CD2 挂载目录出现新文件即刻生成 STRM", "monitor"),
                         strategy_row("定时增量扫描",
                                      f"每 {self._scan_interval} 分钟轮询 CD2 挂载目录", "cron_enabled"),
                         strategy_row("云端同步删除 (Prune)",
                                      "网盘文件被删除时同步清理本地 STRM", "sync_delete"),
                     ]},
                    {"component": "div", "class": "mt-2",
                     "content": [
                         {"component": "div", "html": (
                             '<div style="color:#c9d1d9;font-size:13px;font-weight:500;margin-bottom:6px;">监控扩展名限制</div>'
                         )},
                         {"component": "div", "props": {"class": "d-flex flex-wrap", "style": "gap:4px;"},
                          "content": [
                              {"component": "VChip",
                               "props": {"size": "small", "variant": "tonal", "color": "info", "class": "ma-1"},
                               "content": [
                                   {"component": "span", "text": ext},
                                   {"component": "VBtn",
                                    "props": {"icon": "mdi-close", "size": "x-small", "variant": "text",
                                              "density": "compact", "color": "info"},
                                    "events": {"click": {"api": extension_remove_url, "method": "POST",
                                                          "params": {"ext": ext}}}},
                               ]}
                              for ext in sorted(self._media_exts)
                          ]},
                     ]},
                    {"component": "VBtn",
                     "props": {"prependIcon": "mdi-sync", "size": "small", "variant": "flat",
                               "block": True, "class": "mt-3",
                               "style": self._ui_button_style,
                               "disabled": is_running},
                     "text": "立即全量同步（运行中）" if is_running else "立即全量同步",
                     "events": {"click": {"api": tasks_url, "method": "POST",
                                          "params": {"kind": "full_scan"}}}},
                ]},
            ],
        }

        # Right column: Path Monitoring & STRM Mapping Strategy card
        effective_rules = self.__effective_rules(saved_config, staged_config)
        pending_deleted_rules = self.__pending_deleted_rules()
        mapping_row_views = []
        row_style = "border-bottom:1px solid rgba(51,65,85,.35);min-height:52px;"
        if self._page_editing_rule == -1:
            mapping_row_views.append(hint_box(
                "新增映射规则：请前往插件「设置 → 路径监控与STRM映射策略」的规则表单填写并保存。"))

        for rule_index, rule in enumerate(effective_rules):
            rule_monitor = rule.get("monitor", True)
            rule_state_text, rule_state_color, rule_state_detail = rule_state(rule)
            col1, col2, col3 = mapping_table_cells(rule, rule_state_text, rule_state_color)
            mapping_row_views.append({
                "component": "VRow", "props": {"noGutters": True, "align": "center",
                  "class": "px-2 py-1", "style": row_style },
                "content": [
                    {"component": "VCol", "props": {"cols": 2, "class": "d-flex align-center py-1"},
                     "content": [{"component": "div", "html": col1}]},
                    {"component": "VCol", "props": {"cols": 3, "class": "d-flex align-center py-1"},
                     "content": [{"component": "div", "html": col2}]},
                    {"component": "VCol", "props": {"cols": 3, "class": "d-flex align-center py-1"},
                     "content": [{"component": "div", "html": col3}]},
                    {"component": "VCol", "props": {"cols": 1, "class": "d-flex align-center justify-center py-1"},
                     "content": [{"component": "VBtn",
                      "props": {"icon": "mdi-eye-off" if rule_monitor else "mdi-eye",
                                 "size": "x-small", "variant": "text",
                                 "color": "info" if rule_monitor else "grey",
                                 "title": "停用实时监控" if rule_monitor else "启用实时监控"},
                      "events": {"click": {"api": f"plugin/{self.__class__.__name__}/mappings/{rule_index}/monitor",
                                           "method": "POST", "params": {}}}}]},
                    {"component": "VCol", "props": {"cols": 3, "class": "d-flex align-center justify-end py-1", "style": "gap:2px;"},
                     "content": [
                         {"component": "VBtn",
                          "props": {"icon": "mdi-pencil-outline", "size": "x-small",
                                     "variant": "text", "color": "info"},
                          "events": {"click": {"api": mapping_edit_url, "method": "POST",
                                               "params": {"index": rule_index}}}},
                         {"component": "VBtn",
                          "props": {"icon": "mdi-delete-outline", "size": "x-small",
                                     "variant": "text", "color": "error"},
                          "events": {"click": {"api": mapping_delete_url, "method": "POST",
                                               "params": {"index": rule_index}}}},
                     ]},
                ],
            })
            if self._page_editing_rule == rule_index:
                raw_line = self.__rules_to_monitor_confs([rule])
                mapping_row_views.append(hint_box(
                    "编辑映射规则：请在插件\u300c设置\u2192路径监控与STRM映射策略\u300d的规则表单中修改并保存。"
                    + (f"<br>当前规则原文：<code>{html_escape(raw_line)}</code>" if raw_line else "")))
        for rule in pending_deleted_rules:
            col1, col2, col3 = mapping_table_cells(rule, "待删除", "#f59e0b")
            mapping_row_views.append({
                "component": "VRow", "props": {"noGutters": True, "align": "center",
                  "class": "px-2 py-1", "style": "border-bottom:1px solid rgba(51,65,85,.35);opacity:.45;" },
                "content": [
                    {"component": "VCol", "props": {"cols": 2, "class": "d-flex align-center"},
                     "content": [{"component": "div", "html": col1}]},
                    {"component": "VCol", "props": {"cols": 3, "class": "d-flex align-center"},
                     "content": [{"component": "div", "html": col2}]},
                    {"component": "VCol", "props": {"cols": 3, "class": "d-flex align-center"},
                     "content": [{"component": "div", "html": col3}]},
                    {"component": "VCol", "props": {"cols": 1, "class": "d-flex align-center justify-center"},
                     "content": [{"component": "span", "props": {"class": "text-caption", "style": "color:#d29922;"}, "text": "待删除"}]},
                    {"component": "VCol", "props": {"cols": 3, "class": "d-flex align-center justify-end"},
                     "content": [{"component": "span", "props": {"class": "text-caption", "style": "color:#8b949e;"}, "text": "保存后生效"}]},
                ],
            })
        if not effective_rules and not pending_deleted_rules:
            mapping_row_views.append({"component": "div", "html": (
                "<div style=\"color:#6e7681;padding:18px 12px;text-align:center;font-size:13px;\">"
                "暂无映射规则，点击右上角\u300c前往设置新增\u300d开始配置</div>"
            )})
        mapping_card = {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-4",
                      "style": dark_card_style + "flex-shrink:0;"},
            "content": [
                {"component": "VCardText", "content": [
                    {"component": "VRow", "props": {"align": "center", "noGutters": True},
                     "content": [
                         {"component": "VCol", "props": {"cols": 9}, "content": [
                             {"component": "div", "html": panel_title(
                                 "路径监控与STRM映射策略",
                                 f"本地STRM输出目录 \u2192 移动云盘绝对路径 \u00b7 共{len(effective_rules)}条映射")}]},
                         {"component": "VCol", "props": {"cols": 3, "class": "d-flex justify-end"},
                          "content": [{
                              "component": "VBtn",
                              "props": {"prependIcon": "mdi-plus", "size": "small", "variant": "flat",
                                        "style": "background:#8957e5;color:#ffffff;"},
                              "text": "前往设置新增",
                              "events": {"click": {"api": mapping_edit_url, "method": "POST",
                                                   "params": {"index": -1}}}},
                          ]},
                     ]},
                    {"component": "VSheet", "props": {"variant": "outlined", "class": "rounded-lg",
                      "style": "background:#0d1117;border:1px solid #21262d;overflow:hidden;"},
                     "content": mapping_row_views},
                ]},
            ],
        }

        # Right column: Real-time Sync Task & Operation Log
        feed_tag_colors = {"MONITOR": "#38bdf8", "STRM-GEN": "#10b981", "PRUNE": "#d29922",
                           "POLL": "#60a5fa", "FAIL": "#f85149", "TASK": "#34d399",
                           "CONFIG": "#d29922"}
        feed_lines = []
        live_events = self.__live_event_snapshot(40)
        for event in reversed(live_events[-8:]):
            feed_tag = str(event.get("tag") or "EVENT")
            feed_lines.append(
                f"<div><span style=\"color:#8b949e\">[{html_escape(str(event.get('time') or '-'))}]</span> "
                f"<span style=\"color:{feed_tag_colors.get(feed_tag, '#38bdf8')}\">[{html_escape(feed_tag)}]</span> "
                f"<span style=\"color:#e5e7eb\">{html_escape(str(event.get('message') or ''))}</span></div>"
            )
        if not feed_lines:
            for item in reversed(latest_items[:8]):
                feed_time = item.get("created_at") or "-"
                item_status = item.get("status") or ""
                item_action = item.get("action") or "-"
                item_reason = item.get("reason") or "-"
                feed_source = self.__shorten_path(item.get("source_file") or "")
                if item_status == "success":
                    feed_tag, feed_color = "STRM-GEN", "#10b981"
                    feed_message = f"生成文件 -> {feed_source}"
                elif item_status == "failed":
                    feed_tag, feed_color = "FAIL", "#f85149"
                    feed_message = f"{feed_source} {item_reason}"
                elif item_status == "skipped":
                    feed_tag, feed_color = "SKIP", "#d29922"
                    feed_message = f"跳过 {feed_source} {item_reason}"
                else:
                    feed_tag, feed_color = "EVENT", "#38bdf8"
                    feed_message = f"{item_action} {feed_source} {item_reason}"
                feed_lines.append(
                    f"<div><span style=\"color:#8b949e\">[{html_escape(feed_time)}]</span> "
                    f"<span style=\"color:{feed_color}\">[{feed_tag}]</span> "
                    f"<span style=\"color:#e5e7eb\">{html_escape(feed_message)}</span></div>"
                )
        if not feed_lines:
            feed_lines.append("<div><span style=\"color:#6e7681\">暂无同步记录，等待监控事件..</span></div>")
        feed_html = (
            "<div style=\"background:#0d1117;border:1px solid #21262d;border-radius:10px;"
            "padding:12px 14px;font-family:Consolas,Monaco,monospace;font-size:12px;"
            "line-height:1.8;overflow:auto;flex:1 1 auto;height:auto;min-height:120px;box-sizing:border-box;"
            "display:flex;flex-direction:column-reverse;\">"
            + "".join(feed_lines) + "</div>"
        )
        feed_card = {
            "component": "VCard",
            "props": {"variant": "flat", "class": "mb-4",
                      "style": dark_card_style + "flex:1 1 auto;display:flex;flex-direction:column;"},
            "content": [
                {"component": "VCardText",
                 "props": {"style": "display:flex;flex-direction:column;flex:1 1 auto;"},
                 "content": [
                    {"component": "div", "html": panel_title(
                        "实时同步任务与运行日志", "Live Log Feed \u00b7 OpenList + CD2 事件流")},
                    {"component": "div", "props": {"class": "mt-2"}, "html": feed_html},
                ]},
            ],
        }
        page = [{
            "component": "div",
            "props": {"style": "background:#0d1117;color:#e5e7eb;overflow-x:hidden;scrollbar-gutter:stable;width:100%;max-width:100%;"},
            "content": [
            header_card,
            {
                "component": "VRow",
                "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [kpi_card, strategy_card]},
                    {"component": "VCol", "props": {"cols": 12, "md": 8, "class": "d-flex flex-column",
                                        "style": "height:100%;"}, "content": [mapping_card, feed_card]},
                ],
            },
            {"component": "VExpansionPanels", "props": {"variant": "accordion", "class": "mb-4"},
             "content": [
                {"component": "VExpansionPanel", "content": [
                    {"component": "VExpansionPanelTitle", "content": [
                        {"component": "VIcon", "props": {"icon": "mdi-clipboard-text-outline",
                                                          "size": "small", "class": "mr-2"}},
                        {"component": "span", "text": "任务中心与失败重试（扫描进度、历史与明细，点击展开）"},
                    ]},
                    {"component": "VExpansionPanelText", "content": [
                        {
                            "component": "VCard",
                            "props": {"variant": "tonal", "class": "mb-4"},
                            "content": [
                                {"component": "VCardTitle", "text": "任务中心"},
                                {"component": "VCardText", "content": overview_rows},
                    {"component": "VCardActions", "content": [
                        {"component": "VBtn",
                         "props": {"prependIcon": "mdi-play", "color": "primary", "variant": "flat",
                                   "disabled": is_running},
                         "text": "立即全量扫描（运行中）" if is_running else "立即全量扫描",
                         "events": {"click": {"api": f"plugin/{self.__class__.__name__}/tasks",
                                              "method": "POST", "params": {"kind": "full_scan"}}}},
                        {"component": "VBtn", "props": {"prependIcon": "mdi-refresh", "variant": "text"},
                         "text": "刷新任务状态",
                         "events": {"click": {"api": f"plugin/{self.__class__.__name__}/tasks",
                                              "method": "GET", "params": {}}}},
                    ]},
                ],
            },
            {"component": "VCard", "props": {"class": "mb-4"}, "content": [
                {"component": "VCardTitle", "text": "最近任务"},
                {"component": "VCardText", "content": recent_body},
            ]},
            {"component": "VCard", "props": {"class": "mb-4"}, "content": [
                {"component": "VCardTitle", "text": "任务详情"},
                {"component": "VCardText", "content": [
                    {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                                                      "text": f"当前查看任务 ID：{task_id or '无'}"}},
                ]},
                *([{"component": "VCardActions", "content": detail_actions}] if detail_actions else []),
                {"component": "VCardActions", "content": detail_filters},
                {"component": "div",
                 "html": self.__render_table_html(item_headers, item_rows, "暂无明细数据")},
                *([{"component": "div",
                    "props": {"class": "text-caption text-medium-emphasis mt-1 px-1"},
                    "text": f"仅显示前 100 条，共{items_total}条，可点击上方筛选按钮缩小范围"}]
                  if items_total > len(item_rows) else []),
                {"component": "VCardText", "content": [
                    {"component": "VListSubheader", "text": "可重试失败项（支持全部重试或逐项重试）"},
                    failed_block,
                    {"component": "VBtn", "props": {"prependIcon": "mdi-open-in-new", "variant": "text",
                                                    "disabled": not bool(detail_url)},
                     "text": "刷新任务详情",
                     "events": {"click": {"api": detail_url, "method": "GET", "params": {}}}},
                ]},
            ]},
                        ]},
                    ]},
                ],
            },
            ],
        }
        ]
        return page

    def get_dashboard(self, key: str, **kwargs):
        """Expose a read-only task summary that the dashboard can refresh safely."""
        if key not in {"", "tasks", "task_center"}:
            return None
        refresh = 3 if self.__current_task() else 0
        current = self.__current_task() or {}
        stats = current.get("stats") or {}
        status_names = {
            "running": "运行中", "success": "成功", "partial": "部分成功",
            "failed": "失败", "interrupted": "已中断",
        }
        is_running = bool(current)
        monitor_rows = self.__monitor_config_rows()
        self.__ensure_dir_status_refresh()
        monitor_status, monitor_color = self.__monitor_status()[:2]
        discovered = stats.get("discovered", 0)
        progress = min(100, round(stats.get("processed", 0) * 100 / max(1, discovered))) if is_running else 0
        summary = self.__api_tasks().get("tasks") or []
        rows = []
        for task in summary[:5]:
            task_stats = task.get("stats") or {}
            rows.append({
                "时间": task.get("finished_at") or task.get("created_at"),
                "状态": status_names.get(task.get("status"), task.get("status")),
                "成功": task_stats.get("success", 0),
                "跳过": task_stats.get("skipped", 0),
                "失败": task_stats.get("failed", 0),
            })
        page = [{
            "component": "div",
            "props": {"style": "background:#0d1117;border-radius:8px;padding:12px;color:#e5e7eb;"},
            "content": [
                {
                    "component": "div",
                    "props": {"style": "display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px;"},
                    "content": [
                        {"component": "span", "props": {"style": "background:#21262d;border:1px solid #30363d;border-radius:11px;padding:2px 10px;font-size:11px;color:#8b949e;"}, "text": f"监控：{monitor_status}"},
                        {"component": "span", "props": {"style": "background:#21262d;border:1px solid #30363d;border-radius:11px;padding:2px 10px;font-size:11px;color:#8b949e;"}, "text": f"目录 {len(monitor_rows)}"},
                        {"component": "span", "props": {"style": "background:#21262d;border:1px solid #30363d;border-radius:11px;padding:2px 10px;font-size:11px;color:#8b949e;"}, "text": f"已处理 {stats.get('processed', 0)}"},
                        {"component": "span", "props": {"style": "background:rgba(63,185,80,0.15);border:1px solid #238636;border-radius:11px;padding:2px 10px;font-size:11px;color:#3fb950;"}, "text": f"成功 {stats.get('success', 0)}"},
                        {"component": "span", "props": {"style": "background:rgba(210,153,34,0.15);border:1px solid #9e6a03;border-radius:11px;padding:2px 10px;font-size:11px;color:#d29922;"}, "text": f"跳过 {stats.get('skipped', 0)}"},
                        {"component": "span", "props": {"style": "background:rgba(248,81,73,0.15);border:1px solid #da3633;border-radius:11px;padding:2px 10px;font-size:11px;color:#f85149;"}, "text": f"失败 {stats.get('failed', 0)}"},
                    ],
                },
                {
                    "component": "div",
                    "props": {"style": "margin-bottom:10px;" if is_running else "display:none;"},
                    "content": [
                        {
                            "component": "div",
                            "props": {"style": "background:#21262d;border-radius:10px;height:6px;overflow:hidden;"},
                            "content": [
                                {"component": "div",
                                 "props": {"style": f"background:#58a6ff;width:{progress}%;height:6px;border-radius:10px;transition:width 0.3s;" if discovered else "background:#58a6ff;width:100%;height:6px;border-radius:10px;animation:pulse 1.5s infinite;"}}
                            ],
                        },
                    ],
                },
                {
                    "component": "div",
                    "props": {"style": "overflow:hidden;border:1px solid #30363d;border-radius:8px;"},
                    "html": self.__render_table_html(
                        [{"title": "时间", "key": "时间"}, {"title": "状态", "key": "状态"},
                         {"title": "成功", "key": "成功"}, {"title": "跳过", "key": "跳过"},
                         {"title": "失败", "key": "失败"}],
                        rows, "暂无任务记录"),
                },
            ],
        }]
        return ({"cols": 12, "md": 12},
                {"refresh": refresh, "border": False, "title": "CloudStrm 任务中心"},
                page)
    def get_dashboard_meta(self):
        return [{"key": "task_center", "name": "任务中心"}]

    def stop_service(self):
        """
        退出插件
        """
        self._task_generation += 1
        active_id = self._task_active_id or self._task_thread_task_id
        active_thread = self._task_thread
        if active_id:
            self._task_stop_event.set()
            stop_event = self._task_stop_events.get(active_id)
            if stop_event:
                stop_event.set()
            with self._task_history_lock:
                active = next((task for task in self._task_history
                               if task.get("id") == active_id and task.get("status") == "running"), None)
                if active:
                    now = self.__task_now()
                    active["status"] = "interrupted"
                    active["finished_at"] = now
                    active["updated_at"] = now
                    active["error"] = "插件重启时任务未完成"
                    active["duration_seconds"] = self.__task_duration(active)
                    self._task_history_dirty += 1
            try:
                self.__save_task_history(force=True)
            except OSError as err:
                logger.warning(f"保存中断任务历史失败：{err}")
            self._task_active_id = None
            if active_thread and active_thread is not threading.current_thread():
                active_thread.join(timeout=2)
        if self._observer:
            for observer in self._observer:
                try:
                    observer.stop()
                    observer.join()
                except Exception as e:
                    logger.warning(f"停止目录监控失败：{e}")
        self._observer = []
        with self._event_timers_lock:
            timers = list(self._event_timers.values())
            self._event_timers.clear()
        for timer in timers:
            try:
                timer.cancel()
            except Exception:
                pass
        with self._state_lock:
            manifest_pending = self._manifest_pending
            self._manifest_pending = False
        if manifest_pending:
            self.__save_generated_files()
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._event.set()
                self._scheduler.shutdown()
                self._event.clear()
            self._scheduler = None
