import json
import hashlib
import os
import re
import shutil
import tempfile
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
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



from .config_rules import (
    MonitorRule,
    config_fingerprint,
    parse_comma_path_mappings,
    parse_monitor_confs,
    parse_path_mappings,
    serialize_mapping_line,
    serialize_path_mappings,
)
from .core_paths import (
    find_monitor_path,
    format_content,
    map_library_path,
    map_path,
    normalise_extensions,
    path_key,
    relative_path,
)
from .monitoring import FileMonitorHandler, is_ignored_path, is_temporary_path, wait_for_stable_file
from .state_store import SyncStateStore
from .sync_engine import SyncEngine
from .task_store import PendingJob, TaskStore, classify_failure


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    normalized = str(value).strip().lower()
    if normalized in {"", "0", "false", "no", "off", "n"}:
        return False
    if normalized in {"1", "true", "yes", "on", "y"}:
        return True
    return bool(value)


class CloudStrmButler(_PluginBase):
    # 插件名称
    plugin_name = "云盘Strm小管家"
    plugin_name_en = "Cloud Strm Butler"
    # 插件描述
    plugin_desc = "Cloud Strm Butler - 实时监控、定时全量增量生成strm文件。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/yyhoworks/MoviePilot-Plugins/main/icons/cloudstrm.png"
    # 插件版本
    plugin_version = "2.1.23"
    # 插件作者
    plugin_author = "FelixYang"
    # 作者主页
    author_url = ""
    # 插件配置项ID前缀
    plugin_config_prefix = "cloudstrmbutler_"
    # 加载顺序
    plugin_order = 26
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _enabled = False
    _monitor_confs = None
    _cover = False
    _monitor = False
    _onlyonce = False
    _copy_files = False
    _copy_subtitles = False
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
    _scan_interval: int = 0
    _mediaservers = None
    mediaserver_helper = None
    _emby_paths = {}
    _path_replacements = {}  # 新增：路径替换规则属性
    _reliable_engine = False
    _cleanup_mode = "off"
    _cleanup_probe = ""

    # 定时器
    _scheduler: Optional[BackgroundScheduler] = None
    # 退出事件
    _event = threading.Event()

    _default_rmt_mediaext = ".mp4, .mkv, .ts, .iso,.rmvb, .avi, .mov, .mpeg,.mpg, .wmv, .3gp, .asf, .m4v, .flv, .m2ts, .strm,.tp, .f4v"
    _default_other_mediaext = ".nfo, .jpg, .png, .json"
    _default_subtitle_formats = ".srt, .ass, .ssa, .sub, .vtt"
    _command_stall_seconds = 60
    _command_shutdown_timeout = 20
    _result_status_aliases = {
        "existing_skipped": "existing_skipped",
        "skipped": "existing_skipped",
        "unchanged": "existing_skipped",
        "copied_non_media": "copied_non_media",
        "copied_subtitle": "copied_subtitle",
        "generated_strm": "generated_strm",
        "failed": "failed",
    }

    def __init__(self):
        super().__init__()
        self._command_guard = threading.Lock()
        self._reset_runtime_state()

    def _reset_runtime_state(self):
        self._enabled = False
        self._onlyonce = False
        self._monitor = False
        self._cover = False
        self._copy_files = False
        self._copy_subtitles = False
        self._refresh_emby = False
        self._notify = False
        self._uriencode = False
        self._interval = 10
        self._scan_interval = 0
        self._url = ""
        self._mediaservers = []
        self._monitor_confs = ""
        self._rmt_mediaext = self._default_rmt_mediaext
        self._other_mediaext = self._default_other_mediaext
        self._subtitle_formats = self._default_subtitle_formats
        self._media_extensions = set()
        self._other_extensions = set()
        self._subtitle_extensions = set()
        self._strm_dir_conf = {}
        self._cloud_dir_conf = {}
        self._category_conf = {}
        self._format_conf = {}
        self._path_replacements = {}
        self._reliable_engine = False
        self._cleanup_mode = "off"
        self._cleanup_probe = ""
        self._emby_paths = {}
        self._monitor_rules = []
        self._config_errors = []
        self._medias = {}
        self._observer = []
        self._scheduler = None
        self._event = threading.Event()
        self._media_lock = threading.Lock()
        self._emby_refresh_lock = threading.Lock()
        self._pending_emby_refreshes = {}
        self._active_lock = threading.Lock()
        self._active_paths = set()
        self._scan_guard = threading.Lock()
        self._scan_launch_lock = threading.Lock()
        self._scan_running = False
        self._scan_thread = None
        self._scan_cancel = threading.Event()
        self._cancelled_run_ids = set()
        self._cancelled_run_lock = threading.Lock()
        self._scan_finalized_run_ids = set()
        self._scan_finalized_lock = threading.Lock()
        self._scan_progress_lock = threading.RLock()
        self._scan_progress = self._new_scan_progress()
        self._command_running = False
        self._command_thread = None
        self._command_cancel = threading.Event()
        self._processing_options_lock = threading.RLock()
        self._command_progress_lock = threading.RLock()
        self._command_progress = self._new_command_progress()
        self._state_store = None
        self._task_store = None
        self._sync_engine = None
        self._config_fingerprint = ""
        self._processing_overview_lock = threading.RLock()
        self._processing_overview_cache = None
        self._processing_overview_refreshing = False
        self._processing_overview_last_refresh_at = 0.0

    def init_plugin(self, config: dict = None):
        if not self.stop_service():
            logger.warning("手动 /strm 扫描仍在运行，本次插件重载已延后")
            return
        self._reset_runtime_state()

        migrated = False
        if not config:
            legacy_config = self.get_config("CloudStrmCompanion")
            if legacy_config:
                config = legacy_config
                migrated = True
                logger.info("已从 CloudStrmCompanion 迁移配置到 CloudStrmButler")

        self.mediaserver_helper = MediaServerHelper()

        if config:
            self._enabled = _as_bool(config.get("enabled"))
            # Legacy full-scan switches are retained for config compatibility,
            # but full processing is now always user-initiated from the page.
            self._onlyonce = False
            self._monitor = _as_bool(config.get("monitor"))
            self._cover = _as_bool(config.get("cover"))
            self._copy_files = _as_bool(config.get("copy_files"))
            self._copy_subtitles = _as_bool(config.get("copy_subtitles"))
            self._refresh_emby = _as_bool(config.get("refresh_emby"))
            self._notify = _as_bool(config.get("notify"))
            self._uriencode = _as_bool(config.get("uriencode"))
            self._url = str(config.get("url") or "")
            self._monitor_confs = str(config.get("monitor_confs") or "")
            mediaservers = config.get("mediaservers") or []
            self._mediaservers = list(mediaservers) if isinstance(mediaservers, (list, tuple)) else [mediaservers]
            self._other_mediaext = config.get("other_mediaext") or self._default_other_mediaext
            self._subtitle_formats = str(config.get("subtitle_formats") or self._default_subtitle_formats)
            try:
                self._interval = max(0, int(config.get("interval") or 10))
            except (TypeError, ValueError):
                self._interval = 10
            try:
                # Keep the setting round-trippable for compatibility, but do
                # not schedule a full scan during plugin startup.
                self._scan_interval = max(0, int(config.get("scan_interval") or 0))
            except (TypeError, ValueError):
                self._scan_interval = 0
            self._path_replacements = dict(parse_path_mappings(config.get("path_replacements")))
            # Keep the old switch readable, but make the durable queue the
            # default for new configurations. Explicit false remains compatible.
            self._reliable_engine = _as_bool(config.get("reliable_engine"), True)
            self._cleanup_mode = str(config.get("cleanup_mode") or "off").lower()
            self._cleanup_probe = str(config.get("cleanup_probe") or "").strip()
            self._rmt_mediaext = config.get("rmt_mediaext") or self._default_rmt_mediaext
            self._emby_paths = dict(parse_comma_path_mappings(config.get("emby_path")))
            # 结构化规则优先：从 rule_N_* 键生成 monitor_confs
            structured_rules = self._parse_structured_rules(config)
            if self._has_structured_config(config):
                self._monitor_confs = self._rules_to_monitor_confs(structured_rules)
                # 清理旧的结构化槽位，写回规范化配置
                normalized = dict(config)
                for key in list(normalized.keys()):
                    if key.startswith("rule_"):
                        normalized.pop(key, None)
                for i, rule in enumerate(structured_rules):
                    normalized[f"rule_{i}_category"] = rule.get("category", "")
                    normalized[f"rule_{i}_local"] = rule.get("local", "")
                    normalized[f"rule_{i}_strm"] = rule.get("strm", "")
                    normalized[f"rule_{i}_cloud"] = rule.get("cloud", "")
                    normalized[f"rule_{i}_format"] = rule.get("format", "")
                    normalized[f"rule_{i}_monitor"] = rule.get("monitor", True)
                for key, value in config.items():
                    if re.match(r"^rule_\d+_delete$", str(key)) and _as_bool(value):
                        normalized[key] = True
                normalized.pop("monitor_confs", None)
                normalized["config_version"] = 2
                if normalized != config:
                    try:
                        self.update_config(normalized)
                    except Exception:
                        logger.exception("结构化配置规范化写回失败，保留当前配置继续启动")

        if migrated:
            self.__update_config()

        self._media_extensions = normalise_extensions(self._rmt_mediaext)
        self._other_extensions = normalise_extensions(self._other_mediaext)
        self._subtitle_extensions = normalise_extensions(self._subtitle_formats, self._default_subtitle_formats)
        self._monitor_rules, self._config_errors = parse_monitor_confs(self._monitor_confs)
        for rule in self._monitor_rules:
            self._strm_dir_conf[rule.local_dir] = rule.strm_dir
            self._cloud_dir_conf[rule.local_dir] = rule.cloud_dir
            self._format_conf[rule.local_dir] = rule.format_str
            self._category_conf[rule.local_dir] = rule.category
        self._config_fingerprint = config_fingerprint(
            self._monitor_rules,
            self._path_replacements,
            self._emby_paths,
            self._media_extensions,
            self._other_extensions,
            self._cover,
            self._copy_files,
            self._copy_subtitles,
            self._uriencode,
            self._subtitle_extensions,
        )
        self._state_store = SyncStateStore(self.get_data_path() / "sync_state.sqlite3")
        self._task_store = TaskStore(self.get_data_path() / "task_state.sqlite3")
        self._task_store.prune()
        if self._reliable_engine and self._enabled:
            self._sync_engine = SyncEngine(self._task_store, self._run_reliable_job, completion=self._complete_reliable_job)
            self._sync_engine.start()

        for error in self._config_errors:
            logger.error(error)
            self.systemmessage.put(error)

        if not self._enabled:
            return

        if not self._monitor_rules:
            logger.warning("没有可用的目录配置，不启动扫描任务")
            return

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        if self._scan_interval > 0:
            self._scheduler.add_job(
                func=self.scan,
                trigger="interval",
                minutes=self._scan_interval,
                name="云盘Strm小管家定时全量扫描",
            )

        if self._notify:
            self._scheduler.add_job(
                self.send_msg,
                trigger="interval",
                seconds=15,
                name="云盘Strm小管家通知队列",
            )

        if self._refresh_emby and self._mediaservers:
            self._scheduler.add_job(
                func=self._flush_emby_refreshes,
                trigger="interval",
                seconds=5,
                name="云盘Strm小管家 Emby 批量刷新",
            )

        if self._reliable_engine:
            self._scheduler.add_job(
                func=self._pump_reliable_queue,
                trigger="interval",
                seconds=2,
                name="云盘Strm小管家可靠同步队列",
            )

        for rule in self._monitor_rules:
            if rule.should_monitor(self._monitor):
                self._start_observer(rule)

        if self._scheduler.get_jobs():
            self._scheduler.print_jobs()
        # Starting an empty scheduler only keeps the runtime state consistent;
        # full processing remains available exclusively through the manual API.
        self._scheduler.start()

    def _start_observer(self, rule: MonitorRule):
        """Start a polling watcher for one valid monitor rule."""
        try:
            observer = PollingObserver(timeout=10)
            observer.schedule(FileMonitorHandler(rule.local_dir, self), path=rule.local_dir, recursive=True)
            observer.daemon = True
            observer.start()
            self._observer.append(observer)
            logger.info(f"{rule.local_dir} 的 Strm 生成实时监控服务启动")
        except Exception as exc:
            message = str(exc)
            if "inotify" in message and "reached" in message:
                logger.warning(
                    f"云盘实时监控服务启动出现异常：{message}，请在宿主机上（不是 docker 容器内）执行以下命令并重启："
                    "echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf\n"
                    "echo fs.inotify.max_user_instances=524288 | sudo tee -a /etc/sysctl.conf\n"
                    "sudo sysctl -p"
                )
            else:
                logger.error(f"{rule.local_dir} 启动实时监控失败：{message}")
            self.systemmessage.put(f"{rule.local_dir} 启动实时监控失败：{message}")

    @staticmethod
    def _new_scan_progress() -> Dict[str, Any]:
        return {
            "running": False,
            "run_id": "",
            "kind": "",
            "phase": "idle",
            "current_rule": "",
            "current_path": "",
            "total": 0,
            "processed": 0,
            "failed": 0,
            "result_counts": {
                "existing_skipped": 0,
                "copied_non_media": 0,
                "copied_subtitle": 0,
                "generated_strm": 0,
                "failed": 0,
            },
            "started_at": None,
            "last_progress_at": None,
            "finished_at": None,
        }

    def _update_scan_progress(self, **updates) -> None:
        with self._scan_progress_lock:
            self._scan_progress.update(updates)

    def _scan_progress_snapshot(self) -> Dict[str, Any]:
        with self._scan_progress_lock:
            snapshot = dict(self._scan_progress)
            snapshot["result_counts"] = dict(self._scan_progress.get("result_counts") or {})
        last_progress_at = snapshot.get("last_progress_at")
        stalled_seconds = max(0, int(time.time() - float(last_progress_at))) if snapshot.get("running") and last_progress_at else 0
        snapshot["stalled_seconds"] = stalled_seconds
        snapshot["stalled"] = bool(snapshot.get("running") and stalled_seconds >= self._command_stall_seconds)
        return snapshot

    def _record_scan_discovered(self, rule: MonitorRule, path: str) -> None:
        with self._scan_progress_lock:
            self._scan_progress["total"] = int(self._scan_progress.get("total") or 0) + 1
            self._scan_progress["current_rule"] = rule.local_dir
            self._scan_progress["current_path"] = path
            self._scan_progress["last_progress_at"] = time.time()

    def _record_scan_result(self, run_id: Optional[str], result: Optional[dict]) -> None:
        result = result or {}
        with self._scan_progress_lock:
            if run_id and self._scan_progress.get("run_id") != run_id:
                return
            if self._is_run_cancelled(run_id):
                return
            if not run_id and not self._scan_progress.get("running"):
                return
            self._scan_progress["processed"] = int(self._scan_progress.get("processed") or 0) + 1
            status = str(result.get("status") or "skipped").lower()
            if status in {"failed", "unstable", "invalid_target", "invalid_cloud", "invalid_content"}:
                self._scan_progress["failed"] = int(self._scan_progress.get("failed") or 0) + 1
            if not result.get("count_only"):
                default_status = {"processed": "generated_strm", "unchanged": "existing_skipped", "failed": "failed"}.get(status, "existing_skipped")
                self._merge_result_counts(self._scan_progress["result_counts"], self._normalise_result_statuses(result.get("result_statuses"), default_status))
            self._scan_progress["last_progress_at"] = time.time()

    def _finish_scan_progress(self, run_id: Optional[str], phase: str = "completed") -> None:
        with self._scan_progress_lock:
            if run_id and self._scan_progress.get("run_id") != run_id:
                return
            if not run_id and not self._scan_progress.get("running"):
                return
            finished_at = time.time()
            self._scan_progress.update(running=False, phase=phase, current_path="", finished_at=finished_at, last_progress_at=finished_at)

    def _mark_run_cancelled(self, run_id: Optional[str]) -> None:
        if run_id:
            with self._cancelled_run_lock:
                self._cancelled_run_ids.add(str(run_id))
            if self._task_store:
                self._task_store.cancel_run(str(run_id))
                self._task_store.cancel_run_jobs(str(run_id))

    def _is_run_cancelled(self, run_id: Optional[str]) -> bool:
        if not run_id:
            return False
        with self._cancelled_run_lock:
            return str(run_id) in self._cancelled_run_ids

    def _scan_is_active(self) -> bool:
        thread = self._scan_thread
        other_thread_running = bool(thread and thread.is_alive() and thread is not threading.current_thread())
        return bool(self._scan_running or self._scan_progress_snapshot().get("running") or other_thread_running)

    def scan(self, kind: str = "scan", options: Optional[Dict[str, Any]] = None):
        """Run an idempotent full directory reconciliation."""
        if self._command_running or self._scan_is_active():
            logger.warning("已有手动扫描正在执行，跳过本次全量扫描请求")
            return False
        if not self._scan_guard.acquire(blocking=False):
            logger.warning("已有增量扫描正在执行，跳过本次请求")
            return False
        self._scan_running = True
        self._scan_cancel.clear()
        scan_options = options if isinstance(options, dict) else {}
        previous_options = (self._copy_files, self._copy_subtitles)
        if "copy_files" in scan_options:
            self._copy_files = _as_bool(scan_options.get("copy_files"))
        if "copy_subtitles" in scan_options:
            self._copy_subtitles = _as_bool(scan_options.get("copy_subtitles"))
        started = time.monotonic()
        run_id = self._task_store.start_run(kind) if self._task_store else None
        now = time.time()
        self._update_scan_progress(
            **{
                **self._new_scan_progress(),
                "running": True,
                "run_id": run_id or "",
                "kind": kind,
                "phase": "scanning",
                "started_at": now,
                "last_progress_at": now,
            }
        )
        try:
            logger.info("开始增量执行")
            for rule in self._monitor_rules:
                if self._scan_cancel.is_set():
                    break
                self._update_scan_progress(current_rule=rule.local_dir, current_path=rule.local_dir)
                self._scan_rule(rule, run_id=run_id)
            logger.info("增量执行完成，耗时 %.1f 秒", time.monotonic() - started)
            if run_id and self._task_store:
                if self._scan_cancel.is_set():
                    self._task_store.cancel_run(run_id, "用户取消")
                    self._finish_scan_progress(run_id, phase="cancelled")
                elif self._reliable_engine:
                    with self._scan_finalized_lock:
                        self._scan_finalized_run_ids.add(str(run_id))
                    settled = self._task_store.finish_run_if_settled(run_id)
                    if settled:
                        self._finish_scan_progress(run_id)
                        with self._scan_finalized_lock:
                            self._scan_finalized_run_ids.discard(str(run_id))
                elif self._scan_run_failed(run_id):
                    self._task_store.finish_run(run_id, status="completed_with_errors")
                    self._finish_scan_progress(run_id, phase="completed_with_errors")
                else:
                    self._task_store.finish_run(run_id, status="completed")
                    self._finish_scan_progress(run_id)
            else:
                self._finish_scan_progress(run_id)
        except Exception as exc:
            if run_id and self._task_store:
                self._task_store.finish_run(run_id, status="failed", message=str(exc))
            self._finish_scan_progress(run_id, phase="failed")
            raise
        finally:
            self._copy_files, self._copy_subtitles = previous_options
            self._scan_running = False
            self._scan_cancel.clear()
            self._scan_guard.release()
            if self._scan_thread is threading.current_thread():
                self._scan_thread = None
        return True

    def _scan_run_failed(self, run_id: str) -> bool:
        if not self._task_store:
            return False
        return any(
            int(run.get("failed") or 0) > 0
            for run in self._task_store.status().get("recent_runs", [])
            if run.get("run_id") == run_id
        )

    def _invalidate_processing_overview(self) -> None:
        with self._processing_overview_lock:
            self._processing_overview_cache = None
            self._processing_overview_last_refresh_at = 0.0

    def _index_processing_overview(self) -> Dict[str, Any]:
        counts = {
            "media_total": 0,
            "strm_total": 0,
            "non_media_total": 0,
            "non_media_completed": 0,
            "subtitle_total": 0,
            "subtitle_completed": 0,
        }
        if not self._state_store:
            return {
                **counts,
                "media_strm_consistent": True,
                "ready": True,
                "record_ready": True,
                "refreshing": False,
                "source_scan_pending": True,
                "source_scan_error": "",
                "last_checked_at": time.time(),
            }
        records = [
            record
            for rule in self._monitor_rules
            for record in self._state_store.records_for_root(rule.local_dir)
        ]
        output_owners = {}
        for record in records:
            for output in record.outputs:
                output_owners.setdefault(path_key(output), set()).add(record.source_rel)
        for record in records:
            suffix = Path(record.source_rel).suffix.lower()
            output_suffixes = {Path(output).suffix.lower() for output in record.outputs}
            if suffix in self._media_extensions:
                counts["media_total"] += 1
                if ".strm" in output_suffixes:
                    counts["strm_total"] += 1
                for output in record.outputs:
                    output_key = path_key(output)
                    output_suffix = Path(output).suffix.lower()
                    # A sidecar can be persisted both with its media record and
                    # as its own record when a directory scan sees both files.
                    if len(output_owners.get(output_key, set())) > 1:
                        continue
                    if output_suffix in self._other_extensions and self._copy_files:
                        counts["non_media_total"] += 1
                        if Path(output).is_file():
                            counts["non_media_completed"] += 1
                    if output_suffix in self._subtitle_extensions and self._copy_subtitles:
                        counts["subtitle_total"] += 1
                        if Path(output).is_file():
                            counts["subtitle_completed"] += 1
            elif suffix in self._other_extensions and self._copy_files:
                counts["non_media_total"] += 1
                if all(Path(output).is_file() for output in record.outputs):
                    counts["non_media_completed"] += 1
            elif suffix in self._subtitle_extensions and self._copy_subtitles:
                counts["subtitle_total"] += 1
                if all(Path(output).is_file() for output in record.outputs):
                    counts["subtitle_completed"] += 1
        return {
            **counts,
            "media_strm_consistent": counts["media_total"] == counts["strm_total"],
            # The persisted index is immediately usable. A directory walk is
            # only a background reconciliation and must not block the page.
            "ready": False,
            "record_ready": True,
            "refreshing": False,
            "source_scan_pending": True,
            "source_scan_error": "",
            "last_checked_at": time.time(),
        }

    def _refresh_processing_overview(self) -> None:
        try:
            counts = {
                "media_total": 0,
                "strm_total": 0,
                "non_media_total": 0,
                "non_media_completed": 0,
                "subtitle_total": 0,
                "subtitle_completed": 0,
            }
            for rule in self._monitor_rules:
                if not Path(rule.local_dir).is_dir():
                    continue
                for root, _dirs, files in os.walk(rule.local_dir):
                    for file_name in files:
                        source_file = Path(root) / file_name
                        suffix = source_file.suffix.lower()
                        target_file = self.__remap_path(str(source_file), rule.local_dir, rule.strm_dir)
                        if suffix in self._media_extensions:
                            counts["media_total"] += 1
                            strm_path = Path(target_file).with_suffix(".strm") if target_file else None
                            if strm_path and strm_path.is_file():
                                counts["strm_total"] += 1
                        elif suffix in self._other_extensions and self._copy_files:
                            counts["non_media_total"] += 1
                            if target_file and Path(target_file).is_file():
                                counts["non_media_completed"] += 1
                        elif suffix in self._subtitle_extensions and self._copy_subtitles:
                            counts["subtitle_total"] += 1
                            if target_file and Path(target_file).is_file():
                                counts["subtitle_completed"] += 1
            with self._processing_overview_lock:
                self._processing_overview_cache = {
                    **counts,
                    "media_strm_consistent": counts["media_total"] == counts["strm_total"],
                    "ready": True,
                    "record_ready": True,
                    "refreshing": False,
                    "source_scan_pending": False,
                    "source_scan_error": "",
                    "last_checked_at": time.time(),
                }
                self._processing_overview_last_refresh_at = time.time()
        except Exception as exc:
            logger.warning("刷新处理概览失败：%s", exc)
            with self._processing_overview_lock:
                cached = dict(self._processing_overview_cache or {})
                if not cached:
                    cached = self._index_processing_overview()
                cached.update({
                    "ready": True,
                    "record_ready": True,
                    "refreshing": False,
                    "source_scan_pending": False,
                    "source_scan_error": "目录核对失败，当前显示已记录的处理结果",
                    "last_checked_at": cached.get("last_checked_at") or time.time(),
                })
                self._processing_overview_cache = cached
                self._processing_overview_last_refresh_at = time.time()
        finally:
            with self._processing_overview_lock:
                self._processing_overview_refreshing = False

    def _processing_overview(self) -> Dict[str, Any]:
        with self._processing_overview_lock:
            cached = dict(self._processing_overview_cache or {})
            refreshing = self._processing_overview_refreshing
            last_refresh_at = self._processing_overview_last_refresh_at
        if not cached:
            cached = self._index_processing_overview()
            with self._processing_overview_lock:
                self._processing_overview_cache = dict(cached)
                last_refresh_at = self._processing_overview_last_refresh_at
        # Older persisted/API payloads only have `ready`; keep the distinction
        # explicit so a slow source walk cannot hide usable records.
        cached.setdefault("record_ready", bool(cached.get("ready")))
        should_refresh = (
            not refreshing
            and bool(cached.get("source_scan_pending"))
            or (not refreshing and time.time() - last_refresh_at >= 30)
        )
        if should_refresh:
            with self._processing_overview_lock:
                self._processing_overview_refreshing = True
            threading.Thread(target=self._refresh_processing_overview, name="cloudstrmbutler-overview", daemon=True).start()
            refreshing = True
        cached["refreshing"] = refreshing
        return cached

    def _scan_rule(self, rule: MonitorRule, run_id: Optional[str] = None):
        """Walk one monitor root and reconcile it with the persisted index."""
        if not Path(rule.local_dir).is_dir():
            message = f"监控目录不可用：{rule.local_dir}"
            logger.error(message)
            self._record_scan_rule_error(run_id, message)
            return False
        seen = set()
        covered_sidecars = set()
        completed = True
        def on_walk_error(error):
            nonlocal completed
            completed = False
            message = f"遍历监控目录失败：{rule.local_dir} - {error}"
            logger.error(message)
            self._record_scan_rule_error(run_id, message)

        try:
            for root, _dirs, files in os.walk(rule.local_dir, onerror=on_walk_error):
                if self._scan_cancel.is_set():
                    completed = False
                    break
                # Mark media sidecars before processing this directory so the
                # result counts do not depend on filesystem listing order.
                for file_name in files:
                    media_path = Path(root) / file_name
                    if media_path.suffix.lower() in self._media_extensions:
                        covered_sidecars.update(
                            path_key(sidecar)
                            for sidecar in self._iter_sidecars(media_path)
                            if self._should_copy_sidecar(str(sidecar))
                        )
                for file_name in files:
                    if self._scan_cancel.is_set():
                        completed = False
                        break
                    source_file = os.path.join(root, file_name)
                    relative = relative_path(source_file, rule.local_dir)
                    if relative is None:
                        continue
                    seen.add(str(relative).replace("\\", "/"))
                    self._record_scan_discovered(rule, source_file)
                    source_path_key = path_key(source_file)
                    if (
                        source_path_key in covered_sidecars
                        and self._sidecar_record_is_current(
                            source_file,
                            rule.local_dir,
                            self.__remap_path(source_file, rule.local_dir, rule.strm_dir),
                        )
                    ):
                        self._record_scan_result(run_id, {"status": "covered", "count_only": True})
                        continue
                    if is_ignored_path(source_file) or is_temporary_path(source_file):
                        if run_id and self._task_store:
                            self._task_store.update_run(run_id, queued=1, skipped=1)
                            self._task_store.update_run_result_counts(run_id, {"existing_skipped": 1})
                        self._record_scan_result(run_id, {"status": "skipped", "result_statuses": ["existing_skipped"]})
                        continue
                    if self._reliable_engine and self._sync_engine:
                        payload = {
                            "run_id": run_id,
                            "copy_files": self._copy_files,
                            "copy_subtitles": self._copy_subtitles,
                        } if run_id else None
                        accepted = self._sync_engine.enqueue(rule.local_dir, source_file, "sync", payload=payload)
                        if accepted and run_id and self._task_store:
                            self._task_store.update_run(run_id, queued=1)
                    else:
                        if run_id and self._task_store:
                            self._task_store.update_run(run_id, queued=1)
                        result = self.__handle_file(event_path=source_file, mon_path=rule.local_dir)
                        self._record_run_result(run_id, result)
                        self._record_scan_result(run_id, result)
        except OSError as exc:
            completed = False
            message = f"遍历监控目录失败：{rule.local_dir} - {exc}"
            logger.error(message)
            self._record_scan_rule_error(run_id, message)

        if completed and not self._scan_cancel.is_set() and self._state_store is not None:
            self._reconcile_missing_records(rule.local_dir, seen, run_id)
        return completed

    def _record_scan_rule_error(self, run_id: Optional[str], message: str) -> None:
        if not run_id or not self._task_store:
            return
        self._task_store.update_run(run_id, failed=1)
        self._task_store.update_run_result_counts(run_id, {"failed": 1})
        self._task_store.record_run_error(run_id, message)
        self._record_scan_result(run_id, {"status": "failed", "result_statuses": ["failed"]})

    def _reconcile_missing_records(self, monitor_root: str, seen: set, run_id: Optional[str] = None):
        """Protect generated files from mount outages by staging scan-based cleanup."""
        if not self._state_store:
            return
        probe = self._cleanup_probe
        if probe and not Path(monitor_root, probe).exists():
            logger.warning(f"清理探针不可用，跳过缺失对账：{monitor_root}")
            return
        stale = [
            record for record in self._state_store.records_for_root(monitor_root)
            if record.source_rel not in seen
        ]
        if not stale or self._cleanup_mode == "off":
            return
        if self._cleanup_mode == "event":
            # Scans never perform immediate cleanup. Only explicit watcher deletions do.
            return
        outputs = [output for record in stale for output in record.outputs]
        if self._task_store:
            batch_id = self._task_store.create_cleanup_batch(monitor_root, outputs, records=stale)
            logger.warning(f"发现 {len(stale)} 个缺失源文件，已创建待确认清理批次：{batch_id}")

    def _record_run_result(self, run_id: Optional[str], result: Optional[dict]) -> None:
        if not run_id or not self._task_store:
            return
        result = result or {}
        status = result.get("status")
        if status == "processed":
            self._task_store.update_run(run_id, processed=1)
        elif status == "unchanged":
            self._task_store.update_run(run_id, unchanged=1)
        elif status in {"failed", "unstable", "invalid_target", "invalid_cloud", "invalid_content"}:
            self._task_store.update_run(run_id, failed=1)
        elif status == "deleted":
            self._task_store.update_run(run_id, deleted=1)
        else:
            self._task_store.update_run(run_id, skipped=1)

        default_status = {
            "processed": "generated_strm",
            "unchanged": "existing_skipped",
            "failed": "failed",
            "unstable": "failed",
            "invalid_target": "failed",
            "invalid_cloud": "failed",
            "invalid_content": "failed",
        }.get(status, "existing_skipped")
        statuses = self._normalise_result_statuses(result.get("result_statuses"), default_status)
        self._task_store.update_run_result_counts(run_id, statuses)

    @classmethod
    def _normalise_result_statuses(cls, statuses, default: str = "existing_skipped") -> Dict[str, int]:
        """Keep all task result counts within the five user-facing categories."""
        raw_items = statuses.items() if isinstance(statuses, dict) else ((value, 1) for value in (statuses or [default]))
        normalized: Dict[str, int] = {}
        for key, value in raw_items:
            try:
                amount = int(value)
            except (TypeError, ValueError):
                continue
            if amount <= 0:
                continue
            result_key = cls._result_status_aliases.get(str(key), default)
            normalized[result_key] = normalized.get(result_key, 0) + amount
        return normalized or {default: 1}

    def _pump_reliable_queue(self):
        if self._sync_engine:
            self._sync_engine.pump()

    def _run_reliable_job(self, job: PendingJob) -> dict:
        if self._is_run_cancelled(job.payload.get("run_id")):
            return {"status": "cancelled"}
        with self._processing_options_lock:
            previous_options = (self._copy_files, self._copy_subtitles)
            if "copy_files" in job.payload:
                self._copy_files = _as_bool(job.payload.get("copy_files"))
            if "copy_subtitles" in job.payload:
                self._copy_subtitles = _as_bool(job.payload.get("copy_subtitles"))
            try:
                if job.action == "delete":
                    if self._cleanup_mode == "event":
                        cleanup = self.__handle_deleted_file(job.path, job.monitor_root)
                        failures = cleanup.get("failed") or []
                        if failures:
                            reason = "; ".join(
                                f"{item.get('path')}: {item.get('error')}" for item in failures
                            )
                            actual_target = str(failures[0].get("path") or "")
                            diagnosis = classify_failure(reason, actual_target)
                            retryable = bool(diagnosis.get("retryable"))
                            return {
                                "status": "unstable" if retryable else "failed",
                                "reason": reason,
                                "retryable": retryable,
                                "actual_target": actual_target,
                                "diagnosis": diagnosis,
                                "removed": cleanup.get("removed") or [],
                                "failed": failures,
                            }
                        return {"status": "deleted", **cleanup}
                    return {"status": "ignored"}
                return self.__handle_file(
                    event_path=job.path,
                    mon_path=job.monitor_root,
                    wait_stable=bool(job.payload.get("wait_stable")),
                )
            finally:
                self._copy_files, self._copy_subtitles = previous_options

    def _complete_reliable_job(self, job: PendingJob, result: dict):
        run_id = job.payload.get("run_id")
        if self._is_run_cancelled(run_id):
            return
        self._record_run_result(run_id, result)
        self._record_scan_result(run_id, result)
        if run_id and self._task_store:
            with self._scan_finalized_lock:
                finalized = str(run_id) in self._scan_finalized_run_ids
            if finalized and self._task_store.finish_run_if_settled(run_id):
                self._finish_scan_progress(run_id)
                with self._scan_finalized_lock:
                    self._scan_finalized_run_ids.discard(str(run_id))

    @eventmanager.register(EventType.PluginAction)
    def strm_one(self, event: Event = None):
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "cloudstrm_file":
                return
            file_path = event_data.get("file_path")
            if not file_path:
                logger.error(f"缺少参数：{event_data}")
                return

            # 遍历所有监控目录
            mon_path = find_monitor_path(file_path, self._strm_dir_conf)

            if not mon_path:
                logger.error(f"未找到文件 {file_path} 对应的监控目录")
                return

            # 处理单文件
            self.__handle_file(event_path=file_path, mon_path=mon_path)

    @eventmanager.register(EventType.PluginAction)
    def remote_sync(self, event: Event = None):
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "CloudStrmButler":
                return
            logger.info("开始云盘Strm小管家全量同步")
            self.scan()

    def event_handler(self, event, mon_path: str, text: str, event_path: str, **kwargs):
        """Handle create/modify/move/delete events from watchdog."""
        if getattr(event, "is_directory", False):
            return
        if is_ignored_path(event_path) or is_temporary_path(event_path):
            return

        action = kwargs.get("action", "created")
        old_event_path = kwargs.get("old_event_path")
        if self._reliable_engine and self._sync_engine:
            if action == "deleted":
                self._sync_engine.enqueue(mon_path, event_path, "delete")
            else:
                if old_event_path and old_event_path != event_path:
                    self._sync_engine.enqueue(mon_path, old_event_path, "delete")
                self._sync_engine.enqueue(mon_path, event_path, "sync", payload={"wait_stable": True})
            return
        if action == "deleted":
            logger.debug("监控到文件%s：%s", text, event_path)
            self.__handle_deleted_file(event_path=event_path, mon_path=mon_path)
            return

        if old_event_path and old_event_path != event_path:
            self.__handle_deleted_file(event_path=old_event_path, mon_path=mon_path)

        logger.debug("监控到文件%s：%s", text, event_path)
        self.__handle_file(event_path=event_path, mon_path=mon_path, wait_stable=True)

    @staticmethod
    def __remap_path(event_path: str, source_root: str, target_root: str) -> Optional[str]:
        """Map one local path from a monitor root to another root."""
        return map_path(event_path, source_root, target_root)

    def __wait_stable_file(self, event_path: str, max_wait: int = 30) -> bool:
        """Wait until the source file stops changing."""
        return wait_for_stable_file(event_path, max_wait=max_wait)

    def __handle_file(self, event_path: str, mon_path: str, wait_stable: bool = False):
        """Synchronize one source file and persist its state signature."""
        source = Path(event_path)
        if not source.exists() or not source.is_file():
            return {"status": "missing", "result_statuses": ["failed"]}
        if is_ignored_path(event_path) or is_temporary_path(event_path):
            return {"status": "ignored", "result_statuses": ["skipped"]}
        if wait_stable and not self.__wait_stable_file(event_path):
            return {"status": "unstable", "result_statuses": ["failed"]}

        source_rel = relative_path(event_path, mon_path)
        if source_rel is None:
            logger.error(f"文件 {event_path} 不在监控目录 {mon_path} 下")
            return {"status": "outside_root", "result_statuses": ["failed"]}

        active_key = path_key(event_path)
        with self._active_lock:
            if active_key in self._active_paths:
                return {"status": "duplicate", "result_statuses": ["skipped"]}
            self._active_paths.add(active_key)

        outputs: List[str] = []
        actual_target = ""
        previous_record = (
            self._state_store.get(mon_path, str(source_rel))
            if self._state_store
            else None
        )
        sidecar_records = []
        try:
            stat = source.stat()
            mtime_ns = int(getattr(stat, "st_mtime_ns", stat.st_mtime * 1_000_000_000))
            size = int(stat.st_size)
            cloud_dir = self._cloud_dir_conf.get(mon_path)
            strm_dir = self._strm_dir_conf.get(mon_path)
            format_str = self._format_conf.get(mon_path)
            target_file = self.__remap_path(event_path, mon_path, strm_dir)
            cloud_file = self.__remap_path(event_path, mon_path, cloud_dir)
            if not target_file:
                logger.error(f"无法计算文件 {event_path} 的目标路径")
                reason = f"无法计算文件 {event_path} 的目标路径"
                diagnosis = {"reason_code": "invalid_target", "actual_target": str(strm_dir or "")}
                return {
                    "status": "invalid_target",
                    "reason": reason,
                    "actual_target": str(strm_dir or ""),
                    "diagnosis": diagnosis,
                    "result_statuses": ["failed"],
                }
            if not cloud_file and "{cloud_file}" in (format_str or ""):
                logger.error(f"无法计算文件 {event_path} 的云盘路径")
                actual_target = str(Path(target_file).with_suffix(".strm"))
                reason = f"无法计算文件 {event_path} 的云盘路径"
                diagnosis = {"reason_code": "invalid_cloud", "actual_target": actual_target}
                return {
                    "status": "invalid_cloud",
                    "reason": reason,
                    "actual_target": actual_target,
                    "diagnosis": diagnosis,
                    "result_statuses": ["failed"],
                }

            suffix = source.suffix.lower()
            content_hash = ""
            if suffix in self._media_extensions:
                strm_content = self.__format_content(
                    format_str=format_str,
                    local_file=event_path,
                    cloud_file=str(cloud_file),
                    uriencode=self._uriencode,
                )
                if not strm_content:
                    logger.error(f"文件 {event_path} 的 STRM 模板无效")
                    actual_target = str(Path(target_file).with_suffix(".strm"))
                    reason = f"文件 {event_path} 的 STRM 模板无效"
                    diagnosis = {"reason_code": "invalid_template", "actual_target": actual_target}
                    return {
                        "status": "invalid_content",
                        "reason": reason,
                        "actual_target": actual_target,
                        "diagnosis": diagnosis,
                        "result_statuses": ["failed"],
                    }
                strm_content = self._apply_path_replacements(strm_content)
                content_hash = hashlib.sha256(strm_content.encode("utf-8")).hexdigest()
                sidecar_paths = list(self._iter_sidecars(source))
                strm_target = str(Path(target_file).with_suffix(".strm"))
                strm_existed = Path(strm_target).is_file()
                expected_outputs = [strm_target]
                expected_outputs.extend(
                    sidecar_target
                    for sidecar_path in sidecar_paths
                    if self._should_copy_sidecar(str(sidecar_path))
                    if (sidecar_target := self.__remap_path(str(sidecar_path), mon_path, strm_dir))
                )
                current_sidecar_targets = {
                    path_key(output)
                    for output in expected_outputs
                    if path_key(output) != path_key(strm_target)
                }
                stale_sidecar_outputs = [
                    output
                    for output in (previous_record.outputs if previous_record else [])
                    if path_key(output) != path_key(strm_target)
                    and path_key(output) not in current_sidecar_targets
                ]
                sidecar_needs_processing = any(
                    self._should_copy_sidecar(str(sidecar_path))
                    and bool(sidecar_target)
                    and not self._sidecar_record_is_current(
                        str(sidecar_path), mon_path, sidecar_target
                    )
                    for sidecar_path in sidecar_paths
                    for sidecar_target in [
                        self.__remap_path(str(sidecar_path), mon_path, strm_dir)
                    ]
                ) or bool(stale_sidecar_outputs)
                media_record_current = self._record_is_current(
                    source_rel, mtime_ns, size, content_hash, mon_path, expected_outputs
                )
                if not self._cover and media_record_current and not sidecar_needs_processing:
                    return {"status": "unchanged", "result_statuses": ["existing_skipped"]}
                strm_content_changed = not strm_existed
                if strm_existed and not self._cover:
                    try:
                        strm_content_changed = Path(strm_target).read_text(encoding="utf-8") != strm_content
                    except (OSError, UnicodeError):
                        strm_content_changed = True
                if self._cover or not strm_existed or strm_content_changed:
                    write_check = self._check_write_target(strm_target)
                    if not write_check["writable"]:
                        return {
                            "status": "failed",
                            "reason": write_check["raw_error"],
                            "retryable": False,
                            "actual_target": strm_target,
                            "diagnosis": write_check,
                            "result_statuses": ["failed"],
                        }
                actual_target = strm_target
                strm_output = self.__create_strm_file(strm_file=target_file, strm_content=strm_content, source_file=event_path)
                if strm_output:
                    outputs.append(strm_output)
                result_statuses = [
                    "generated_strm" if self._cover or strm_content_changed else "existing_skipped"
                ]
                for sidecar_path in sidecar_paths:
                    sidecar_target = self.__remap_path(str(sidecar_path), mon_path, strm_dir)
                    if sidecar_target:
                        actual_target = sidecar_target
                        if (
                            self._should_copy_sidecar(str(sidecar_path))
                            and not self._cover
                            and self._sidecar_record_is_current(
                                str(sidecar_path), mon_path, sidecar_target
                            )
                        ):
                            outputs.append(sidecar_target)
                            continue
                        if not self._should_copy_sidecar(str(sidecar_path)):
                            self._remove_recorded_output(
                                mon_path, sidecar_target, source_path=str(sidecar_path)
                            )
                            continue
                        sidecar_outputs = self.__handle_other_files(str(sidecar_path), sidecar_target)
                        outputs.extend(sidecar_outputs)
                        if sidecar_outputs:
                            result_statuses.extend(self._sidecar_result_statuses(str(sidecar_path)))
                            sidecar_records.append((str(sidecar_path), sidecar_target))
            else:
                should_copy_sidecar = self._should_copy_sidecar(event_path)
                if not should_copy_sidecar:
                    self._remove_recorded_output(mon_path, target_file, source_path=event_path)
                    return {"status": "skipped", "result_statuses": ["skipped"]}
                content_hash = self._file_content_hash(source)
                sidecar_expected = [
                    target_file
                ]
                if not self._cover and self._sidecar_record_is_current(
                    event_path, mon_path, target_file
                ):
                    return {"status": "unchanged", "result_statuses": ["existing_skipped"]}
                actual_target = target_file
                outputs.extend(self.__handle_other_files(event_path, target_file))
                result_statuses = self._sidecar_result_statuses(event_path) if outputs else ["skipped"]

            if not outputs:
                if suffix not in self._media_extensions:
                    self._remove_recorded_output(mon_path, target_file, source_path=event_path)
                elif previous_record:
                    self._remove_obsolete_outputs(mon_path, str(source_rel), previous_record.outputs, [])
                else:
                    self._remove_recorded_output(mon_path, target_file, source_path=event_path)
                return {"status": "skipped", "result_statuses": ["skipped"]}

            if self._state_store is not None:
                if previous_record:
                    self._remove_obsolete_outputs(mon_path, str(source_rel), previous_record.outputs, outputs)
                for sidecar_path, sidecar_target in sidecar_records:
                    self._upsert_sidecar_record(sidecar_path, sidecar_target, mon_path)
                self._state_store.upsert(
                    monitor_root=mon_path,
                    source_rel=str(source_rel),
                    mtime_ns=mtime_ns,
                    size=size,
                    content_hash=content_hash,
                    outputs=outputs,
                    config_fingerprint=self._config_fingerprint,
                )
            self._invalidate_processing_overview()
            return {
                "status": "processed",
                "outputs": outputs,
                "result_statuses": result_statuses,
            }
        except Exception as exc:
            logger.error("目录监控发生错误：%s - %s", str(exc), traceback.format_exc())
            actual_target = str(actual_target or locals().get("target_file") or "")
            diagnosis = self._check_write_target(actual_target) if actual_target else {}
            return {
                "status": "failed",
                "reason": str(exc),
                "retryable": self._is_retryable_io_error(exc),
                "actual_target": actual_target,
                "diagnosis": diagnosis,
                "result_statuses": ["failed"],
            }
        finally:
            with self._active_lock:
                self._active_paths.discard(active_key)

    def _should_copy_sidecar(self, event_path: str) -> bool:
        suffix = Path(event_path).suffix.lower()
        return (
            (self._copy_files and suffix in self._other_extensions)
            or (self._copy_subtitles and suffix in self._subtitle_extensions)
        )

    def _sidecar_record_is_current(
        self, event_path: str, mon_path: str, target_file: str
    ) -> bool:
        if not self._state_store or not target_file:
            return False
        source = Path(event_path)
        source_rel = relative_path(event_path, mon_path)
        if source_rel is None:
            return False
        try:
            stat = source.stat()
        except OSError:
            return False
        content_hash = self._file_content_hash(source)
        if not content_hash or not Path(target_file).is_file():
            return False
        if self._file_content_hash(Path(target_file)) != content_hash:
            return False
        return self._record_is_current(
            source_rel,
            int(getattr(stat, "st_mtime_ns", stat.st_mtime * 1_000_000_000)),
            int(stat.st_size),
            content_hash,
            mon_path,
            [target_file],
        )

    @staticmethod
    def _check_write_target(target: str) -> Dict[str, Any]:
        """Probe the real output directory so failures explain the host-side cause."""
        target_path = Path(str(target or "")).expanduser()
        if not str(target_path):
            return {
                "writable": False,
                "reason_code": "path_missing",
                "reason_label": "STRM 输出目标为空",
                "raw_error": "STRM 输出目标为空",
                "repair_hint": "检查规则中的 STRM 输出目录和路径映射。",
                "actual_target": "",
            }
        parent = target_path.parent
        probe_parent = parent
        try:
            probe_parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            diagnosis = "read_only_mount" if getattr(exc, "errno", None) == 30 else "permission_denied" if getattr(exc, "errno", None) in {1, 13} else "storage_unavailable"
            return {
                "writable": False,
                "reason_code": diagnosis,
                "reason_label": {"read_only_mount": "目标挂载为只读", "permission_denied": "目标目录不可写", "storage_unavailable": "目标挂载暂时不可用"}[diagnosis],
                "raw_error": str(exc),
                "repair_hint": "检查 NAS 挂载状态、读写参数、容器 UID/GID 和 ACL；修复宿主环境后再重试。",
                "actual_target": str(target_path),
            }
        if not os.access(str(probe_parent), os.W_OK | os.X_OK):
            return {
                "writable": False,
                "reason_code": "permission_denied",
                "reason_label": "目标目录不可写",
                "raw_error": f"目标目录不可写: {parent}",
                "repair_hint": "检查 MoviePilot 容器 UID/GID、NAS ACL 和挂载是否为只读；这些权限需要在宿主环境修复。",
                "actual_target": str(target_path),
            }
        probe_path = None
        try:
            with tempfile.NamedTemporaryFile(prefix=".cloudstrm-write-test-", dir=str(probe_parent), delete=False) as probe:
                probe.write(b"cloudstrm-write-test")
                probe_path = probe.name
            return {
                "writable": True,
                "reason_code": "ok",
                "reason_label": "目标目录可写",
                "raw_error": "",
                "repair_hint": "",
                "actual_target": str(target_path),
            }
        except OSError as exc:
            diagnosis = "read_only_mount" if getattr(exc, "errno", None) == 30 else "permission_denied" if getattr(exc, "errno", None) in {1, 13} else "storage_unavailable"
            labels = {
                "read_only_mount": "目标挂载为只读",
                "permission_denied": "目标目录不可写",
                "storage_unavailable": "目标挂载暂时不可用",
            }
            return {
                "writable": False,
                "reason_code": diagnosis,
                "reason_label": labels[diagnosis],
                "raw_error": str(exc),
                "repair_hint": "检查 NAS 挂载状态、读写参数、容器 UID/GID 和 ACL；修复宿主环境后再重试。",
                "actual_target": str(target_path),
            }
        finally:
            if probe_path:
                try:
                    Path(probe_path).unlink(missing_ok=True)
                except OSError:
                    pass

    def _sidecar_result_statuses(self, event_path: str) -> List[str]:
        suffix = Path(event_path).suffix.lower()
        statuses = []
        if self._copy_files and suffix in self._other_extensions:
            statuses.append("copied_non_media")
        if self._copy_subtitles and suffix in self._subtitle_extensions:
            statuses.append("copied_subtitle")
        return statuses or ["skipped"]

    def __handle_other_files(self, event_path: str, target_file: str):
        """Copy a sidecar when configured and return its target path."""
        if not target_file:
            return []
        outputs: List[str] = []
        suffix = Path(event_path).suffix.lower()
        if self._copy_files and suffix in self._other_extensions:
            output = self.__copy_sidecar_file(event_path, target_file, "非媒体文件")
            if output:
                outputs.append(output)
        if self._copy_subtitles and suffix in self._subtitle_extensions:
            output = self.__copy_sidecar_file(event_path, target_file, "字幕文件")
            if output:
                outputs.append(output)
        return outputs

    def __copy_sidecar_file(self, event_path: str, target_file: str, kind: str):
        """Copy one sidecar through a same-directory temporary file."""
        temp_path = None
        try:
            target_dir = os.path.dirname(target_file)
            if target_dir:
                os.makedirs(target_dir, exist_ok=True)
            write_check = self._check_write_target(target_file)
            if not write_check["writable"]:
                raise PermissionError(write_check["raw_error"])
            target_path = Path(target_file)
            with tempfile.NamedTemporaryFile(
                prefix=f".{target_path.name}.",
                suffix=".tmp",
                dir=str(target_path.parent),
                delete=False,
            ) as temp_file:
                temp_path = Path(temp_file.name)
            shutil.copy2(str(event_path), str(temp_path))
            if temp_path.stat().st_size != Path(event_path).stat().st_size:
                raise OSError(11, f"{kind}复制后大小校验失败")
            try:
                os.replace(str(temp_path), str(target_path))
            except OSError as atomic_error:
                # Some SMB/FUSE mounts allow writing but reject rename/replace.
                logger.warning(
                    f"{kind}原子替换失败，回退普通复制到 {target_file}：{atomic_error}"
                )
                shutil.copy2(str(event_path), str(target_path))
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
                temp_path = None
            temp_path = None
            logger.info(f"复制{kind} {str(event_path)} 到 {target_file}")
            return target_file
        except PermissionError:
            logger.error(f"复制{kind}失败：目标目录没有写入权限 {os.path.dirname(target_file)}")
            raise
        except Exception as exc:
            logger.error(f"复制{kind}失败：{exc}")
            raise
        finally:
            if temp_path:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass

    def _upsert_sidecar_record(self, event_path: str, target_file: str, mon_path: str) -> None:
        if not self._state_store:
            return
        source = Path(event_path)
        stat = source.stat()
        source_rel = relative_path(event_path, mon_path)
        if source_rel is None:
            return
        content_hash = self._file_content_hash(source)
        if not content_hash:
            return
        self._state_store.upsert(
            monitor_root=mon_path,
            source_rel=str(source_rel),
            mtime_ns=int(getattr(stat, "st_mtime_ns", stat.st_mtime * 1_000_000_000)),
            size=int(stat.st_size),
            content_hash=content_hash,
            outputs=[target_file],
            config_fingerprint=self._config_fingerprint,
        )

    def _remove_recorded_output(
        self,
        mon_path: str,
        target_file: str,
        source_path: Optional[str] = None,
        source_rel: Optional[str] = None,
    ) -> None:
        if not self._state_store or not target_file:
            return
        if source_rel is None and source_path:
            relative = relative_path(source_path, mon_path)
            source_rel = str(relative) if relative is not None else None
        if not source_rel:
            logger.warning(f"跳过清理未确认归属的生成文件：{target_file}")
            return
        record = self._state_store.get(mon_path, source_rel)
        if not record or path_key(target_file) not in {path_key(output) for output in record.outputs}:
            logger.warning(f"跳过清理未由插件记录的输出：{target_file}（来源：{source_rel}）")
            return
        cleanup = self._remove_outputs([target_file])
        if not cleanup.get("failed") and self._state_store.remove_output(mon_path, target_file):
            self._invalidate_processing_overview()

    def _remove_obsolete_outputs(self, mon_path: str, source_rel: str, previous_outputs, current_outputs) -> None:
        if not self._state_store:
            return
        current = {path_key(output) for output in current_outputs or []}
        for output in previous_outputs or []:
            if path_key(output) in current:
                continue
            record = self._state_store.get(mon_path, source_rel)
            if not record or path_key(output) not in {path_key(item) for item in record.outputs}:
                logger.warning(f"跳过清理未由当前来源记录的输出：{output}（来源：{source_rel}）")
                continue
            if self._state_store.has_output_for_other_source(mon_path, output, source_rel):
                self._state_store.remove_output_for_source(mon_path, source_rel, output)
                continue
            cleanup = self._remove_outputs(
                [output],
                notify_emby=Path(output).suffix.lower() == ".strm",
            )
            if not cleanup.get("failed"):
                self._state_store.remove_output_for_source(mon_path, source_rel, output)

    @staticmethod
    def _is_retryable_io_error(error: Exception) -> bool:
        """Classify transient NAS/FUSE failures for durable queue retries."""
        errno = getattr(error, "errno", None)
        if errno in {5, 6, 11, 110, 111, 113, 116}:
            return True
        message = str(error).lower()
        return any(token in message for token in (
            "connection timed out",
            "i/o error",
            "input/output error",
            "temporarily unavailable",
            "stale file handle",
        ))


    def _record_is_current(
        self,
        source_rel,
        mtime_ns: int,
        size: int,
        content_hash: str,
        mon_path: str,
        expected_outputs=None,
    ) -> bool:
        if self._state_store is None:
            return False
        record = self._state_store.get(mon_path, str(source_rel))
        if not record:
            return False
        if record.config_fingerprint != self._config_fingerprint:
            return False
        if record.mtime_ns != mtime_ns or record.size != size or record.content_hash != content_hash:
            return False
        if not all(Path(output).exists() for output in record.outputs):
            return False
        if expected_outputs is None:
            return True
        recorded = {path_key(output) for output in record.outputs}
        return all(
            path_key(output) in recorded and Path(output).exists()
            for output in expected_outputs
        )

    def _apply_path_replacements(self, content: str) -> str:
        for source, target in self._path_replacements.items():
            if source in content:
                content = content.replace(source, target)
                logger.debug(f"应用路径替换规则: {source} -> {target}")
        return content

    @staticmethod
    def _iter_sidecars(source: Path):
        stem = source.stem
        # Match the legacy ``Path.glob(f"{stem}.*")`` behavior, including
        # language and release tags such as ``movie.zh-CN.srt``.
        prefix = f"{stem}.".casefold()
        for sibling in source.parent.iterdir():
            if (
                sibling.is_file()
                and sibling != source
                and sibling.name.casefold().startswith(prefix)
            ):
                yield sibling
        thumb = source.parent / f"{stem}-thumb.jpg"
        if thumb.is_file():
            yield thumb

    def __handle_deleted_file(self, event_path: str, mon_path: str):
        """Remove generated outputs and the persisted record for a deleted source."""
        empty_cleanup = {"removed": [], "failed": []}
        if self._state_store is None:
            return empty_cleanup
        source_rel = relative_path(event_path, mon_path)
        if source_rel is None:
            return empty_cleanup
        record = self._state_store.get(mon_path, str(source_rel))
        if record:
            is_sidecar = (
                Path(event_path).suffix.lower() in self._other_extensions
                or Path(event_path).suffix.lower() in self._subtitle_extensions
            )
            if is_sidecar:
                owned_outputs = [
                    output for output in record.outputs
                ]
                cleanup = self._remove_outputs(owned_outputs)
                for output in cleanup.get("removed") or []:
                    self._state_store.remove_output(mon_path, output)
                if not cleanup.get("failed"):
                    self._state_store.delete_if_matches(mon_path, str(source_rel), record.outputs)
                return cleanup
            owned_outputs = [
                output
                for output in record.outputs
                if not self._state_store.has_output_for_other_source(mon_path, output, str(source_rel))
            ]
            cleanup = self._remove_outputs(owned_outputs, notify_emby=bool(record.content_hash))
            for output in cleanup.get("removed") or []:
                self._state_store.remove_output_for_source(mon_path, str(source_rel), output)
            if not cleanup.get("failed"):
                self._state_store.delete(mon_path, str(source_rel))
            return cleanup
        else:
            logger.warning(f"跳过清理未由插件记录的删除事件：{event_path}")
            return empty_cleanup

    def _remove_outputs(self, outputs, notify_emby: bool = False):
        """Remove only files that are recorded as generated by this plugin."""
        removed = []
        failed = []
        for output in outputs or []:
            path = Path(output)
            try:
                path.unlink(missing_ok=True)
                logger.info(f"已清理生成文件 {path}")
                removed.append(str(path))
            except OSError as exc:
                logger.warning(f"清理生成文件失败 {path}：{exc}")
                failed.append({"path": str(path), "error": str(exc)})
                continue
            if notify_emby and path.suffix.lower() == ".strm" and self._refresh_emby:
                try:
                    self._queue_emby_refresh(str(path), update_type="Deleted")
                except Exception as exc:
                    logger.warning(f"通知 Emby 删除失败 {path}：{exc}")
        return {"removed": removed, "failed": failed}

    @staticmethod
    def _file_content_hash(source: Path) -> str:
        """Hash sidecars so coarse NAS timestamps cannot hide replacements."""
        digest = hashlib.sha256()
        try:
            with source.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        except OSError:
            return ""
        return digest.hexdigest()

    @staticmethod
    def __format_content(format_str: str, local_file: str, cloud_file: str, uriencode: bool):
        """Render a STRM template."""
        return format_content(format_str, local_file, cloud_file, uriencode)

    def __create_strm_file(self, strm_file: str, strm_content: str, source_file: str = None) -> Optional[str]:
        """Write a STRM file atomically and trigger optional post-processing."""
        if not strm_content:
            logger.error(f"STRM 内容为空：{strm_file}")
            return None
        try:
            parent = Path(strm_file).parent
            if not parent.exists():
                logger.info(f"创建目标文件夹 {parent}")
                parent.mkdir(parents=True, exist_ok=True)

            strm_file = str(parent / f"{Path(strm_file).stem}.strm")
            if Path(strm_file).exists() and not self._cover:
                try:
                    if Path(strm_file).read_text(encoding="utf-8") == strm_content:
                        logger.debug(f"目标文件 {strm_file} 内容未变化")
                        return strm_file
                except (OSError, UnicodeError):
                    pass

            temp_file = Path(strm_file).with_name(f".{Path(strm_file).name}.tmp")
            with temp_file.open("w", encoding="utf-8", newline="") as file:
                file.write(strm_content)
            os.replace(str(temp_file), strm_file)
            logger.info(f"创建strm文件成功 {strm_file} -> {strm_content}")

            source_suffix = Path(source_file).suffix.lower() if source_file else ""
            if self._url and source_suffix in self._media_extensions:
                try:
                    response = RequestUtils(content_type="application/json").post(
                        url=self._url,
                        json={"path": strm_content, "type": "add"},
                    )
                    if response is None or getattr(response, "status_code", 0) not in range(200, 300):
                        logger.warning(f"任务推送失败：{strm_file}")
                except Exception as exc:
                    logger.warning(f"任务推送异常：{exc}")

            if self._notify and source_suffix in self._media_extensions:
                self._record_media_notification(strm_file)
            if self._refresh_emby and self._mediaservers:
                self._queue_emby_refresh(strm_file)
            return strm_file
        except Exception as exc:
            logger.error(f"创建strm文件失败 {strm_file} -> {exc}")
            raise

    def _record_media_notification(self, strm_file: str):
        """Queue a media notification entry behind a dedicated lock."""
        file_meta = MetaInfoPath(Path(strm_file))
        match = re.search(r"tmdbid=(\d+)", strm_file)
        if match:
            file_meta.tmdbid = match.group(1)
        key = f"{file_meta.cn_name} ({file_meta.year}){f' {file_meta.season}' if file_meta.season else ''}"
        with self._media_lock:
            media_list = self._medias.get(key) or {}
            episodes = list(media_list.get("episodes") or [])
            if file_meta.begin_episode:
                episode = int(file_meta.begin_episode)
                if episode not in episodes:
                    episodes.append(episode)
            self._medias[key] = {
                "episodes": episodes,
                "file_meta": file_meta,
                "type": "tv" if file_meta.season else "movie",
                "time": datetime.now(),
            }

    def __refresh_emby_file(self, strm_file: str, update_type: str = "Created"):
        """Notify configured Emby servers and keep failures observable."""
        self._refresh_emby_updates([(strm_file, update_type)])

    def _refresh_emby_updates(self, updates):
        """Send a batch of created/deleted paths in one request per configured server."""
        if not updates:
            return
        emby_servers = self.mediaserver_helper.get_services(
            name_filters=self._mediaservers, type_filter="emby"
        )
        if not emby_servers:
            logger.error("未配置Emby媒体服务器")
            return
        mapped_updates = [
            {"Path": self.__get_path(paths=self._emby_paths, file_path=strm_file), "UpdateType": update_type}
            for strm_file, update_type in updates
        ]
        for emby_name, emby_server in emby_servers.items():
            emby = emby_server.instance
            try:
                res = emby.post_data(
                    url="[HOST]emby/Library/Media/Updated?api_key=[APIKEY]&reqformat=json",
                    data=json.dumps({
                        "Updates": mapped_updates
                    }),
                    headers={"Content-Type": "application/json"},
                )
                if res and res.status_code in [200, 204]:
                    logger.info(f"媒体服务器 {emby_name} 已刷新 {len(mapped_updates)} 个路径")
                else:
                    status_code = getattr(res, "status_code", "无响应")
                    logger.error(f"通知媒体服务器 {emby_name} 失败，错误码：{status_code}")
            except Exception as err:
                logger.error(f"通知媒体服务器 {emby_name} 失败：{err}")

    def _queue_emby_refresh(self, strm_file: str, update_type: str = "Created"):
        """Coalesce bursts of updates and flush them every five seconds or at 200 paths."""
        with self._emby_refresh_lock:
            self._pending_emby_refreshes[strm_file] = update_type
            should_flush = len(self._pending_emby_refreshes) >= 200
        if should_flush:
            self._flush_emby_refreshes()

    def _flush_emby_refreshes(self):
        with self._emby_refresh_lock:
            pending = list(self._pending_emby_refreshes.items())
            self._pending_emby_refreshes.clear()
        self._refresh_emby_updates(pending)

    def __get_path(self, paths, file_path: str):
        """Map a local file path using the longest matching library root."""
        return map_library_path(paths or {}, file_path)

    @staticmethod
    def _new_command_summary() -> Dict[str, Any]:
        return {
            "total": 0,
            "processed": 0,
            "unchanged": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
            "result_counts": {
                "existing_skipped": 0,
                "copied_non_media": 0,
                "copied_subtitle": 0,
                "generated_strm": 0,
                "failed": 0,
            },
        }

    @staticmethod
    def _new_command_progress() -> Dict[str, Any]:
        return {
            "running": False,
            "run_id": "",
            "label": "",
            "monitor_root": "",
            "phase": "idle",
            "total": 0,
            "processed": 0,
            "unchanged": 0,
            "skipped": 0,
            "failed": 0,
            "current_path": "",
            "last_progress_at": None,
            "started_at": None,
            "finished_at": None,
            "stalled": False,
            "stalled_seconds": 0,
            "result_counts": {
                "existing_skipped": 0,
                "copied_non_media": 0,
                "copied_subtitle": 0,
                "generated_strm": 0,
                "failed": 0,
            },
            "errors": [],
        }

    def _update_command_progress(self, **updates) -> None:
        with self._command_progress_lock:
            self._command_progress.update(updates)

    def _touch_command_progress(self, current_path: Optional[str] = None, phase: Optional[str] = None) -> None:
        updates = {"last_progress_at": time.time()}
        if current_path is not None:
            updates["current_path"] = str(current_path)
        if phase is not None:
            updates["phase"] = phase
        self._update_command_progress(**updates)

    def _merge_result_counts(self, target: Dict[str, int], statuses) -> None:
        values = statuses.keys() if isinstance(statuses, dict) else statuses or []
        increments = statuses.items() if isinstance(statuses, dict) else ((key, 1) for key in values)
        for key, value in increments:
            key = str(key)
            if key:
                target[key] = int(target.get(key) or 0) + int(value)

    def _command_progress_snapshot(self) -> Dict[str, Any]:
        with self._command_progress_lock:
            snapshot = dict(self._command_progress)
            snapshot["result_counts"] = dict(self._command_progress.get("result_counts") or {})
            snapshot["errors"] = list(self._command_progress.get("errors") or [])
        last_progress_at = snapshot.get("last_progress_at")
        elapsed = max(0, int(time.time() - float(last_progress_at))) if last_progress_at else 0
        snapshot["stalled_seconds"] = elapsed if snapshot.get("running") else 0
        snapshot["stalled"] = bool(snapshot.get("running") and elapsed >= self._command_stall_seconds)
        return snapshot

    def _record_command_file(self, summary: Dict[str, Any], run_id: Optional[str], event_path: str, mon_path: str) -> None:
        summary["total"] += 1
        self._touch_command_progress(current_path=event_path, phase="processing")
        if run_id and self._task_store:
            self._task_store.update_run(run_id, queued=1)
        try:
            result = self.__handle_file(event_path=event_path, mon_path=mon_path) or {}
        except Exception as exc:
            result = {"status": "failed", "reason": str(exc), "result_statuses": ["failed"]}
            logger.exception("手动 /strm 处理文件失败：%s", event_path)

        status = str(result.get("status") or "skipped").lower()
        if status == "processed":
            summary["processed"] += 1
            progress_key = "processed"
        elif status == "unchanged":
            summary["unchanged"] += 1
            progress_key = "unchanged"
        elif status in {"failed", "unstable", "invalid_target", "invalid_cloud", "invalid_content"}:
            summary["failed"] += 1
            progress_key = "failed"
            reason = str(result.get("reason") or status).replace("\n", " ").strip()
            detail = f"{event_path}: {reason}"
            if detail not in summary["errors"] and len(summary["errors"]) < 3:
                summary["errors"].append(detail[:200])
        else:
            summary["skipped"] += 1
            progress_key = "skipped"

        default_status = {
            "processed": "generated_strm",
            "unchanged": "existing_skipped",
            "failed": "failed",
        }.get(status, "existing_skipped")
        statuses = self._normalise_result_statuses(result.get("result_statuses"), default_status)
        self._merge_result_counts(summary["result_counts"], statuses)
        with self._command_progress_lock:
            self._command_progress[progress_key] = int(self._command_progress.get(progress_key) or 0) + 1
            self._merge_result_counts(self._command_progress["result_counts"], statuses)
            self._command_progress["last_progress_at"] = time.time()
        self._record_run_result(run_id, result)

    def _record_command_failure(self, summary: Dict[str, Any], run_id: Optional[str], reason: str) -> None:
        summary["total"] += 1
        summary["failed"] += 1
        detail = str(reason).replace("\n", " ").strip()[:200]
        if detail and detail not in summary["errors"] and len(summary["errors"]) < 3:
            summary["errors"].append(detail)
        summary["result_counts"]["failed"] = int(summary["result_counts"].get("failed") or 0) + 1
        if run_id and self._task_store:
            self._task_store.update_run(run_id, queued=1)
        result = {"status": "failed", "reason": reason, "result_statuses": ["failed"]}
        self._record_run_result(run_id, result)
        with self._command_progress_lock:
            self._command_progress["failed"] = int(self._command_progress.get("failed") or 0) + 1
            self._merge_result_counts(self._command_progress["result_counts"], ["failed"])
            self._command_progress["last_progress_at"] = time.time()

    def _collect_command_files(self, paths) -> List[str]:
        """Collect files before processing so the command can show a real total."""
        files: List[str] = []
        for candidate in paths or []:
            if self._command_cancel.is_set():
                break
            candidate_path = Path(candidate)
            self._touch_command_progress(current_path=str(candidate_path), phase="discovering")
            if candidate_path.is_file():
                files.append(str(candidate_path))
                continue
            if not candidate_path.is_dir():
                continue
            for root, _dirs, names in os.walk(str(candidate_path)):
                if self._command_cancel.is_set():
                    break
                self._touch_command_progress(current_path=root, phase="discovering")
                for file_name in names:
                    if self._command_cancel.is_set():
                        break
                    src_file = os.path.join(root, file_name)
                    if Path(src_file).is_file():
                        files.append(src_file)
        covered_sidecars = {
            path_key(str(sidecar))
            for media_path in files
            if Path(media_path).suffix.lower() in self._media_extensions
            for sidecar in self._iter_sidecars(Path(media_path))
        }
        return [file_path for file_path in files if path_key(file_path) not in covered_sidecars]

    def _run_command_worker(
        self,
        paths,
        mon_path: str,
        event: Event,
        label: str,
        initial_failures: Optional[List[str]],
        run_id: Optional[str],
    ) -> None:
        summary = self._new_command_summary()
        try:
            files = self._collect_command_files(paths)
            self._update_command_progress(
                total=len(files) + len(initial_failures or []),
                current_path=files[0] if files else "",
                phase="processing" if files or initial_failures else "completed",
            )
            for reason in initial_failures or []:
                if self._command_cancel.is_set():
                    break
                self._record_command_failure(summary, run_id, reason)
            for event_path in files:
                if self._command_cancel.is_set():
                    break
                self._record_command_file(summary, run_id, event_path, mon_path)
        except Exception as exc:
            logger.exception("手动 /strm 扫描失败：%s", label)
            self._record_command_failure(summary, run_id, str(exc))
        finally:
            cancelled = bool(self._command_cancel.is_set() or self._is_run_cancelled(run_id))
            if run_id and self._task_store:
                if cancelled:
                    status = "cancelled"
                    self._task_store.cancel_run(run_id, "用户取消")
                elif summary["failed"]:
                    status = "completed_with_errors"
                elif not summary["total"]:
                    status = "completed_empty"
                else:
                    status = "completed"
                self._task_store.finish_run(
                    run_id,
                    status=status,
                    message=self._command_summary_title(label, summary),
                )
            finished_at = time.time()
            self._update_command_progress(
                running=False,
                phase="cancelled" if cancelled else "completed",
                current_path="",
                finished_at=finished_at,
                total=summary["total"],
                processed=summary["processed"],
                unchanged=summary["unchanged"],
                skipped=summary["skipped"],
                failed=summary["failed"],
                result_counts=dict(summary["result_counts"]),
                errors=list(summary["errors"]),
                stalled=False,
                stalled_seconds=0,
                last_progress_at=finished_at,
            )
            self._command_running = False
            self._command_guard.release()
            self._command_cancel.clear()
            self._post_command_summary(event, label, summary)

    def _command_summary_title(self, label: str, summary: Dict[str, Any]) -> str:
        total = int(summary.get("total", 0))
        failed = int(summary.get("failed", 0))
        if total == 0:
            state = "未找到可处理文件"
        elif failed == total:
            state = "失败"
        elif failed:
            state = "部分完成"
        else:
            state = "完成"
        title = (
            f"{label} Strm生成{state}：总数 {total}，成功 {summary.get('processed', 0)}，"
            f"未变化 {summary.get('unchanged', 0)}，跳过 {summary.get('skipped', 0)}，失败 {failed}"
        )
        errors = summary.get("errors") or []
        if errors:
            title += "；失败示例：" + "；".join(errors)
        return title

    def _post_command_summary(self, event: Event, label: str, summary: Dict[str, Any]) -> None:
        if not event.event_data.get("user"):
            return
        self.post_message(
            channel=event.event_data.get("channel"),
            title=self._command_summary_title(label, summary),
            userid=event.event_data.get("user"),
        )

    def _run_command_paths(
        self,
        paths,
        mon_path: str,
        event: Event,
        label: str,
        initial_failures: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        summary = self._new_command_summary()
        if self._scan_is_active():
            summary["total"] = 1
            summary["failed"] = 1
            summary["result_counts"]["failed"] = 1
            summary["errors"].append("全量处理正在执行，当前手动命令无法执行")
            self._post_command_summary(event, label + "（无法执行）", summary)
            return summary
        if not self._command_guard.acquire(blocking=False):
            summary["total"] = 1
            summary["failed"] = 1
            summary["result_counts"]["failed"] = 1
            summary["errors"].append("已有手动同步命令正在处理")
            self._post_command_summary(event, label + "（无法执行）", summary)
            return summary

        self._command_running = True
        self._command_cancel.clear()
        run_id = self._task_store.start_run("command", mon_path) if self._task_store else None
        started_at = time.time()
        self._update_command_progress(
            **{
                **self._new_command_progress(),
                "running": True,
                "run_id": run_id or "",
                "label": label,
                "monitor_root": mon_path,
                "phase": "discovering",
                "started_at": started_at,
                "last_progress_at": started_at,
            }
        )
        worker = threading.Thread(
            target=self._run_command_worker,
            args=(paths, mon_path, event, label, initial_failures, run_id),
            name="cloudstrmbutler-command",
            daemon=True,
        )
        self._command_thread = worker
        worker.start()
        return summary

    @eventmanager.register(EventType.PluginAction)
    def remote_sync_one(self, event: Event = None):
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "strm_one":
                return
            args = event_data.get("arg_str")
            if not args:
                logger.error(f"缺少参数：{event_data}")
                return
            all_args = args

            # 使用正则表达式匹配
            category = None
            args_arr = args.split(maxsplit=1)
            limit = None
            if len(args_arr) == 2:
                category = args_arr[0]
                args = args_arr[1]
                if str(args).isdigit():
                    limit = int(args)

            if category:
                # 判断是不是目录
                if Path(category).is_dir() and Path(category).exists() and limit is not None:
                    # 遍历所有监控目录
                    mon_path = find_monitor_path(category, self._category_conf)

                    # 指定路径
                    if not mon_path:
                        logger.error(f"未找到 {category} 对应的监控目录")
                        self.post_message(channel=event.event_data.get("channel"),
                                          title=f"未找到 {category} 对应的监控目录",
                                          userid=event.event_data.get("user"))
                        return

                    self.__handle_limit(path=category, mon_path=mon_path, limit=limit, event=event)
                    return
                else:
                    for mon_path in self._category_conf.keys():
                        mon_category = self._category_conf.get(mon_path)
                        logger.info(f"开始检查 {mon_path} {mon_category}")
                        mon_categories = [t.strip() for t in str(mon_category or "").split(",") if t.strip()]
                        if mon_category and str(category) in mon_categories + [mon_category]:
                            parent_path = os.path.join(mon_path, category)
                            if limit:
                                logger.info(f"获取到 {category} 对应的监控目录 {parent_path}")
                                self.__handle_limit(path=parent_path, mon_path=mon_path, limit=limit, event=event)
                            else:
                                logger.info(f"获取到 {category} {args} 对应的监控目录 {parent_path}")
                                target_path = os.path.join(str(parent_path), args)
                                logger.info(f"开始处理 {target_path}")
                                target_paths = self.__find_related_paths(os.path.join(str(parent_path), args))
                                if not target_paths:
                                    logger.error(f"未查找到 {category} {args} 对应的具体目录")
                                    self.post_message(channel=event.event_data.get("channel"),
                                                      title=f"未查找到 {category} {args} 对应的具体目录",
                                                      userid=event.event_data.get("user"))
                                    return
                                self._run_command_paths(target_paths[:1], mon_path, event, all_args)
                            return
            else:
                # 遍历所有监控目录
                mon_path = find_monitor_path(args, self._category_conf)

                # 指定路径
                if mon_path:
                    if not Path(args).exists():
                        logger.info(f"同步路径 {args} 不存在")
                        self._run_command_paths(
                            [],
                            mon_path,
                            event,
                            f"{all_args}（失败）",
                            initial_failures=[f"{args} 不存在"],
                        )
                        return
                    # 处理单文件
                    if Path(args).is_file():
                        self._run_command_paths([str(args)], mon_path, event, all_args)
                        return
                    else:
                        # 处理指定目录
                        logger.info(f"获取到 {args} 对应的监控目录 {mon_path}")

                        logger.info(f"开始定向处理文件夹 ...{args}")
                        self._run_command_paths([args], mon_path, event, all_args)
                        return
                else:
                    for mon_path in self._category_conf.keys():
                        mon_category = self._category_conf.get(mon_path)
                        logger.info(f"开始检查 {mon_path} {mon_category}")
                        mon_categories = [t.strip() for t in str(mon_category or "").split(",") if t.strip()]
                        if mon_category and str(args) in mon_categories + [mon_category]:
                            parent_path = os.path.join(mon_path, args)
                            logger.info(f"获取到 {args} 对应的监控目录 {parent_path}")
                            self._run_command_paths([parent_path], mon_path, event, all_args)
                            return
            if event.event_data.get("user"):
                self.post_message(channel=event.event_data.get("channel"),
                                  title=f"{all_args} 未检索到，请检查输入是否正确！",
                                  userid=event.event_data.get("user"))

    @staticmethod
    def __find_related_paths(base_path):
        related_paths = []
        base_dir = os.path.dirname(base_path)
        base_name = os.path.basename(base_path)

        for entry in os.listdir(base_dir):
            if entry.startswith(base_name):
                full_path = os.path.join(base_dir, entry)
                if os.path.isdir(full_path):
                    related_paths.append(full_path)

        # 按照修改时间倒序排列
        related_paths.sort(key=lambda path: os.path.getmtime(path), reverse=True)

        return related_paths

    def __handle_limit(self, path, limit, mon_path, event):
        """
        处理文件数量限制
        """
        sub_paths = []
        try:
            for entry in os.listdir(path):
                full_path = os.path.join(path, entry)
                if os.path.isdir(full_path):
                    sub_paths.append(full_path)
        except OSError as exc:
            logger.error(f"读取 {path} 目录失败：{exc}")
            self._run_command_paths(
                [],
                mon_path,
                event,
                f"{path}（读取失败）",
                initial_failures=[f"{path}: {exc}"],
            )
            return

        if not sub_paths:
            logger.error(f"未找到 {path} 目录下的文件夹")
            self._run_command_paths([], mon_path, event, f"{path}（最近 {limit} 个文件夹）")
            return

        # 按照修改时间倒序排列
        sub_paths.sort(key=lambda path: os.path.getmtime(path), reverse=True)
        logger.info(f"开始定向处理文件夹 ...{path}, 最新 {limit} 个文件夹")
        self._run_command_paths(
            sub_paths[:limit],
            mon_path,
            event,
            f"{path}（最近 {limit} 个文件夹）",
        )

    def send_msg(self):
        """Check queued media notifications and send stale entries."""
        with self._media_lock:
            snapshot = list(self._medias.items())
        for key, media_list in snapshot:
            if not media_list:
                continue
            last_update_time = media_list.get("time")
            if not last_update_time:
                continue
            mtype = media_list.get("type")
            if (datetime.now() - last_update_time).total_seconds() <= int(self._interval) \
                    and str(mtype) != "movie":
                continue
            try:
                if self._notify:
                    file_meta = media_list.get("file_meta")
                    episodes = media_list.get("episodes") or []
                    file_count = len(episodes) if episodes else 1
                    if str(mtype) == "tv":
                        season_episode = f"{key} {StringUtils.format_ep(episodes)}"
                        media_type = MediaType.TV
                    else:
                        season_episode = key
                        media_type = MediaType.MOVIE
                    mediainfo = self.chain.recognize_media(
                        meta=file_meta, mtype=media_type, tmdbid=file_meta.tmdbid
                    )
                    image = None
                    if mediainfo:
                        image = mediainfo.backdrop_path or mediainfo.poster_path
                    self.send_transfer_message(season_episode, file_count, image)
            except Exception as exc:
                logger.warning(f"发送媒体消息失败 {key}：{exc}")
            with self._media_lock:
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
        更新配置：结构化规则是唯一保存格式，旧版文本仅用于读取迁移。
        """
        payload = {
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cover": self._cover,
            "notify": self._notify,
            "monitor": self._monitor,
            "interval": self._interval,
            "scan_interval": self._scan_interval,
            "copy_files": self._copy_files,
            "copy_subtitles": self._copy_subtitles,
            "subtitle_formats": self._subtitle_formats,
            "refresh_emby": self._refresh_emby,
            "url": self._url,
            "config_version": 2,
            "rmt_mediaext": self._rmt_mediaext,
            "other_mediaext": self._other_mediaext,
            "mediaservers": self._mediaservers,
            "uriencode": self._uriencode,
            "emby_path": ",".join(serialize_mapping_line(source, target) for source, target in self._emby_paths.items()),
            "path_replacements": serialize_path_mappings(list(self._path_replacements.items())),
            "reliable_engine": self._reliable_engine,
            "cleanup_mode": self._cleanup_mode,
            "cleanup_probe": self._cleanup_probe,
        }
        # 写入结构化规则键供 Vue 组件读取
        for i, rule in enumerate(self._monitor_rules):
            payload[f"rule_{i}_category"] = rule.category or ""
            payload[f"rule_{i}_local"] = rule.local_dir
            payload[f"rule_{i}_strm"] = rule.strm_dir
            payload[f"rule_{i}_cloud"] = rule.cloud_dir
            payload[f"rule_{i}_format"] = rule.format_str
            payload[f"rule_{i}_monitor"] = rule.should_monitor(True)
            payload[f"rule_{i}_delete"] = False
        self.update_config(payload)

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """注册插件公共服务，当前无独立定时服务。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """注册插件 API，当前由 MoviePilot 原生命令和配置接口承担。"""
        return [
            {"path": "/sync_status", "endpoint": self.sync_status_api, "methods": ["GET"], "auth": "bear", "summary": "同步状态"},
            {"path": "/sync_failures", "endpoint": self.sync_failures_api, "methods": ["GET"], "auth": "bear", "summary": "同步失败记录"},
            {"path": "/sync_retry_failure", "endpoint": self.sync_retry_failure_api, "methods": ["POST"], "auth": "bear", "summary": "重试同步失败"},
            {"path": "/sync_retry_failures", "endpoint": self.sync_retry_failures_api, "methods": ["POST"], "auth": "bear", "summary": "批量重试同步失败"},
            {"path": "/sync_confirm_cleanup", "endpoint": self.sync_confirm_cleanup_api, "methods": ["POST"], "auth": "bear", "summary": "确认 STRM 清理"},
            {"path": "/sync_full_scan", "endpoint": self.sync_full_scan_api, "methods": ["POST"], "auth": "bear", "summary": "执行一次全量处理"},
            {"path": "/sync_cancel", "endpoint": self.sync_cancel_api, "methods": ["POST"], "auth": "bear", "summary": "取消当前同步任务"},
        ]

    def sync_status_api(self) -> Dict[str, Any]:
        status = self._task_store.status() if self._task_store else {"queued": 0, "running": [], "recent_runs": [], "cleanup_batches": []}
        engine = self._sync_engine.snapshot() if self._sync_engine else {"memory_queued": 0, "inflight": 0, "scheduled": 0, "workers": 0}
        pending_jobs = int(status.get("queued") or 0)
        queue_active = bool(self._sync_engine and int(engine.get("workers") or 0) > 0)
        monitor_active = bool(self._observer)
        active_queued = pending_jobs if queue_active else 0
        orphaned_queued = max(0, pending_jobs - active_queued)
        scan_progress = self._scan_progress_snapshot()
        scan_running = bool(self._scan_running or scan_progress.get("running"))
        service_running = bool(
            self._command_running
            or scan_running
            or monitor_active
            or queue_active
        )
        if self._command_running:
            service_state = "command_running"
        elif scan_running:
            service_state = "scan_running"
        elif orphaned_queued:
            service_state = "pending_recovery"
        elif queue_active and (int(engine.get("scheduled") or 0) or pending_jobs):
            service_state = "queue_running"
        elif monitor_active:
            service_state = "monitoring_idle"
        elif queue_active:
            service_state = "engine_idle"
        elif service_running:
            service_state = "running"
        elif self._enabled:
            service_state = "enabled_idle"
        else:
            service_state = "disabled"
        service_busy = bool(
            self._command_running
            or scan_running
            or int(engine.get("inflight") or 0) > 0
            or service_state == "queue_running"
        )
        status.update({
            "enabled": self._enabled,
            "reliable_engine": self._reliable_engine,
            "scan_running": scan_running,
            "command_running": self._command_running,
            "monitor_active": monitor_active,
            "queue_active": queue_active,
            "service_running": service_running,
            "service_busy": service_busy,
            "service_state": service_state,
            "pending_jobs": pending_jobs,
            "orphaned_queued": orphaned_queued,
            "active_queued": active_queued,
            "engine": engine,
            "command_progress": self._command_progress_snapshot(),
            "scan_progress": scan_progress,
            "processing_overview": self._processing_overview(),
        })
        return {"code": 0, "data": status}

    def sync_full_scan_api(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Start one idempotent full scan without blocking the API request."""
        raw_payload = payload if isinstance(payload, dict) else {}
        options = {
            "copy_files": _as_bool(raw_payload.get("copy_files"), self._copy_files),
            "copy_subtitles": _as_bool(raw_payload.get("copy_subtitles"), self._copy_subtitles),
        }
        with self._scan_launch_lock:
            if self._command_running or self._scan_is_active():
                return {"code": 1, "msg": "已有扫描正在执行，请等待当前任务完成"}
            worker = threading.Thread(
                target=self.scan,
                args=("manual_full", options),
                name="cloudstrmbutler-full-scan",
                daemon=True,
            )
            self._scan_thread = worker
            worker.start()
        return {"code": 0, "data": {"kind": "manual_full", "status": "started", **options}, "msg": "全量处理已开始"}

    def sync_cancel_api(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Request cancellation for the active full scan or /strm command."""
        del payload
        if self._scan_running or self._scan_progress_snapshot().get("running"):
            self._scan_cancel.set()
            run_id = self._scan_progress_snapshot().get("run_id") or ""
            self._mark_run_cancelled(run_id)
            return {"code": 0, "data": {"kind": "manual_full", "status": "cancelling", "run_id": run_id}, "msg": "已请求取消全量处理"}
        if self._command_running or self._command_progress_snapshot().get("running"):
            self._command_cancel.set()
            run_id = self._command_progress_snapshot().get("run_id") or ""
            self._mark_run_cancelled(run_id)
            return {"code": 0, "data": {"kind": "command", "status": "cancelling", "run_id": run_id}, "msg": "已请求取消定向处理"}
        return {"code": 1, "msg": "当前没有正在执行的任务"}

    def sync_failures_api(self, limit: int = 100) -> Dict[str, Any]:
        failures = self._task_store.failures(limit=limit) if self._task_store else []
        return {"code": 0, "data": {"items": failures, "total": len(failures)}}

    def sync_retry_failure_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raw_id = (payload or {}).get("failure_id") if isinstance(payload, dict) else None
        try:
            failure_id = int(raw_id or 0)
        except (TypeError, ValueError):
            return {"code": 1, "msg": "失败任务 ID 无效"}
        if failure_id <= 0:
            return {"code": 1, "msg": "失败任务 ID 无效"}
        if not self._task_store or not self._sync_engine or not self._task_store.retry_failure(failure_id):
            return {"code": 1, "msg": "未找到可重试的失败任务"}
        self._sync_engine.pump()
        return {"code": 0, "msg": "已重新加入同步队列"}

    def sync_retry_failures_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Requeue a selected set of open failures in one request."""
        raw_ids = (payload or {}).get("failure_ids")
        if not isinstance(raw_ids, list):
            raw_ids = [raw_ids] if raw_ids is not None else []
        if not self._task_store or not self._sync_engine:
            return {"code": 1, "msg": "可靠同步引擎未启用，无法重试失败任务"}
        retried_ids = self._task_store.retry_failures(raw_ids)
        if not retried_ids:
            return {"code": 1, "msg": "未找到可重试的失败任务"}
        self._sync_engine.pump()
        return {
            "code": 0,
            "data": {"retried_ids": retried_ids, "retried": len(retried_ids)},
            "msg": f"已将 {len(retried_ids)} 个失败任务重新加入同步队列",
        }

    def sync_confirm_cleanup_api(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        batch_id = str((payload or {}).get("batch_id") or "")
        items = self._task_store.claim_cleanup_batch(batch_id) if self._task_store else None
        if items is None:
            return {"code": 1, "msg": "未找到待确认清理批次"}
        removed = 0
        failures = []
        completed_items = []
        for item in items:
            if isinstance(item, dict):
                root = str(item.get("monitor_root") or "")
                source_rel = str(item.get("source_rel") or "")
                expected_outputs = item.get("outputs") or []
                if not root or not source_rel or Path(root, source_rel).exists():
                    continue
                record = self._state_store.get(root, source_rel) if self._state_store else None
                if not record:
                    continue
                if {path_key(output) for output in record.outputs} != {path_key(output) for output in expected_outputs}:
                    failures.append({"path": source_rel, "error": "状态记录已变化，跳过本次清理"})
                    continue
                owned_outputs = [
                    output
                    for output in record.outputs
                    if not self._state_store.has_output_for_other_source(root, output, source_rel)
                ]
                cleanup = self._remove_outputs(owned_outputs, notify_emby=bool(record.content_hash))
                failures.extend(cleanup.get("failed") or [])
                if not cleanup.get("failed"):
                    completed_items.append((root, source_rel, tuple(record.outputs)))
                removed += len(cleanup.get("removed") or [])
                continue

            # Support batches created by older plugin versions.
            wanted = path_key(item)
            for root in self._strm_dir_conf:
                if not self._state_store:
                    continue
                for candidate in self._state_store.records_for_root(root):
                    if wanted not in {path_key(output) for output in candidate.outputs}:
                        continue
                    if Path(root, candidate.source_rel).exists():
                        continue
                    record = self._state_store.get(root, candidate.source_rel)
                    if not record:
                        continue
                    owned_outputs = [
                        output
                        for output in record.outputs
                        if not self._state_store.has_output_for_other_source(root, output, candidate.source_rel)
                    ]
                    cleanup = self._remove_outputs(owned_outputs, notify_emby=bool(record.content_hash))
                    failures.extend(cleanup.get("failed") or [])
                    if not cleanup.get("failed"):
                        completed_items.append((root, candidate.source_rel, tuple(record.outputs)))
                    removed += len(cleanup.get("removed") or [])
        if not failures and self._state_store:
            for root, source_rel, expected_outputs in completed_items:
                if not self._state_store.delete_if_matches(root, source_rel, expected_outputs):
                    failures.append({"path": source_rel, "error": "状态记录已变化，未移除 ownership"})
        if failures:
            if self._task_store:
                self._task_store.release_cleanup_batch(batch_id)
            return {
                "code": 1,
                "data": {"removed": removed, "failed": failures},
                "msg": f"清理完成 {removed} 个文件，{len(failures)} 个文件删除失败",
            }
        if self._task_store:
            self._task_store.confirm_cleanup_batch(batch_id)
        return {"code": 0, "data": {"removed": removed, "failed": []}, "msg": f"已清理 {removed} 个生成文件"}

    @staticmethod
    def get_render_mode() -> Tuple[str, str]:
        """声明插件使用 Vue 联邦组件渲染配置页面。"""
        return "vue", "dist/assets"

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        :return: 命令关键字、事件、描述、附带数据
        """
        return [
            {
                "cmd": "/cloud_strm_butler",
                "event": EventType.PluginAction,
                "desc": "云盘Strm助手同步",
                "category": "",
                "data": {
                    "action": "CloudStrmButler"
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

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """Vue 模式：返回空表单和完整配置模型。"""
        saved = self.get_config() or {}
        rules = self._parse_structured_rules(saved)

        def saved_value(key: str, fallback: Any) -> Any:
            return saved[key] if key in saved else fallback

        def saved_bool(key: str, fallback: bool) -> bool:
            return _as_bool(saved_value(key, fallback), fallback)

        def saved_int(key: str, fallback: int) -> int:
            try:
                return max(0, int(saved_value(key, fallback)))
            except (TypeError, ValueError):
                return fallback

        saved_mediaservers = saved_value("mediaservers", self._mediaservers)
        if isinstance(saved_mediaservers, (list, tuple)):
            mediaservers = list(saved_mediaservers)
        elif saved_mediaservers:
            mediaservers = [saved_mediaservers]
        else:
            mediaservers = []

        model = {
            "enabled": saved_bool("enabled", self._enabled),
            "monitor": saved_bool("monitor", self._monitor),
            "cover": saved_bool("cover", self._cover),
            "notify": saved_bool("notify", self._notify),
            "copy_files": saved_bool("copy_files", self._copy_files),
            "copy_subtitles": saved_bool("copy_subtitles", self._copy_subtitles),
            "refresh_emby": saved_bool("refresh_emby", self._refresh_emby),
            "uriencode": saved_bool("uriencode", self._uriencode),
            "onlyonce": saved_bool("onlyonce", self._onlyonce),
            "interval": saved_int("interval", self._interval),
            "scan_interval": saved_int("scan_interval", self._scan_interval),
            "url": str(saved_value("url", self._url) or ""),
            "rmt_mediaext": str(saved_value("rmt_mediaext", self._rmt_mediaext) or ""),
            "other_mediaext": str(saved_value("other_mediaext", self._other_mediaext) or ""),
            "subtitle_formats": str(saved_value("subtitle_formats", self._subtitle_formats) or ""),
            "emby_path": str(saved_value("emby_path", self._emby_path_serialized()) or ""),
            "path_replacements": str(saved_value("path_replacements", self._path_replacements_serialized()) or ""),
            "reliable_engine": saved_bool("reliable_engine", self._reliable_engine),
            "cleanup_mode": str(saved_value("cleanup_mode", self._cleanup_mode) or "off").lower(),
            "cleanup_probe": str(saved_value("cleanup_probe", self._cleanup_probe) or ""),
            "mediaservers": mediaservers,
            "config_version": saved_value("config_version", 2),
        }
        for i, rule in enumerate(rules):
            model[f"rule_{i}_category"] = rule.get("category", "")
            model[f"rule_{i}_local"] = rule.get("local", "")
            model[f"rule_{i}_strm"] = rule.get("strm", "")
            model[f"rule_{i}_cloud"] = rule.get("cloud", "")
            model[f"rule_{i}_format"] = rule.get("format", "")
            model[f"rule_{i}_monitor"] = rule.get("monitor", True)
            model[f"rule_{i}_delete"] = False
        return [], model

    def _parse_structured_rules(self, config: dict) -> list:
        """从结构化 rule_N_* 键或旧 monitor_confs 解析规则列表。"""
        rules = []
        structured_slots = 0
        for key in (config or {}).keys():
            if re.match(r"^rule_(\d+)_", str(key)):
                try:
                    idx = int(str(key).split("_")[1])
                    structured_slots = max(structured_slots, idx + 1)
                except (ValueError, IndexError):
                    pass

        if structured_slots > 0:
            for i in range(structured_slots):
                delete_val = str(config.get(f"rule_{i}_delete") or "").strip().lower()
                if _as_bool(delete_val):
                    continue
                local = str(config.get(f"rule_{i}_local") or "").strip()
                strm = str(config.get(f"rule_{i}_strm") or "").strip()
                if not local and not strm:
                    continue
                rules.append({
                    "category": str(config.get(f"rule_{i}_category") or ""),
                    "local": local,
                    "strm": strm,
                    "cloud": str(config.get(f"rule_{i}_cloud") or "").strip(),
                    "format": str(config.get(f"rule_{i}_format") or "").strip(),
                    "monitor": _as_bool(config.get(f"rule_{i}_monitor"), True),
                })
            return rules

        # Fallback to legacy monitor_confs
        for raw_line in str(config.get("monitor_confs") or "").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            monitor_flag = None
            if line.count("$") == 1:
                line, monitor_flag = line.split("$", 1)
                monitor_flag = monitor_flag.strip().lower()
            category = None
            if line.count("@") == 1:
                line, category = line.split("@", 1)
                category = category.strip()
            if line.count("#") < 3:
                continue
            local_dir, strm_dir, cloud_dir, format_str = [p.strip() for p in line.split("#", 3)]
            if not local_dir or not strm_dir:
                continue
            rules.append({
                "category": category or "",
                "local": local_dir,
                "strm": strm_dir,
                "cloud": cloud_dir,
                "format": format_str,
                "monitor": monitor_flag not in ("0", "nomonitor", "false", "off"),
            })
        return rules

    @staticmethod
    def _has_structured_config(config: dict) -> bool:
        return any(re.match(r"^rule_(\d+)_", str(key)) for key in (config or {}))

    @staticmethod
    def _rules_to_monitor_confs(rules: list) -> str:
        """把规则列表序列化为旧版 monitor_confs 文本。"""
        lines = []
        for rule in rules or []:
            local = str(rule.get("local") or "").strip()
            strm = str(rule.get("strm") or "").strip()
            if not local or not strm:
                continue
            category_str = str(rule.get("category") or "").strip()
            cloud = str(rule.get("cloud") or "").strip()
            fmt = str(rule.get("format") or "").strip()
            line = f"{local}#{strm}#{cloud}#{fmt}"
            if category_str:
                line += f"@{category_str}"
            if not rule.get("monitor", True):
                line += "$0"
            lines.append(line)
        return "\n".join(lines)

    def _emby_path_serialized(self) -> str:
        return ",".join(f"{s}=>{t}" for s, t in self._emby_paths.items())

    def _path_replacements_serialized(self) -> str:
        return "\n".join(f"{s}=>{t}" for s, t in self._path_replacements.items())

    def get_page(self) -> List[dict]:
        """Vue 模式：详情页由远程 Page 组件渲染。"""
        return []

    def stop_service(self) -> bool:
        """
        退出插件
        """
        observers = list(self._observer)
        self._observer = []
        # Stop all polling observers before waiting so MoviePilot config reload
        # is not delayed once per monitored directory.
        for observer in observers:
            try:
                observer.stop()
            except Exception as e:
                logger.warning(f"停止目录监控失败：{e}")
        observer_deadline = time.monotonic() + 2
        for observer in observers:
            try:
                observer.join(timeout=max(0, observer_deadline - time.monotonic()))
            except Exception as e:
                logger.warning(f"等待目录监控退出失败：{e}")
        if self._scheduler:
            self._scheduler.remove_all_jobs()
            if self._scheduler.running:
                self._event.set()
                self._scheduler.shutdown()
                self._event.clear()
            self._scheduler = None
        scan_thread = self._scan_thread
        if scan_thread and scan_thread.is_alive():
            scan_thread.join(timeout=max(0, float(self._command_shutdown_timeout)))
        if scan_thread and scan_thread.is_alive():
            logger.warning(
                "全量处理仍在运行，保留状态数据库等待任务完成：%s",
                self._scan_progress_snapshot().get("current_path") or "未知文件",
            )
            return False
        scan_progress = self._scan_progress_snapshot()
        if scan_progress.get("running"):
            logger.warning(
                "全量处理仍有可靠队列任务未结算，保留状态数据库等待任务完成：%s",
                scan_progress.get("current_path") or "未知文件",
            )
            return False
        self._scan_thread = None
        if self._sync_engine:
            if self._sync_engine.stop() is False:
                logger.warning("可靠同步队列仍有任务运行，暂不关闭状态数据库")
                return False
            self._sync_engine = None
        command_thread = self._command_thread
        if command_thread and command_thread.is_alive():
            command_thread.join(timeout=max(0, float(self._command_shutdown_timeout)))
        if command_thread and command_thread.is_alive():
            logger.warning("手动 /strm 扫描仍在运行，保留状态数据库等待任务完成：%s", self._command_progress_snapshot().get("current_path") or "未知文件")
            return False
        self._command_thread = None
        if self._state_store:
            self._state_store.close()
            self._state_store = None
        if self._task_store:
            self._task_store.close()
            self._task_store = None
        return True
