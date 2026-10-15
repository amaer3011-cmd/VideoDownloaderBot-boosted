import contextlib
import datetime
import logging
import os
import queue
import shutil
import signal
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlsplit

import telebot
import yt_dlp
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor
from telebot import types
from telebot.util import quick_markup

from app.download_guard import (
    DownloadByteLimiter,
    DownloadStorageError,
    is_download_size_error,
    is_enospc_error,
    raise_if_terminal_storage_error,
)
from app.download_utils import (
    calc_download_progress as _calc_download_progress,
)
from app.download_utils import (
    find_downloaded_file as _find_downloaded_file_impl,
)
from app.download_utils import (
    find_file_by_prefix as _find_file_by_prefix_impl,
)
from app.download_utils import (
    render_status as _render_status,
)
from app.healthcheck import HEALTH_MARKER
from app.http_utils import telegram_upload_session
from app.logging_setup import configure_logging
from app.media_extras import build_caption, make_thumbnail, probe_video_size
from app.media_validation import validate_media_file
from app.models import ActiveJob, DownloadJob, PendingRequest, VideoFormatCandidate
from app.planner import (
    apply_generic_impersonation_opts as _apply_generic_impersonation_opts,
)
from app.planner import (
    apply_instagram_stability_opts as _apply_instagram_stability_opts,
)
from app.planner import (
    apply_youtube_runtime_opts as _apply_youtube_runtime_opts,
)
from app.planner import audio_languages as _audio_languages
from app.planner import PLAYLIST_LIST_LIMIT
from app.planner import set_pot_base_url as _set_pot_base_url
from app.planner import (
    build_audio_plan_mp3 as _build_audio_plan_mp3,
)
from app.planner import build_audio_plans_mp3 as _build_audio_plans_mp3
from app.planner import build_video_candidates as _build_video_candidates
from app.planner import (
    get_video_meta as _get_video_meta,
)
from app.planner import has_downloadable_video as _has_downloadable_video
from app.planner import (
    is_instagram_url as _is_instagram_url,
)
from app.planner import is_youtube_url as _is_youtube_url
from app.planner import metadata_has_original_audio as _metadata_has_original_audio
from app.planner import metadata_without_format_selection as _metadata_without_format_selection
from app.planner import original_audio_language as _original_audio_language
from app.planner import original_audio_language_candidates as _original_audio_language_candidates
from app.planner import remember_original_audio_language as _remember_original_audio_language
from app.planner import same_language as _same_language
from app.planner import youtube_player_clients as _youtube_player_clients
from app.settings import Settings, load_settings
from app.storage_guard import (
    ensure_workspace_capacity,
    required_workspace_bytes,
    validate_workspace_location,
)
from app.temp_files import cleanup_directory_contents, cleanup_job_directory, cleanup_stale_directories
from app.text_utils import (
    extract_first_url as _extract_first_url,
)
from app.text_utils import (
    fmt_bytes as _fmt_bytes,
)
from app.text_utils import (
    sanitize_filename_base as _sanitize_filename_base,
)
from app.text_utils import (
    strip_hashtags as _strip_hashtags,
)
from app.text_utils import (
    youtube_url_validation,
)
from app.url_security import UnsafeUrlError, safe_error_for_log, safe_url_for_log, validate_public_url

# =========================
# Telegram bot init
# =========================
bot: telebot.TeleBot | None = None
SETTINGS: Settings | None = None
LOGGER = logging.getLogger("video_downloader_bot")

# Edit throttling (avoid Telegram flood limits)
EDIT_INTERVAL_SEC = 1.8

# Parallel jobs: how many downloads/uploads can run simultaneously
WORKERS = 2

# TTL for pending "choice" requests to avoid memory leaks
PENDING_TTL_SEC = 10 * 60

# Telegram Bot API upload limit (you keep it in config)
MAX_SEND_BYTES = 50 * 1024 * 1024

# yt-dlp optimization for segmented streams (HLS/DASH)
YTDLP_CONCURRENT_FRAGMENTS = 4


# =========================
# Global state
# =========================
bot_lock = threading.RLock()
state_lock = threading.RLock()
stop_event = threading.Event()

last_edited: dict[str, datetime.datetime] = {}
last_text: dict[str, str] = {}

pending_requests: dict[str, PendingRequest] = {}
jobs_q: "queue.Queue[DownloadJob | None]" = queue.Queue(maxsize=200)

# Cancel support
cancel_events: dict[str, threading.Event] = {}
active_jobs: dict[str, ActiveJob] = {}
worker_threads: list[threading.Thread] = []
maintenance_thread: threading.Thread | None = None
upload_slots = threading.BoundedSemaphore(2)
metadata_slots = threading.BoundedSemaphore(2)
metadata_executor: ThreadPoolExecutor | None = None
maintenance_finished = threading.Event()
fatal_lifecycle_error = threading.Event()
worker_failure_alerted = threading.Event()
# Healthy also requires Telegram to have answered getUpdates recently, so a
# revoked token, a network outage, or a second instance polling the same token
# (HTTP 409) reports the container unhealthy and a fresh release rolls back.
POLL_STALE_SECONDS = 90
last_successful_poll = 0.0
poll_stale = threading.Event()


class JobCancelled(RuntimeError):
    pass


class JobTimedOut(RuntimeError):
    pass


def _settings() -> Settings:
    if SETTINGS is None:
        raise RuntimeError("application is not initialized")
    return SETTINGS


def _output_folder() -> Path:
    return _settings().output_dir


def _check_job(job_id: str, deadline: float) -> None:
    if _is_cancelled(job_id):
        raise JobCancelled("cancelled")
    if time.monotonic() >= deadline:
        raise JobTimedOut("job deadline exceeded")


# =========================
# Helpers (safe bot calls)
# =========================
def _bot_call(fn, *args, **kwargs):
    with bot_lock:
        return fn(*args, **kwargs)


def _safe_delete(chat_id: int, message_id: int) -> None:
    try:
        assert bot is not None
        _bot_call(bot.delete_message, chat_id, message_id)
    except Exception:
        LOGGER.debug("telegram delete failed chat_id=%s message_id=%s", chat_id, message_id, exc_info=True)


def _safe_edit(chat_id: int, message_id: int, text: str, reply_markup=None, force: bool = False) -> None:
    key = f"{chat_id}-{message_id}"
    now = datetime.datetime.now()

    if not force:
        with state_lock:
            last = last_edited.get(key)
            if last is not None and (now - last).total_seconds() < EDIT_INTERVAL_SEC:
                return
            if last_text.get(key) == text:
                return

    try:
        assert bot is not None
        _bot_call(
            bot.edit_message_text,
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
        with state_lock:
            last_edited[key] = now
            last_text[key] = text
    except Exception:
        LOGGER.debug("telegram edit failed chat_id=%s message_id=%s", chat_id, message_id, exc_info=True)


def _safe_send_message(chat_id: int, text: str, reply_to_message_id: int | None = None, reply_markup=None):
    try:
        assert bot is not None
        return _bot_call(
            bot.send_message,
            chat_id,
            text,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    except Exception:
        LOGGER.warning("telegram send message failed chat_id=%s", chat_id, exc_info=True)
        return None


def _safe_answer_callback(call_id: str, text: str = "") -> None:
    try:
        assert bot is not None
        _bot_call(bot.answer_callback_query, call_id, text=text)
    except Exception:
        LOGGER.debug("telegram callback answer failed", exc_info=True)


def _notify_operator_critical(text: str) -> None:
    if SETTINGS is None or SETTINGS.logs_chat_id is None or bot is None:
        return
    try:
        _bot_call(bot.send_message, SETTINGS.logs_chat_id, text, disable_web_page_preview=True)
    except Exception:
        LOGGER.exception("operator critical notification failed")


# =========================
# Cancel UI
# =========================
def _cancel_markup(job_id: str) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton("Cancel", callback_data=f"cnl|{job_id}"))
    return kb


def _is_cancelled(job_id: str) -> bool:
    with state_lock:
        ev = cancel_events.get(job_id)
    return bool(ev and ev.is_set())


# =========================
# Upload via Bot API with progress + cancel
# =========================
def _send_via_bot_api_with_progress(
    job_id: str,
    chat_id: int,
    reply_to_message_id: int,
    status_message_id: int,
    title: str,
    method_name: str,
    file_field_name: str,
    file_path: str,
    send_filename: str,
    stage_label: str,
    extra_params: dict[str, Any],
    deadline: float,
    thumb_path: str | None = None,
) -> None:
    local_api = _settings().telegram_api_url
    base_url = local_api or "https://api.telegram.org"
    api_url = f"{base_url}/bot{_settings().token}/{method_name}"
    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

    def render_upload(pct: int | None, sent: int | None, total_len: int | None) -> str:
        line = f"الحالة: ⬆️ {stage_label}"
        if pct is not None:
            pct = max(0, min(100, int(pct)))
            line += f" {pct}%"
        if isinstance(sent, int) and isinstance(total_len, int) and total_len > 0:
            line += f"\n{_fmt_bytes(sent)} / {_fmt_bytes(total_len)}"
        return f"{title}\n\n{line}"

    _safe_edit(
        chat_id, status_message_id, render_upload(0, 0, file_size), reply_markup=_cancel_markup(job_id), force=True
    )

    _check_job(job_id, deadline)

    remaining = max(0.0, deadline - time.monotonic())
    if not upload_slots.acquire(timeout=remaining):
        raise JobTimedOut("upload slot deadline exceeded")

    try:
        if local_api:
            # The API server reads the file from the shared volume itself and uploads it
            # to Telegram over MTProto, so no bytes pass through this HTTP request.
            _check_job(job_id, deadline)
            _safe_edit(
                chat_id, status_message_id,
                f"{title}\n\nالحالة: ⬆️ {stage_label}",
                reply_markup=_cancel_markup(job_id), force=True,
            )
            # Path uploads carry no separate filename, so give the file the wanted name.
            upload_path = file_path
            try:
                renamed = os.path.join(os.path.dirname(file_path), send_filename)
                if renamed != file_path:
                    os.replace(file_path, renamed)
                    upload_path = renamed
            except OSError:
                LOGGER.debug("could not rename file for upload", exc_info=True)
            session = telegram_upload_session()
            try:
                remaining = max(1.0, deadline - time.monotonic())
                resp = session.post(
                    api_url,
                    data={
                        "chat_id": str(chat_id),
                        "reply_to_message_id": str(reply_to_message_id),
                        **{k: str(v) for k, v in extra_params.items() if v is not None},
                        file_field_name: "file://" + os.path.abspath(upload_path),
                        **({"thumbnail": "file://" + os.path.abspath(thumb_path)} if thumb_path else {}),
                    },
                    timeout=(20, min(60 * 60, remaining)),
                )
            finally:
                session.close()
            _parse_upload_response(resp)
            return
        with open(file_path, "rb") as f, contextlib.ExitStack() as stack:
            fields = {
                "chat_id": str(chat_id),
                "reply_to_message_id": str(reply_to_message_id),
                **{k: str(v) for k, v in extra_params.items() if v is not None},
                file_field_name: (send_filename, f),
            }
            if thumb_path:
                fields["thumbnail"] = ("thumb.jpg", stack.enter_context(open(thumb_path, "rb")), "image/jpeg")

            encoder = MultipartEncoder(fields=fields)

            def _cb(monitor: MultipartEncoderMonitor):
                _check_job(job_id, deadline)

                total_len = monitor.len
                sent = monitor.bytes_read
                pct = int((sent * 100) / total_len) if total_len else None
                _safe_edit(
                    chat_id,
                    status_message_id,
                    render_upload(pct, sent, total_len),
                    reply_markup=_cancel_markup(job_id),
                )

            monitor = MultipartEncoderMonitor(encoder, _cb)

            session = telegram_upload_session()
            try:
                remaining = max(1.0, deadline - time.monotonic())
                resp = session.post(
                    api_url,
                    data=monitor,
                    headers={"Content-Type": monitor.content_type},
                    timeout=(20, min(60 * 30, remaining)),
                )
            finally:
                session.close()
    finally:
        upload_slots.release()

    _parse_upload_response(resp)

    _safe_edit(
        chat_id,
        status_message_id,
        render_upload(100, file_size, file_size),
        reply_markup=_cancel_markup(job_id),
        force=True,
    )


def _video_send_params(file_path: str, title: str, duration: int | None) -> dict[str, Any]:
    params: dict[str, Any] = {"supports_streaming": "true", "caption": build_caption(title, duration)}
    if duration:
        params["duration"] = duration
    size = probe_video_size(file_path)
    if size:
        params["width"], params["height"] = size
    return params


def _parse_upload_response(response: Any) -> dict[str, Any]:
    try:
        data = response.json()
    except Exception as exc:
        raise RuntimeError(f"Telegram API returned HTTP {response.status_code}") from exc
    if not isinstance(data, dict) or not data.get("ok"):
        description = data.get("description", "request failed") if isinstance(data, dict) else "request failed"
        raise RuntimeError(f"Telegram API error: {description}")
    return data


# =========================
# Downloaded file discovery
# =========================
def _find_file_by_prefix(output_folder: str, prefix: str, prefer_ext: str | None = None) -> str | None:
    return _find_file_by_prefix_impl(output_folder, prefix, prefer_ext=prefer_ext)


def _find_downloaded_file(
    info: dict[str, Any], output_folder: str, fallback_prefix: str, prefer_ext: str | None = None
) -> str | None:
    return _find_downloaded_file_impl(info, output_folder, fallback_prefix, prefer_ext=prefer_ext)


def _get_youtube_meta_for_client(
    url: str,
    player_client: str | None,
    preferred_language: str | None,
) -> tuple[dict[str, Any], str | None, bool]:
    settings = _settings()

    def extract(language: str | None) -> dict[str, Any]:
        metadata = _get_video_meta(
            url,
            js_runtimes=settings.ytdlp_js_runtimes,
            remote_components=settings.ytdlp_remote_components,
            cookies_file=str(settings.cookies_file) if settings.cookies_file else None,
            youtube_player_client=player_client,
            youtube_language=language,
            generic_impersonate=None,
        )
        if not isinstance(metadata, dict) or not _has_downloadable_video(metadata):
            raise RuntimeError("extractor returned metadata without a downloadable video")
        _validate_metadata_urls(metadata)
        return metadata

    requested_language = preferred_language
    candidate = extract(requested_language)
    discovered_language = preferred_language
    if not discovered_language:
        language_candidates = _original_audio_language_candidates(candidate)
        if len(language_candidates) == 1:
            discovered_language = language_candidates[0]
        elif language_candidates:
            # A player response can mark both the source and a regional
            # automatic dub as `*-orig`. Re-query the bounded candidate set;
            # the true source marker remains common while a dub is localized.
            common_languages = set(language_candidates)
            probes: list[tuple[str, dict[str, Any], set[str]]] = []
            for language in language_candidates[:4]:
                try:
                    localized = extract(language)
                except Exception as error:
                    LOGGER.info(
                        "YouTube audio-language probe failed client=%s language=%s error=%s detail=%s",
                        player_client or "default",
                        language,
                        type(error).__name__,
                        safe_error_for_log(error),
                    )
                    continue
                localized_languages = set(_original_audio_language_candidates(localized))
                probes.append((language, localized, localized_languages))
                if localized_languages:
                    common_languages.intersection_update(localized_languages)

            if len(common_languages) == 1:
                discovered_language = next(iter(common_languages))
                matching_probe = next(
                    (probe for language, probe, _languages in probes if _same_language(language, discovered_language)),
                    None,
                )
                if matching_probe is None:
                    try:
                        matching_probe = extract(discovered_language)
                    except Exception as error:
                        LOGGER.info(
                            "YouTube resolved-language retry failed client=%s language=%s error=%s detail=%s",
                            player_client or "default",
                            discovered_language,
                            type(error).__name__,
                            safe_error_for_log(error),
                        )
                if matching_probe is not None:
                    candidate = matching_probe
                    requested_language = discovered_language
            elif probes:

                def probe_score(item: tuple[str, dict[str, Any], set[str]]) -> tuple[int, int, int, int]:
                    language, probe, probe_languages = item
                    available = _audio_languages(probe)
                    return (
                        int(
                            len(probe_languages) == 1
                            and any(_same_language(value, language) for value in probe_languages)
                        ),
                        int(any(_same_language(value, language) for value in available)),
                        int(any(_same_language(value, language) for value in probe_languages)),
                        -len(probe_languages),
                    )

                discovered_language, candidate, _probe_languages = max(probes, key=probe_score)
                requested_language = discovered_language
            else:
                discovered_language = language_candidates[0]

    has_original = _metadata_has_original_audio(candidate, discovered_language, requested_language)

    if discovered_language and not has_original and not _same_language(requested_language, discovered_language):
        try:
            localized = extract(discovered_language)
        except Exception as error:
            LOGGER.info(
                "YouTube original-language metadata retry failed client=%s language=%s error=%s detail=%s",
                player_client or "default",
                discovered_language,
                type(error).__name__,
                safe_error_for_log(error),
            )
        else:
            candidate = localized
            has_original = _metadata_has_original_audio(candidate, discovered_language, discovered_language)

    _remember_original_audio_language(candidate, discovered_language)
    return candidate, discovered_language, has_original


def _get_video_meta_with_hidden_retries(url: str) -> dict[str, Any]:
    settings = _settings()
    if _is_youtube_url(url):
        last_error: Exception | None = None
        fallback_meta: dict[str, Any] | None = None
        preferred_language: str | None = None
        for player_client in _youtube_player_clients(settings.ytdlp_youtube_player_clients):
            try:
                meta, discovered_language, has_original = _get_youtube_meta_for_client(
                    url,
                    player_client,
                    preferred_language,
                )
                preferred_language = discovered_language or preferred_language
                if discovered_language and not has_original:
                    if fallback_meta is None:
                        fallback_meta = meta
                    LOGGER.info(
                        "YouTube client omitted original audio client=%s language=%s available=%s",
                        player_client or "default",
                        discovered_language,
                        ",".join(sorted(_audio_languages(meta))) or "unknown",
                    )
                    continue
                return meta
            except Exception as exc:
                last_error = exc
                LOGGER.info(
                    "YouTube metadata client failed client=%s",
                    player_client or "default",
                    exc_info=True,
                )
        if fallback_meta is not None:
            return fallback_meta
        raise RuntimeError("all YouTube metadata clients failed") from last_error

    try:
        meta = _get_video_meta(
            url,
            js_runtimes=settings.ytdlp_js_runtimes,
            remote_components=settings.ytdlp_remote_components,
            instagram_impersonate=settings.ytdlp_instagram_impersonate,
            instagram_retries=settings.ytdlp_instagram_retries,
            instagram_fragment_retries=settings.ytdlp_instagram_fragment_retries,
            instagram_socket_timeout=settings.ytdlp_instagram_socket_timeout,
            cookies_file=str(settings.cookies_file) if settings.cookies_file else None,
            generic_impersonate=None,
        )
        _validate_metadata_urls(meta)
        return meta
    except Exception as primary_error:
        if not _is_instagram_url(url):
            if not settings.ytdlp_generic_impersonate:
                raise
            LOGGER.info(
                "generic metadata retry with browser impersonation error=%s detail=%s",
                type(primary_error).__name__,
                safe_error_for_log(primary_error),
            )
            meta = _get_video_meta(
                url,
                js_runtimes=settings.ytdlp_js_runtimes,
                remote_components=settings.ytdlp_remote_components,
                cookies_file=str(settings.cookies_file) if settings.cookies_file else None,
                generic_impersonate=settings.ytdlp_generic_impersonate,
            )
            _validate_metadata_urls(meta)
            return meta
        LOGGER.debug("Instagram metadata primary attempt failed; using fallback", exc_info=True)
        # Fallback: retry without forced impersonation.
        meta = _get_video_meta(
            url,
            js_runtimes=settings.ytdlp_js_runtimes,
            remote_components=settings.ytdlp_remote_components,
            instagram_impersonate=None,
            instagram_retries=5,
            instagram_fragment_retries=5,
            instagram_socket_timeout=20,
            cookies_file=str(settings.cookies_file) if settings.cookies_file else None,
            generic_impersonate=None,
        )
        _validate_metadata_urls(meta)
        return meta


class MetadataBusy(RuntimeError):
    pass


class MetadataTimedOut(RuntimeError):
    pass


def _run_metadata_operation(url: str) -> dict[str, Any]:
    executor = metadata_executor
    if executor is None:
        raise RuntimeError("metadata executor is not initialized")
    if not metadata_slots.acquire(blocking=False):
        raise MetadataBusy("metadata capacity is full")

    def run() -> dict[str, Any]:
        try:
            return _get_video_meta_with_hidden_retries(url)
        finally:
            metadata_slots.release()

    future = executor.submit(run)
    try:
        return future.result(timeout=_settings().metadata_timeout_seconds)
    except FutureTimeoutError as exc:
        future.cancel()
        raise MetadataTimedOut("metadata extraction timed out") from exc


def _validate_metadata_urls(meta: dict[str, Any]) -> None:
    candidates = [meta.get(key) for key in ("webpage_url", "original_url", "url")]
    candidates.extend(item.get("url") for item in meta.get("formats", []) if isinstance(item, dict))
    checked_hosts: set[tuple[str, int | None]] = set()
    for value in candidates:
        if not isinstance(value, str) or not value.startswith(("http://", "https://")):
            continue
        parts = urlsplit(value)
        host_key = ((parts.hostname or "").lower(), parts.port)
        if host_key in checked_hosts:
            continue
        validate_public_url(value)
        checked_hosts.add(host_key)


# =========================
# Worker: download + send
# =========================
def _download_and_send(job: DownloadJob) -> None:
    job_id = job.job_id
    chat_id = job.chat_id
    reply_to_message_id = job.reply_to_message_id
    status_message_id = job.status_message_id
    url = job.url
    mode = job.mode
    title = job.title
    deadline = job.deadline

    job_dir = _output_folder() / job_id
    try:
        _output_folder().mkdir(parents=True, exist_ok=True)
        ensure_workspace_capacity(_output_folder(), required_workspace_bytes(MAX_SEND_BYTES))
        job_dir.mkdir(parents=True, exist_ok=False)
    except Exception as error:
        storage_failure = isinstance(error, DownloadStorageError) or is_enospc_error(error)
        LOGGER.error(
            "job workspace creation failed job_id=%s error=%s detail=%s",
            job_id,
            type(error).__name__,
            safe_error_for_log(error),
        )
        if storage_failure:
            _notify_operator_critical(f"Download workspace resource error\njob_id={job_id}")
        _safe_edit(
            chat_id,
            status_message_id,
            (
                f"{title}\n\nالحالة: ❌ مساحة التحميل غير متاحة مؤقتًا، جرّب بعد قليل."
                if storage_failure
                else f"{title}\n\nالحالة: ❌ فشل التحميل، حاول مرة أخرى."
            ),
            reply_markup=None,
            force=True,
        )
        with state_lock:
            cancel_events.pop(job_id, None)
            active_jobs.pop(job_id, None)
        return
    file_path: str | None = None
    settings = _settings()
    initial_youtube_language = _original_audio_language(job.metadata) if _is_youtube_url(url) else None

    def build_options(
        plan: VideoFormatCandidate | dict[str, Any],
        tmp_id: str,
        youtube_player_client: str | None = None,
        youtube_language: str | None = None,
        instagram_impersonate: bool = True,
        generic_impersonate: bool = False,
    ) -> dict[str, Any]:
        progress_state: dict[str, Any] = {"pct": 0}
        byte_limiter = DownloadByteLimiter(MAX_SEND_BYTES)

        def progress_hook(data: dict[str, Any]) -> None:
            _check_job(job_id, deadline)
            # Enforce the byte budget before any presentation-only filtering.
            # yt-dlp may report component/fragment filenames that do not match
            # our final output template.
            byte_limiter.check(data)
            filename = data.get("filename") or data.get("tmpfilename") or ""
            if filename and tmp_id not in os.path.basename(filename):
                return
            if data.get("status") == "downloading":
                pct, done_bytes, total_bytes = _calc_download_progress(data, progress_state)
                _safe_edit(
                    chat_id,
                    status_message_id,
                    _render_status(title, "downloading", pct, done_bytes, total_bytes),
                    reply_markup=_cancel_markup(job_id),
                )
            elif data.get("status") == "finished":
                progress_state["pct"] = 100
                _safe_edit(
                    chat_id,
                    status_message_id,
                    _render_status(title, "downloading", 100, None, None),
                    reply_markup=_cancel_markup(job_id),
                    force=True,
                )

        options: dict[str, Any] = {
            "format": str(plan.get("format_spec", "best")),
            "outtmpl": os.fspath(job_dir / f"{tmp_id}.%(ext)s"),
            "progress_hooks": [progress_hook],
            "max_filesize": MAX_SEND_BYTES,
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "concurrent_fragment_downloads": settings.concurrent_fragments,
            "retries": 5,
            "fragment_retries": 5,
            "socket_timeout": 20,
            "postprocessor_hooks": [lambda _status: _check_job(job_id, deadline)],
        }
        if settings.cookies_file:
            options["cookiefile"] = os.fspath(settings.cookies_file)
        if settings.ytdlp_http_chunk_size and _is_youtube_url(url):
            # Chunked ranged requests avoid YouTube's per-connection throttling.
            options["http_chunk_size"] = settings.ytdlp_http_chunk_size
        _apply_youtube_runtime_opts(
            options,
            url,
            settings.ytdlp_js_runtimes,
            settings.ytdlp_remote_components,
            player_client=youtube_player_client,
            youtube_language=youtube_language,
        )
        _apply_instagram_stability_opts(
            options,
            url,
            impersonate=settings.ytdlp_instagram_impersonate if instagram_impersonate else None,
            retries=settings.ytdlp_instagram_retries,
            fragment_retries=settings.ytdlp_instagram_fragment_retries,
            socket_timeout=settings.ytdlp_instagram_socket_timeout,
        )
        _apply_generic_impersonation_opts(
            options,
            url,
            settings.ytdlp_generic_impersonate if generic_impersonate else None,
        )
        if plan.get("merge_output_format"):
            options["merge_output_format"] = str(plan["merge_output_format"])
        if mode == "audio":
            options["postprocessors"] = [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": str(int(plan.get("mp3_kbps", 128))),
                }
            ]
        return options

    if mode == "audio":
        initial_audio_plans, _initial_audio_reason = _build_audio_plans_mp3(
            job.metadata,
            MAX_SEND_BYTES,
            original_language=initial_youtube_language,
        )
        initial_plans: list[VideoFormatCandidate | dict[str, Any]] = list(
            initial_audio_plans or ([job.audio_plan] if job.audio_plan else [])
        )
    else:
        initial_plans = list(job.video_candidates)

    def fresh_plans(
        metadata: dict[str, Any],
        youtube_language: str | None = None,
    ) -> list[VideoFormatCandidate | dict[str, Any]]:
        if mode == "audio":
            audio_plans, _reason = _build_audio_plans_mp3(
                metadata,
                MAX_SEND_BYTES,
                original_language=youtube_language,
            )
            return list(audio_plans)
        return list(
            _build_video_candidates(
                metadata,
                MAX_SEND_BYTES,
                original_language=youtube_language,
            )
        )

    def cleanup_attempt_files() -> None:
        try:
            cleanup_directory_contents(job_dir, strict=True)
        except OSError as error:
            raise_if_terminal_storage_error(error)
            raise RuntimeError("download attempt cleanup failed") from error

    def try_plans(
        metadata: dict[str, Any],
        plans: list[VideoFormatCandidate | dict[str, Any]],
        *,
        phase: str,
        youtube_player_client: str | None = None,
        youtube_language: str | None = None,
        concurrent_fragments: int | None = None,
        instagram_impersonate: bool = True,
        generic_impersonate: bool = False,
    ) -> str | None:
        for index, plan in enumerate(plans, start=1):
            _check_job(job_id, deadline)
            cleanup_attempt_files()
            tmp_id = uuid.uuid4().hex
            options = build_options(
                plan,
                tmp_id,
                youtube_player_client=youtube_player_client,
                youtube_language=youtube_language,
                instagram_impersonate=instagram_impersonate,
                generic_impersonate=generic_impersonate,
            )
            if concurrent_fragments is not None:
                options["concurrent_fragment_downloads"] = concurrent_fragments
            try:
                with yt_dlp.YoutubeDL(options) as ydl:
                    info = ydl.process_ie_result(
                        _metadata_without_format_selection(metadata),
                        download=True,
                    )
                _check_job(job_id, deadline)
                prefer_ext = ".mp3" if mode == "audio" else None
                candidate_path = _find_downloaded_file(
                    info,
                    os.fspath(job_dir),
                    tmp_id,
                    prefer_ext=prefer_ext,
                )
                if not candidate_path:
                    candidate_path = _find_file_by_prefix(
                        os.fspath(job_dir),
                        tmp_id,
                        prefer_ext=prefer_ext,
                    )
                if not candidate_path:
                    raise RuntimeError("downloaded file was not found")
                remaining = max(1.0, deadline - time.monotonic())
                validate_media_file(
                    candidate_path,
                    mode,
                    MAX_SEND_BYTES,
                    timeout_seconds=min(15.0, remaining),
                )
                return candidate_path
            except (JobCancelled, JobTimedOut):
                raise
            except Exception as error:
                raise_if_terminal_storage_error(error)
                size_limited = is_download_size_error(error)
                LOGGER.warning(
                    "download candidate failed job_id=%s phase=%s candidate=%s attempt=%s/%s "
                    "client=%s reason=%s error=%s detail=%s",
                    job_id,
                    phase,
                    plan.get("format_spec"),
                    index,
                    len(plans),
                    youtube_player_client or "default",
                    "size-limit" if size_limited else "download-or-validation",
                    type(error).__name__,
                    safe_error_for_log(error),
                )
                cleanup_attempt_files()
                _check_job(job_id, deadline)
        return None

    def extract_fresh_youtube(player_client: str | None) -> tuple[dict[str, Any], str | None]:
        metadata, discovered_language, _has_original = _get_youtube_meta_for_client(
            url,
            player_client,
            initial_youtube_language,
        )
        return metadata, discovered_language or initial_youtube_language

    try:
        _check_job(job_id, deadline)

        _safe_edit(
            chat_id,
            status_message_id,
            _render_status(title, "downloading", 0, None, None),
            reply_markup=_cancel_markup(job_id),
            force=True,
        )

        file_path = try_plans(
            job.metadata,
            initial_plans,
            phase="initial",
            youtube_language=initial_youtube_language,
        )

        if file_path is None and _is_youtube_url(url):
            for player_client in _youtube_player_clients(settings.ytdlp_youtube_player_clients):
                _check_job(job_id, deadline)
                cleanup_attempt_files()
                try:
                    fresh_metadata, fresh_language = extract_fresh_youtube(player_client)
                    candidate_plans = fresh_plans(fresh_metadata, fresh_language)
                    if not candidate_plans:
                        raise RuntimeError("fresh metadata has no eligible download candidates")
                except (JobCancelled, JobTimedOut):
                    raise
                except Exception as error:
                    raise_if_terminal_storage_error(error)
                    LOGGER.warning(
                        "YouTube fallback extraction failed job_id=%s client=%s error=%s detail=%s",
                        job_id,
                        player_client or "default",
                        type(error).__name__,
                        safe_error_for_log(error),
                    )
                    continue
                file_path = try_plans(
                    fresh_metadata,
                    candidate_plans,
                    phase="fresh",
                    youtube_player_client=player_client,
                    youtube_language=fresh_language,
                    concurrent_fragments=1,
                )
                if file_path is not None:
                    break

        if file_path is None and _is_instagram_url(url):
            cleanup_attempt_files()
            try:
                fresh_metadata = _get_video_meta(
                    url,
                    js_runtimes=settings.ytdlp_js_runtimes,
                    remote_components=settings.ytdlp_remote_components,
                    instagram_impersonate=None,
                    instagram_retries=5,
                    instagram_fragment_retries=5,
                    instagram_socket_timeout=20,
                    cookies_file=str(settings.cookies_file) if settings.cookies_file else None,
                    generic_impersonate=None,
                )
                _validate_metadata_urls(fresh_metadata)
                file_path = try_plans(
                    fresh_metadata,
                    fresh_plans(fresh_metadata),
                    phase="fresh",
                    concurrent_fragments=1,
                    instagram_impersonate=False,
                )
            except (JobCancelled, JobTimedOut):
                raise
            except Exception as error:
                raise_if_terminal_storage_error(error)
                LOGGER.warning(
                    "Instagram fallback extraction failed job_id=%s error=%s detail=%s",
                    job_id,
                    type(error).__name__,
                    safe_error_for_log(error),
                )

        if (
            file_path is None
            and not _is_youtube_url(url)
            and not _is_instagram_url(url)
            and settings.ytdlp_generic_impersonate
        ):
            cleanup_attempt_files()
            try:
                fresh_metadata = _get_video_meta(
                    url,
                    js_runtimes=settings.ytdlp_js_runtimes,
                    remote_components=settings.ytdlp_remote_components,
                    cookies_file=str(settings.cookies_file) if settings.cookies_file else None,
                    generic_impersonate=settings.ytdlp_generic_impersonate,
                )
                _validate_metadata_urls(fresh_metadata)
                file_path = try_plans(
                    fresh_metadata,
                    fresh_plans(fresh_metadata),
                    phase="impersonated",
                    concurrent_fragments=1,
                    generic_impersonate=True,
                )
            except (JobCancelled, JobTimedOut):
                raise
            except Exception as error:
                raise_if_terminal_storage_error(error)
                LOGGER.warning(
                    "generic impersonation fallback failed job_id=%s error=%s detail=%s",
                    job_id,
                    type(error).__name__,
                    safe_error_for_log(error),
                )

        if file_path is None:
            raise RuntimeError("all prechecked download candidates failed")

        _check_job(job_id, deadline)
        if not file_path:
            raise RuntimeError("downloaded file was not found")

        base = _sanitize_filename_base(title)
        raw_duration = job.metadata.get("duration") if isinstance(job.metadata, dict) else None
        duration_hint = int(raw_duration) if isinstance(raw_duration, (int, float)) and raw_duration > 0 else None

        if mode == "audio":
            send_filename = f"{base}.mp3"
            _send_via_bot_api_with_progress(
                job_id=job_id,
                chat_id=chat_id,
                reply_to_message_id=reply_to_message_id,
                status_message_id=status_message_id,
                title=title,
                method_name="sendAudio",
                file_field_name="audio",
                file_path=file_path,
                send_filename=send_filename,
                stage_label="جارٍ إرسال الصوت...",
                extra_params={
                    "caption": build_caption(title, duration_hint),
                    "title": title[:64],
                    **({"duration": int(duration_hint)} if duration_hint else {}),
                },
                deadline=deadline,
            )
        elif mode == "doc":
            ext = os.path.splitext(file_path)[1] or ".mp4"
            send_filename = f"{base}{ext}"
            _send_via_bot_api_with_progress(
                job_id=job_id,
                chat_id=chat_id,
                reply_to_message_id=reply_to_message_id,
                status_message_id=status_message_id,
                title=title,
                method_name="sendDocument",
                file_field_name="document",
                file_path=file_path,
                send_filename=send_filename,
                stage_label="جارٍ إرسال الملف...",
                extra_params={"caption": build_caption(title)},
                deadline=deadline,
            )
        else:
            ext = os.path.splitext(file_path)[1] or ".mp4"
            send_filename = f"{base}{ext}"
            _send_via_bot_api_with_progress(
                job_id=job_id,
                chat_id=chat_id,
                reply_to_message_id=reply_to_message_id,
                status_message_id=status_message_id,
                title=title,
                method_name="sendVideo",
                file_field_name="video",
                file_path=file_path,
                send_filename=send_filename,
                stage_label="جارٍ إرسال الفيديو...",
                extra_params=_video_send_params(file_path, title, duration_hint),
                deadline=deadline,
                thumb_path=make_thumbnail(file_path, job_dir / "thumb.jpg", duration_hint),
            )

        # Success: delete status message (only media remains)
        _safe_delete(chat_id, status_message_id)

    except JobCancelled:
        LOGGER.info("job cancelled job_id=%s stage=processing", job_id)
        _safe_delete(chat_id, status_message_id)
    except JobTimedOut:
        LOGGER.warning("job timed out job_id=%s stage=processing", job_id)
        _safe_edit(
            chat_id,
            status_message_id,
            f"{title}\n\nالحالة: ❌ انتهت مهلة المهمة، حاول مرة أخرى.",
            reply_markup=None,
            force=True,
        )
    except DownloadStorageError as error:
        LOGGER.error(
            "job stopped by terminal storage error job_id=%s detail=%s",
            job_id,
            safe_error_for_log(error),
        )
        _notify_operator_critical(f"Download workspace resource error\njob_id={job_id}")
        _safe_edit(
            chat_id,
            status_message_id,
            f"{title}\n\nالحالة: ❌ مساحة التحميل غير متاحة مؤقتًا، جرّب بعد قليل.",
            reply_markup=None,
            force=True,
        )
    except Exception:
        LOGGER.exception("job failed job_id=%s stage=processing url=%s", job_id, safe_url_for_log(url))
        if _is_cancelled(job_id):
            _safe_delete(chat_id, status_message_id)
        else:
            _safe_edit(
                chat_id,
                status_message_id,
                f"{title}\n\nالحالة: ❌ فشل التحميل، حاول مرة أخرى.",
                reply_markup=None,
                force=True,
            )

    finally:
        cleanup_job_directory(job_dir)
        with state_lock:
            cancel_events.pop(job_id, None)
            active_jobs.pop(job_id, None)


def _worker_loop():
    while not stop_event.is_set():
        job = jobs_q.get()
        try:
            if job is None:
                return
            _download_and_send(job)
        except Exception:
            LOGGER.exception("worker crashed while processing a job")
        finally:
            jobs_q.task_done()


# =========================
# Logging (kept as-is)
# =========================
def log(message, text: str, media: str):
    LOGGER.info(
        "download request media=%s user_id=%s chat_id=%s url=%s",
        media,
        message.from_user.id,
        message.chat.id,
        safe_url_for_log(text),
    )


# =========================
# Commands
# =========================
def start_help(message):
    assert bot is not None
    bot.reply_to(
        message,
        "*أرسل لي رابط فيديو* وسأحمّله لك.\n\n"
        "تقدر تختار:\n"
        "• *فيديو* (ويمكنك اختيار الجودة)\n"
        "• *ملف* (الملف الأصلي)\n"
        "• *صوت (MP3)*\n"
        "• *قائمة تشغيل*: أرسل رابطها واختر عدد الفيديوهات\n\n"
        f"حد الرفع: *{_fmt_bytes(MAX_SEND_BYTES)}*\n\n"
        "_Powered by_ [Avazbek Olimov](https://github.com/Avazbek22/VideoDownloaderBot)",
        parse_mode="MARKDOWN",
        disable_web_page_preview=True,
    )


# =========================
# Pending cleanup
# =========================
def _cleanup_pending() -> None:
    now = time.time()
    with state_lock:
        expired = [rid for rid, data in pending_requests.items() if now - data.created_at > PENDING_TTL_SEC]
        for rid in expired:
            pending_requests.pop(rid, None)
        for rid in [r for r, d in pending_playlists.items() if now - d["created_at"] > PENDING_TTL_SEC]:
            pending_playlists.pop(rid, None)
        stale_edit_keys = [
            key
            for key, edited_at in last_edited.items()
            if (datetime.datetime.now() - edited_at).total_seconds() > PENDING_TTL_SEC
        ]
        for key in stale_edit_keys:
            last_edited.pop(key, None)
            last_text.pop(key, None)


# =========================
# Main flow: message -> Getting info -> buttons
# (NO downloading unless size <= limit is proven)
# =========================
def _quality_choices(candidates, limit: int = 5) -> list[tuple[int, str]]:
    """(index, label) of the first candidate for every distinct quality label, best first."""
    seen: set[str] = set()
    choices: list[tuple[int, str]] = []
    for index, candidate in enumerate(candidates):
        label = str(candidate.get("quality_label", "") or "")
        if not label or label in seen:
            continue
        seen.add(label)
        choices.append((index, label))
        if len(choices) >= limit:
            break
    return choices


pending_playlists: dict[str, dict[str, Any]] = {}


def _is_playlist_meta(meta: dict[str, Any]) -> bool:
    return meta.get("_type") == "playlist" and bool(meta.get("entries")) and not meta.get("formats")


def _playlist_entry_urls(meta: dict[str, Any], limit: int) -> list[str]:
    urls: list[str] = []
    for entry in list(meta.get("entries") or []):
        if not isinstance(entry, dict):
            continue
        value = entry.get("webpage_url") or entry.get("url")
        if isinstance(value, str) and value.startswith(("http://", "https://")) and value not in urls:
            urls.append(value)
        if len(urls) >= limit:
            break
    return urls


def _offer_playlist(message, url: str, meta: dict[str, Any], title: str) -> None:
    assert bot is not None
    settings = _settings()
    urls = _playlist_entry_urls(meta, PLAYLIST_LIST_LIMIT)
    if not urls:
        bot.reply_to(message, "قائمة التشغيل فارغة أو غير مدعومة.", disable_web_page_preview=True)
        return
    top = min(len(urls), settings.playlist_max)
    counts = sorted({n for n in (1, 3, 5, top) if 1 <= n <= top})
    request_id = uuid.uuid4().hex[:18]
    with state_lock:
        pending_playlists[request_id] = {
            "created_at": time.time(),
            "user_id": message.from_user.id,
            "chat_id": message.chat.id,
            "reply_to": message.message_id,
            "urls": urls[:top],
            "title": title,
        }
    kb = types.InlineKeyboardMarkup(row_width=4)
    kb.add(*[types.InlineKeyboardButton(f"🎬 أول {n}", callback_data=f"pl|video|{n}|{request_id}") for n in counts])
    kb.add(*[types.InlineKeyboardButton(f"🎵 أول {n}", callback_data=f"pl|audio|{n}|{request_id}") for n in counts])
    _safe_send_message(
        chat_id=message.chat.id,
        text=(
            f"📃 {title}\n\nقائمة تشغيل: {len(urls)} عنصر ظاهر.\n"
            f"اختر كم عنصر تحمّل (الحد الأقصى {settings.playlist_max})، فيديو 🎬 أو صوت 🎵.\n"
            "كل عنصر يُحمَّل بأفضل جودة تناسب حد الرفع."
        ),
        reply_to_message_id=message.message_id,
        reply_markup=kb,
    )


def _enqueue_job(
    *,
    user_id: int,
    chat_id: int,
    reply_to_message_id: int,
    status_message_id: int,
    url: str,
    title: str,
    mode: str,
    video_candidates,
    audio_plan,
    metadata,
) -> bool:
    job_id = uuid.uuid4().hex[:18]
    cancel_event = threading.Event()
    with state_lock:
        cancel_events[job_id] = cancel_event
        active_jobs[job_id] = ActiveJob(
            user_id=user_id, chat_id=chat_id, status_message_id=status_message_id, cancel_event=cancel_event
        )
    _safe_edit(
        chat_id,
        status_message_id,
        _render_status(title, "queued", None, None, None, queued_pos=jobs_q.qsize() + 1),
        reply_markup=_cancel_markup(job_id),
        force=True,
    )
    job = DownloadJob(
        job_id=job_id,
        user_id=user_id,
        chat_id=chat_id,
        reply_to_message_id=reply_to_message_id,
        status_message_id=status_message_id,
        url=url,
        title=title,
        mode=mode,
        video_candidates=tuple(video_candidates),
        audio_plan=audio_plan,
        metadata=metadata,
        deadline=time.monotonic() + _settings().job_timeout_seconds,
    )
    try:
        jobs_q.put_nowait(job)
    except queue.Full:
        with state_lock:
            cancel_events.pop(job_id, None)
            active_jobs.pop(job_id, None)
        return False
    return True


def _run_playlist(user_id: int, chat_id: int, reply_to: int, urls: list[str], mode: str) -> None:
    """Queue every playlist item as an ordinary job, one after another."""
    total = len(urls)
    for position, item_url in enumerate(urls, start=1):
        if stop_event.is_set():
            return
        prefix = f"[{position}/{total}] "
        status = _safe_send_message(chat_id, prefix + "جارٍ جلب المعلومات...", reply_to_message_id=reply_to)
        if status is None:
            continue

        def fail(text: str) -> None:
            _safe_edit(chat_id, status.message_id, prefix + text, force=True)

        try:
            validate_public_url(item_url)
        except UnsafeUrlError:
            fail("رابط غير صالح.")
            continue
        meta = None
        for _attempt in range(5):
            try:
                meta = _run_metadata_operation(item_url)
                break
            except MetadataBusy:
                time.sleep(3)  # other lookups are running; wait for a free slot
            except Exception:
                LOGGER.warning("playlist item metadata failed url=%s", safe_url_for_log(item_url), exc_info=True)
                break
        if meta is None:
            fail("تعذّر جلب المعلومات.")
            continue
        title = _strip_hashtags((meta.get("title") or "Video").strip()) or "Video"
        if _is_playlist_meta(meta):
            fail("هذا العنصر قائمة وليس فيديو، تم تخطيه.")
            continue
        candidates = [c for c in _build_video_candidates(meta, 2**63 - 1) if c.estimated_size <= MAX_SEND_BYTES]
        audio_plan, _reason = _build_audio_plan_mp3(meta, MAX_SEND_BYTES)
        if mode == "audio" and not audio_plan:
            fail(f"{title}\nالصوت غير متاح أو كبير على الحد.")
            continue
        if mode == "video" and not candidates:
            fail(f"{title}\nلا توجد جودة تناسب حد الرفع.")
            continue
        queued = _enqueue_job(
            user_id=user_id,
            chat_id=chat_id,
            reply_to_message_id=reply_to,
            status_message_id=status.message_id,
            url=item_url,
            title=prefix + title,
            mode=mode,
            video_candidates=candidates if mode == "video" else (),
            audio_plan=audio_plan if mode == "audio" else None,
            metadata=meta,
        )
        if not queued:
            fail("الطابور ممتلئ، تم إيقاف باقي القائمة.")
            return
        time.sleep(3)  # spread requests out: bursts are what get an IP flagged


def on_playlist_choice(call):
    try:
        parts = call.data.split("|")
        if len(parts) != 4 or parts[1] not in {"video", "audio"} or not parts[2].isdigit():
            _safe_answer_callback(call.id, "إجراء غير صالح")
            return
        mode, count, rid = parts[1], int(parts[2]), parts[3]
        with state_lock:
            data = pending_playlists.get(rid)
            if data is not None and time.time() - data["created_at"] > PENDING_TTL_SEC:
                pending_playlists.pop(rid, None)
                data = None
            if data is not None and call.from_user.id == data["user_id"]:
                pending_playlists.pop(rid, None)
        if not data:
            _safe_answer_callback(call.id, "انتهت صلاحية الطلب، أرسل الرابط مرة أخرى.")
            return
        if call.from_user.id != data["user_id"]:
            _safe_answer_callback(call.id, "هذا الطلب ليس طلبك.")
            return
        urls = data["urls"][: max(1, min(count, _settings().playlist_max))]
        _safe_answer_callback(call.id, "تمام")
        _safe_delete(call.message.chat.id, call.message.message_id)
        threading.Thread(
            target=_run_playlist,
            args=(data["user_id"], data["chat_id"], data["reply_to"], urls, mode),
            name="playlist",
            daemon=True,
        ).start()
    except Exception:
        LOGGER.exception("playlist callback failed")
        _safe_answer_callback(call.id, "حدث خطأ")


def _send_choice_ui(message, url: str) -> None:
    assert bot is not None
    _cleanup_pending()

    processing_msg = bot.reply_to(message, "جارٍ جلب المعلومات...", disable_web_page_preview=True)

    try:
        meta = _run_metadata_operation(url)
    except (MetadataBusy, MetadataTimedOut):
        LOGGER.warning("metadata unavailable url=%s", safe_url_for_log(url), exc_info=True)
        _safe_delete(message.chat.id, processing_msg.message_id)
        bot.reply_to(message, "الرابط غير صالح أو الموقع غير مدعوم.", disable_web_page_preview=True)
        return
    except Exception:
        LOGGER.exception("metadata failed url=%s", safe_url_for_log(url))
        _safe_delete(message.chat.id, processing_msg.message_id)
        bot.reply_to(message, "الرابط غير صالح أو الموقع غير مدعوم.", disable_web_page_preview=True)
        return

    title = (meta.get("title") or "Video").strip()
    title = _strip_hashtags(title) or "Video"

    if _is_playlist_meta(meta):
        _safe_delete(message.chat.id, processing_msg.message_id)
        _offer_playlist(message, url, meta, title)
        return

    # Approximate fragmented sizes carry explicit headroom; the runtime byte
    # limiter remains authoritative for every retained candidate.
    estimated_candidates = _build_video_candidates(meta, 2**63 - 1)
    video_candidates = [candidate for candidate in estimated_candidates if candidate.estimated_size <= MAX_SEND_BYTES]
    video_plan = video_candidates[0] if video_candidates else None
    display_plan = video_plan or (estimated_candidates[0] if estimated_candidates else None)

    audio_plan, audio_reason = _build_audio_plan_mp3(meta, MAX_SEND_BYTES)

    _safe_delete(message.chat.id, processing_msg.message_id)

    video_size = display_plan.get("estimated_size") if display_plan else None
    video_size_known = bool(display_plan and display_plan.estimated_size > 0)
    video_ok = bool(video_size_known and isinstance(video_size, int) and video_size <= MAX_SEND_BYTES)

    if video_size_known and isinstance(video_size, int) and video_size > MAX_SEND_BYTES:
        msg = (
            f"{title}\n\n"
            f"الفيديو كبير جدًا على تيليجرام.\n"
            f"الحجم التقريبي: {_fmt_bytes(video_size)}\n"
            f"الحد: {_fmt_bytes(MAX_SEND_BYTES)}\n"
        )

        # If audio fits, offer only audio button
        if audio_plan:
            request_id = uuid.uuid4().hex[:18]
            with state_lock:
                pending_requests[request_id] = PendingRequest(
                    created_at=time.time(),
                    user_id=message.from_user.id,
                    chat_id=message.chat.id,
                    reply_to_message_id=message.message_id,
                    url=url,
                    title=title,
                    video_candidates=(),
                    audio_plan=audio_plan,
                    metadata=meta,
                )

            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.add(types.InlineKeyboardButton("🎵 تحميل كصوت (MP3)", callback_data=f"dl|audio|{request_id}"))

            _safe_send_message(
                chat_id=message.chat.id,
                text=msg + f"\nمتاح كصوت: {audio_plan.get('quality_label', 'mp3')}",
                reply_to_message_id=message.message_id,
                reply_markup=kb,
            )
            return

        # No audio either
        if audio_reason:
            msg += f"\nالصوت غير متاح: {audio_reason}"
        _safe_send_message(message.chat.id, msg, reply_to_message_id=message.message_id)
        return

    # Unknown-size formats are not offered. Approximate fragmented formats are
    # allowed only after adding headroom and remain protected by the hard cap.
    if not video_ok:
        msg = (
            f"{title}\n\n"
            f"لا أستطيع تحديد الحجم النهائي للفيديو قبل التحميل.\n"
            f"حد الرفع على تيليجرام {_fmt_bytes(MAX_SEND_BYTES)}.\n"
            f"جرّب فيديو أقصر.\n"
        )

        # If audio fits, offer audio
        if audio_plan:
            request_id = uuid.uuid4().hex[:18]
            with state_lock:
                pending_requests[request_id] = PendingRequest(
                    created_at=time.time(),
                    user_id=message.from_user.id,
                    chat_id=message.chat.id,
                    reply_to_message_id=message.message_id,
                    url=url,
                    title=title,
                    video_candidates=(),
                    audio_plan=audio_plan,
                    metadata=meta,
                )

            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.add(types.InlineKeyboardButton("🎵 تحميل كصوت (MP3)", callback_data=f"dl|audio|{request_id}"))

            _safe_send_message(
                chat_id=message.chat.id,
                text=msg + f"\nمتاح كصوت: {audio_plan.get('quality_label', 'mp3')}",
                reply_to_message_id=message.message_id,
                reply_markup=kb,
            )
            return

        if audio_reason:
            msg += f"\nالصوت غير متاح: {audio_reason}"
        _safe_send_message(message.chat.id, msg, reply_to_message_id=message.message_id)
        return

    # If video_ok == True -> show normal 3 buttons (Video/Document/Audio if available)
    request_id = uuid.uuid4().hex[:18]
    with state_lock:
        pending_requests[request_id] = PendingRequest(
            created_at=time.time(),
            user_id=message.from_user.id,
            chat_id=message.chat.id,
            reply_to_message_id=message.message_id,
            url=url,
            title=title,
            video_candidates=tuple(video_candidates),
            audio_plan=audio_plan,
            metadata=meta,
        )

    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("⭐ أفضل جودة تناسب الحد", callback_data=f"dl|video|{request_id}"),
        types.InlineKeyboardButton("📄 كملف", callback_data=f"dl|doc|{request_id}"),
    )
    quality_choices = _quality_choices(video_candidates)
    if len(quality_choices) > 1:
        kb.add(
            *[
                types.InlineKeyboardButton(f"🎬 {label}", callback_data=f"dl|video|{request_id}|{index}")
                for index, label in quality_choices
            ]
        )
    if audio_plan:
        kb.add(types.InlineKeyboardButton("🎵 تحميل كصوت (MP3)", callback_data=f"dl|audio|{request_id}"))

    size_kind = "الحجم المتوقع" if video_plan.estimated_confident else "الحجم التقريبي"
    info_lines = [
        f"{size_kind}: {_fmt_bytes(int(video_size))} (الحد {_fmt_bytes(MAX_SEND_BYTES)})",
        f"الجودة المختارة: {video_plan.get('quality_label', 'mp4')}",
    ]
    if audio_plan:
        info_lines.append(f"الصوت: {audio_plan.get('quality_label', 'mp3')}")

    _safe_send_message(
        chat_id=message.chat.id,
        text=f"{title}\n\nاختر طريقة التحميل:\n" + "\n".join(info_lines),
        reply_to_message_id=message.message_id,
        reply_markup=kb,
    )


def handle_private_messages(message):
    if message.chat.type != "private":
        return

    text = message.text if message.text else message.caption if message.caption else None
    if not text:
        return

    if isinstance(text, str) and text.strip().startswith("/"):
        return

    url = _extract_first_url(text)
    if not url:
        return

    try:
        validate_public_url(url)
    except UnsafeUrlError:
        bot.reply_to(message, "الرابط غير صالح", disable_web_page_preview=True)
        return

    url_info = urlparse(url)

    if url_info.netloc in ["www.youtube.com", "youtu.be", "youtube.com", "youtu.be"] and not youtube_url_validation(
        url
    ):
        bot.reply_to(message, "الرابط غير صالح", disable_web_page_preview=True)
        return

    log(message, url, "video")
    _send_choice_ui(message, url)


# =========================
# Callback: cancel
# =========================
def on_cancel(call):
    try:
        parts = call.data.split("|")
        if len(parts) != 2:
            _safe_answer_callback(call.id, "إجراء غير صالح")
            return

        job_id = parts[1]
        with state_lock:
            job_info = active_jobs.get(job_id)
        if not job_info:
            _safe_answer_callback(call.id, "لا يوجد ما يُلغى.")
            return

        if call.from_user.id != job_info.user_id:
            _safe_answer_callback(call.id, "هذا الطلب ليس طلبك.")
            return

        ev = job_info.cancel_event
        if ev:
            ev.set()

        chat_id = job_info.chat_id
        status_mid = job_info.status_message_id
        if isinstance(chat_id, int) and isinstance(status_mid, int):
            _safe_delete(chat_id, status_mid)

        _safe_answer_callback(call.id, "تم الإلغاء.")
    except Exception:
        LOGGER.exception("cancel callback failed")
        _safe_answer_callback(call.id, "حدث خطأ")


# =========================
# Callback: buttons -> enqueue job
# =========================
def on_download_choice(call):
    try:
        parts = call.data.split("|")
        if len(parts) not in (3, 4):
            _safe_answer_callback(call.id, "إجراء غير صالح")
            return

        mode = parts[1]  # video/doc/audio
        rid = parts[2]
        quality_index: int | None = None
        if len(parts) == 4:
            try:
                quality_index = int(parts[3])
            except ValueError:
                _safe_answer_callback(call.id, "إجراء غير صالح")
                return

        with state_lock:
            req = pending_requests.get(rid)
            if req is not None and time.time() - req.created_at > PENDING_TTL_SEC:
                pending_requests.pop(rid, None)
                req = None
            if req is not None and call.from_user.id == req.user_id:
                pending_requests.pop(rid, None)

        if not req:
            _safe_answer_callback(call.id, "انتهت صلاحية الطلب، أرسل الرابط مرة أخرى.")
            return

        if call.from_user.id != req.user_id:
            _safe_answer_callback(call.id, "هذا الطلب ليس طلبك.")
            return

        chat_id = req.chat_id
        reply_to_message_id = req.reply_to_message_id
        status_message_id = call.message.message_id
        url = req.url
        title = req.title

        if mode == "audio":
            if not req.audio_plan:
                _safe_answer_callback(call.id, "الصوت غير متاح.")
                return
            job_mode = "audio"
        elif mode == "doc":
            if not req.video_candidates:
                _safe_answer_callback(call.id, "الفيديو غير متاح.")
                return
            job_mode = "doc"
        else:
            if not req.video_candidates:
                _safe_answer_callback(call.id, "الفيديو غير متاح.")
                return
            job_mode = "video"

        video_candidates = req.video_candidates
        if quality_index is not None:
            if not 0 <= quality_index < len(video_candidates):
                _safe_answer_callback(call.id, "إجراء غير صالح")
                return
            # The chosen quality goes first; the rest stay as automatic fallbacks.
            chosen = video_candidates[quality_index]
            video_candidates = (chosen, *[c for c in video_candidates if c is not chosen])

        _safe_answer_callback(call.id, "تمام")

        job_id = uuid.uuid4().hex[:18]
        cancel_event = threading.Event()
        with state_lock:
            cancel_events[job_id] = cancel_event
            active_jobs[job_id] = ActiveJob(
                user_id=req.user_id,
                chat_id=chat_id,
                status_message_id=status_message_id,
                cancel_event=cancel_event,
            )

        queued_pos = jobs_q.qsize() + 1
        _safe_edit(
            chat_id,
            status_message_id,
            _render_status(title, "queued", None, None, None, queued_pos=queued_pos),
            reply_markup=_cancel_markup(job_id),
            force=True,
        )

        job = DownloadJob(
            job_id=job_id,
            user_id=req.user_id,
            chat_id=chat_id,
            reply_to_message_id=reply_to_message_id,
            status_message_id=status_message_id,
            url=url,
            title=title,
            mode=job_mode,
            video_candidates=video_candidates,
            audio_plan=req.audio_plan,
            metadata=req.metadata,
            deadline=time.monotonic() + _settings().job_timeout_seconds,
        )
        try:
            jobs_q.put_nowait(job)
        except queue.Full:
            with state_lock:
                cancel_events.pop(job_id, None)
                active_jobs.pop(job_id, None)
            _safe_edit(
                chat_id,
                status_message_id,
                f"{title}\n\nالحالة: ❌ الطابور ممتلئ، حاول لاحقًا.",
                reply_markup=None,
                force=True,
            )

    except Exception:
        LOGGER.exception("download callback failed")
        _safe_answer_callback(call.id, "حدث خطأ")


# =========================
# Keep your /custom as-is
# =========================
def get_text(message):
    if not message.text:
        return None
    if len(message.text.split(" ")) < 2:
        if message.reply_to_message and message.reply_to_message.text:
            return message.reply_to_message.text
        return None
    return message.text.split(" ")[1]


def custom(message):
    assert bot is not None
    text = get_text(message)
    if not text:
        bot.reply_to(message, "استخدام غير صحيح، اكتب `/custom الرابط`", parse_mode="MARKDOWN")
        return

    msg = bot.reply_to(message, "جارٍ جلب الصيغ...", disable_web_page_preview=True)

    try:
        info = _get_video_meta_with_hidden_retries(text)

        data = {
            f"{x.get('resolution')}.{x.get('ext')}": {"callback_data": f"{x.get('format_id')}"}
            for x in info.get("formats", [])
            if x.get("video_ext") != "none"
        }

        markup = quick_markup(data, row_width=2)

        _safe_delete(msg.chat.id, msg.message_id)
        bot.reply_to(message, "اختر صيغة", reply_markup=markup, disable_web_page_preview=True)
    except Exception:
        LOGGER.exception("custom formats metadata failed url=%s", safe_url_for_log(text))
        _safe_delete(msg.chat.id, msg.message_id)
        bot.reply_to(message, "تعذّر جلب الصيغ.", disable_web_page_preview=True)


def callback_custom_format(call):
    try:
        if not call.message.reply_to_message:
            return
        if call.from_user.id != call.message.reply_to_message.from_user.id:
            _safe_answer_callback(call.id, "أنت لم ترسل هذا الطلب")
            return

        url = get_text(call.message.reply_to_message)
        if not url:
            _safe_answer_callback(call.id, "لا يوجد رابط")
            return

        _safe_delete(call.message.chat.id, call.message.message_id)

        _send_choice_ui(call.message.reply_to_message, url)

        _safe_answer_callback(call.id, "تمام")
    except Exception:
        LOGGER.exception("custom callback failed")


def register_handlers(instance: telebot.TeleBot) -> None:
    instance.message_handler(commands=["start", "help"])(start_help)
    instance.message_handler(commands=["custom"])(custom)
    instance.message_handler(
        func=lambda m: True,
        content_types=["text", "photo", "video", "document", "audio", "voice"],
    )(handle_private_messages)
    instance.callback_query_handler(func=lambda call: bool(call.data and call.data.startswith("cnl|")))(on_cancel)
    instance.callback_query_handler(func=lambda call: bool(call.data and call.data.startswith("dl|")))(
        on_download_choice
    )
    instance.callback_query_handler(func=lambda call: bool(call.data and call.data.startswith("pl|")))(
        on_playlist_choice
    )
    instance.callback_query_handler(
        func=lambda call: bool(call.data)
        and not call.data.startswith(("dl|", "cnl|", "pl|"))
    )(callback_custom_format)


def _preflight(settings: Settings, instance: telebot.TeleBot) -> None:
    validate_workspace_location(settings.output_dir)
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    ensure_workspace_capacity(
        settings.output_dir,
        required_workspace_bytes(settings.max_filesize, settings.workers),
    )
    probe = settings.output_dir / ".write-test"
    probe.write_text("ok", encoding="utf-8")
    probe.unlink()
    if settings.cookies_file and not settings.cookies_file.is_file():
        raise RuntimeError(f"COOKIES_FILE does not exist: {settings.cookies_file}")
    if settings.cookies_file and not os.access(settings.cookies_file, os.R_OK):
        raise RuntimeError(f"COOKIES_FILE is not readable: {settings.cookies_file}")
    for executable, version_flag in (("ffmpeg", "-version"), ("node", "--version")):
        path = shutil.which(executable)
        if not path:
            raise RuntimeError(f"required executable is unavailable: {executable}")
        subprocess.run(
            [path, version_flag],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        LOGGER.info("preflight executable=%s path=%s", executable, path)
    LOGGER.info("preflight yt-dlp version=%s", yt_dlp.version.__version__)
    identity = instance.get_me()
    LOGGER.info("preflight Telegram getMe ok bot_id=%s username=%s", identity.id, identity.username)


def _maintenance_loop() -> None:
    last_cleanup = 0.0
    try:
        while not stop_event.wait(25):
            if not _heartbeat():
                break
            try:
                now = time.monotonic()
                if now - last_cleanup >= 300:
                    _cleanup_pending()
                    cleanup_stale_directories(_output_folder(), _settings().job_timeout_seconds * 2)
                    last_cleanup = now
            except Exception:
                LOGGER.exception("periodic maintenance failed")
    finally:
        maintenance_finished.set()


def _heartbeat() -> bool:
    workers_alive = bool(worker_threads) and all(thread.is_alive() for thread in worker_threads)
    if stop_event.is_set() or maintenance_finished.is_set() or not workers_alive:
        HEALTH_MARKER.unlink(missing_ok=True)
        if not stop_event.is_set() and not workers_alive:
            LOGGER.critical("required download worker terminated; application is unhealthy")
            fatal_lifecycle_error.set()
            if not worker_failure_alerted.is_set():
                worker_failure_alerted.set()
                _notify_operator_critical("Critical lifecycle failure\nA download worker terminated")
            stop_event.set()
            if bot is not None:
                try:
                    bot.stop_polling()
                except Exception:
                    LOGGER.exception("failed to stop polling after worker failure")
        return False
    if time.monotonic() - last_successful_poll > POLL_STALE_SECONDS:
        # Keep running: polling recovers on its own once Telegram answers again.
        if not poll_stale.is_set():
            poll_stale.set()
            LOGGER.warning(
                "Telegram getUpdates has not succeeded for %ss; reporting unhealthy",
                POLL_STALE_SECONDS,
            )
        HEALTH_MARKER.unlink(missing_ok=True)
        return True
    if poll_stale.is_set():
        poll_stale.clear()
        LOGGER.info("Telegram getUpdates succeeds again; reporting healthy")
    HEALTH_MARKER.touch()
    return True


def _record_successful_polls(instance: telebot.TeleBot) -> None:
    get_updates = instance.get_updates

    def get_updates_and_record(*args: Any, **kwargs: Any) -> Any:
        global last_successful_poll
        updates = get_updates(*args, **kwargs)
        last_successful_poll = time.monotonic()
        return updates

    instance.get_updates = get_updates_and_record  # type: ignore[method-assign]


def startup(settings: Settings) -> telebot.TeleBot:
    global SETTINGS, bot, WORKERS, PENDING_TTL_SEC, MAX_SEND_BYTES
    global YTDLP_CONCURRENT_FRAGMENTS, jobs_q, upload_slots, maintenance_thread
    global metadata_slots, metadata_executor, last_successful_poll

    HEALTH_MARKER.unlink(missing_ok=True)
    SETTINGS = settings
    WORKERS = settings.workers
    PENDING_TTL_SEC = settings.pending_ttl_seconds
    MAX_SEND_BYTES = settings.max_filesize
    YTDLP_CONCURRENT_FRAGMENTS = settings.concurrent_fragments
    jobs_q = queue.Queue(maxsize=settings.max_queue)
    upload_slots = threading.BoundedSemaphore(settings.upload_workers)
    metadata_slots = threading.BoundedSemaphore(settings.metadata_workers)
    stop_event.clear()
    maintenance_finished.clear()
    fatal_lifecycle_error.clear()
    worker_failure_alerted.clear()

    _set_pot_base_url(settings.ytdlp_pot_base_url)
    if settings.telegram_api_url:
        # Local Telegram Bot API server: uploads by file path, up to 2 GB.
        telebot.apihelper.API_URL = settings.telegram_api_url + "/bot{0}/{1}"
        telebot.apihelper.FILE_URL = settings.telegram_api_url + "/file/bot{0}/{1}"
        LOGGER.info("using local Telegram Bot API server")
    bot = telebot.TeleBot(settings.token, threaded=True)
    register_handlers(bot)
    _preflight(settings, bot)
    # getMe in the preflight proves the token and the network; polling keeps it fresh.
    _record_successful_polls(bot)
    last_successful_poll = time.monotonic()
    poll_stale.clear()
    cleanup_stale_directories(settings.output_dir, settings.job_timeout_seconds * 2)
    metadata_executor = ThreadPoolExecutor(
        max_workers=settings.metadata_workers,
        thread_name_prefix="metadata",
    )

    worker_threads.clear()
    for index in range(settings.workers):
        thread = threading.Thread(target=_worker_loop, name=f"download-worker-{index + 1}", daemon=False)
        thread.start()
        worker_threads.append(thread)
    maintenance_thread = threading.Thread(target=_maintenance_loop, name="maintenance", daemon=False)
    maintenance_thread.start()
    if not maintenance_thread.is_alive() or not _heartbeat():
        stop()
        raise RuntimeError("application lifecycle threads failed to start")
    LOGGER.info("application started workers=%s max_queue=%s", settings.workers, settings.max_queue)
    return bot


def stop() -> None:
    global metadata_executor
    HEALTH_MARKER.unlink(missing_ok=True)
    stop_event.set()
    with state_lock:
        events = list(cancel_events.values())
    for event in events:
        event.set()
    if bot is not None:
        try:
            bot.stop_bot()
        except Exception:
            LOGGER.exception("failed to stop Telegram bot worker pool")
    for _ in worker_threads:
        try:
            jobs_q.put(None, timeout=5)
        except queue.Full:
            LOGGER.error("could not enqueue worker shutdown sentinel")
    for thread in worker_threads:
        thread.join(timeout=25)
        if thread.is_alive():
            LOGGER.error("worker did not stop gracefully name=%s", thread.name)
    if maintenance_thread is not None:
        maintenance_thread.join(timeout=5)
    if metadata_executor is not None:
        metadata_executor.shutdown(wait=False, cancel_futures=True)
        metadata_executor = None
    HEALTH_MARKER.unlink(missing_ok=True)
    LOGGER.info("application stopped")


def run() -> None:
    if bot is None:
        raise RuntimeError("application is not started")
    bot.infinity_polling()


def main() -> None:
    settings = load_settings()
    configure_logging(settings.logs_dir, settings.log_level)
    instance: telebot.TeleBot | None = None
    try:
        instance = startup(settings)

        def _signal_handler(signum, _frame) -> None:
            LOGGER.info("received signal=%s", signum)
            if instance is not None:
                instance.stop_polling()

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
        run()
        if fatal_lifecycle_error.is_set():
            raise RuntimeError("required application worker terminated")
    except Exception:
        LOGGER.exception("application terminated unexpectedly")
        raise
    finally:
        if instance is not None:
            stop()


if __name__ == "__main__":
    main()
