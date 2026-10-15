"""Small helpers for what Telegram shows with a sent file: caption, size, thumbnail."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

CAPTION_LIMIT = 1024


def fmt_duration(seconds: Any) -> str | None:
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or seconds <= 0:
        return None
    total = int(seconds)
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}" if hours else f"{minutes}:{secs:02d}"


def build_caption(title: str, duration: Any = None) -> str:
    text = (title or "").strip()
    shown = fmt_duration(duration)
    if shown:
        text = f"{text}\n⏱ {shown}" if text else f"⏱ {shown}"
    return text[:CAPTION_LIMIT]


def probe_video_size(path: str | os.PathLike, timeout: float = 20.0) -> tuple[int, int] | None:
    """(width, height) of the first video stream, or None."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "json", os.fspath(path)],
            check=True, capture_output=True, text=True, timeout=timeout,
        )
        stream = (json.loads(result.stdout).get("streams") or [{}])[0]
        width, height = int(stream.get("width") or 0), int(stream.get("height") or 0)
    except (OSError, subprocess.SubprocessError, ValueError, json.JSONDecodeError):
        return None
    return (width, height) if width > 0 and height > 0 else None


def make_thumbnail(video: str | os.PathLike, out: str | os.PathLike, duration: Any = None,
                   timeout: float = 30.0) -> str | None:
    """A <=320px JPEG frame taken from the video itself (what Telegram wants), or None."""
    at = 1.0
    if isinstance(duration, (int, float)) and not isinstance(duration, bool) and duration > 0:
        at = max(0.0, min(duration * 0.1, duration - 0.5, 30.0))
    try:
        subprocess.run(
            ["ffmpeg", "-nostdin", "-v", "error", "-y", "-ss", f"{at:.2f}", "-i", os.fspath(video),
             "-frames:v", "1", "-vf", "scale='min(320,iw)':-2", "-q:v", "5", os.fspath(out)],
            check=True, capture_output=True, timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    path = Path(out)
    if path.is_file() and 0 < path.stat().st_size <= 200 * 1024:
        return os.fspath(path)
    path.unlink(missing_ok=True)
    return None
