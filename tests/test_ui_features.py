from __future__ import annotations

import shutil
import subprocess
from types import SimpleNamespace

import pytest

import main
from app.media_extras import build_caption, fmt_duration, make_thumbnail, probe_video_size
from app.models import VideoFormatCandidate
from app.planner import PLAYLIST_LIST_LIMIT


def _cand(label: str, size: int = 10) -> VideoFormatCandidate:
    return VideoFormatCandidate(
        format_spec=label, merge_output_format=None, estimated_size=size,
        estimated_confident=True, quality_label=label, compatibility=2, direct_urls=(),
    )


def test_quality_choices_one_button_per_distinct_quality() -> None:
    cands = [_cand("1080p"), _cand("1080p"), _cand("720p"), _cand("480p"), _cand("")]
    assert main._quality_choices(cands) == [(0, "1080p"), (2, "720p"), (3, "480p")]
    assert main._quality_choices([_cand("720p")]) == [(0, "720p")]


def test_caption_has_title_and_duration_and_respects_limit() -> None:
    assert fmt_duration(65) == "1:05" and fmt_duration(3725) == "1:02:05" and fmt_duration(0) is None
    assert build_caption("Title", 65) == "Title\n⏱ 1:05"
    assert build_caption("Title") == "Title"
    assert len(build_caption("x" * 5000, 10)) == 1024


@pytest.mark.skipif(not shutil.which("ffmpeg"), reason="ffmpeg required")
def test_thumbnail_and_size_come_from_the_video(tmp_path) -> None:
    video = tmp_path / "v.mp4"
    subprocess.run(
        ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=640x360:rate=10:duration=3",
         "-pix_fmt", "yuv420p", str(video)],
        check=True,
    )
    assert probe_video_size(video) == (640, 360)
    thumb = make_thumbnail(video, tmp_path / "t.jpg", duration=3)
    assert thumb is not None and (tmp_path / "t.jpg").stat().st_size <= 200 * 1024
    assert make_thumbnail(tmp_path / "missing.mp4", tmp_path / "x.jpg") is None


def test_playlist_detection_and_entry_urls() -> None:
    meta = {"_type": "playlist", "entries": [
        {"url": "https://www.youtube.com/watch?v=a"},
        {"webpage_url": "https://www.youtube.com/watch?v=b", "url": "x"},
        {"url": "https://www.youtube.com/watch?v=a"},
        {"url": "javascript:alert(1)"},
        "junk",
    ]}
    assert main._is_playlist_meta(meta)
    assert not main._is_playlist_meta({"formats": [{}], "_type": "video"})
    assert main._playlist_entry_urls(meta, 10) == [
        "https://www.youtube.com/watch?v=a", "https://www.youtube.com/watch?v=b"]
    assert main._playlist_entry_urls(meta, 1) == ["https://www.youtube.com/watch?v=a"]
    assert PLAYLIST_LIST_LIMIT >= 10


def test_run_playlist_queues_each_item_and_reports_failures(monkeypatch) -> None:
    sent_status: list[str] = []
    queued: list[tuple[str, str]] = []
    counter = iter(range(100, 200))
    monkeypatch.setattr(main.time, "sleep", lambda _s: None)
    monkeypatch.setattr(main, "_safe_send_message",
                        lambda chat, text, reply_to_message_id=None, reply_markup=None:
                        SimpleNamespace(message_id=next(counter)))
    monkeypatch.setattr(main, "_safe_edit", lambda chat, mid, text, **kw: sent_status.append(text))
    monkeypatch.setattr(main, "validate_public_url", lambda u: u)

    def fake_meta(url):
        if url.endswith("bad"):
            raise RuntimeError("boom")
        if url.endswith("list"):
            return {"_type": "playlist", "entries": [{"url": "https://x/1"}]}
        return {"title": "Vid"}

    monkeypatch.setattr(main, "_run_metadata_operation", fake_meta)
    monkeypatch.setattr(main, "_build_video_candidates", lambda meta, limit: [_cand("720p")])
    monkeypatch.setattr(main, "_build_audio_plan_mp3", lambda meta, limit: (None, "no"))
    monkeypatch.setattr(main, "_enqueue_job", lambda **kw: queued.append((kw["url"], kw["mode"])) or True)

    main._run_playlist(1, 2, 3, ["https://x/ok", "https://x/bad", "https://x/list"], "video")
    assert queued == [("https://x/ok", "video")]
    assert any("تعذّر" in t for t in sent_status) and any("قائمة" in t for t in sent_status)

    queued.clear(); sent_status.clear()
    main._run_playlist(1, 2, 3, ["https://x/ok"], "audio")  # no audio plan -> skipped, not queued
    assert queued == [] and any("الصوت" in t for t in sent_status)


def test_chosen_quality_goes_first_and_multipart_carries_thumbnail(tmp_path, monkeypatch) -> None:
    import threading, time
    from pathlib import Path
    from app.models import PendingRequest
    from test_download_candidate_fallback import _settings as base_settings

    monkeypatch.setattr(main, "SETTINGS", base_settings(tmp_path))
    monkeypatch.setattr(main, "_safe_answer_callback", lambda *a, **k: None)
    monkeypatch.setattr(main, "_safe_edit", lambda *a, **k: None)
    cands = (_cand("1080p"), _cand("720p"), _cand("480p"))
    main.pending_requests["rid"] = PendingRequest(
        created_at=time.time(), user_id=7, chat_id=1, reply_to_message_id=2, url="https://x/v",
        title="T", video_candidates=cands, audio_plan=None, metadata={})
    call = SimpleNamespace(id="c", data="dl|video|rid|2", from_user=SimpleNamespace(id=7),
                           message=SimpleNamespace(message_id=9))
    main.jobs_q = main.queue.Queue(maxsize=5)
    main.on_download_choice(call)
    job = main.jobs_q.get_nowait()
    assert [c.quality_label for c in job.video_candidates] == ["480p", "1080p", "720p"]

    # multipart upload (cloud API) includes the thumbnail file and caption fields
    seen = {}

    class FakeResp:
        status_code = 200
        def json(self): return {"ok": True}

    class FakeSession:
        def post(self, url, data=None, headers=None, timeout=None):
            seen["body"] = data.read() if hasattr(data, "read") else b""
            seen["ct"] = headers["Content-Type"]
            return FakeResp()
        def close(self): pass

    monkeypatch.setattr(main, "telegram_upload_session", lambda: FakeSession())
    monkeypatch.setattr(main, "_check_job", lambda *a, **k: None)
    video, thumb = tmp_path / "a.mp4", tmp_path / "t.jpg"
    video.write_bytes(b"V" * 50); thumb.write_bytes(b"\xff\xd8JPEG")
    main._send_via_bot_api_with_progress(
        "j", 1, 2, 3, "T", "sendVideo", "video", str(video), "T.mp4", "x",
        {"caption": "T\n⏱ 1:05", "duration": 65}, time.monotonic() + 30, thumb_path=str(thumb))
    body = seen["body"]
    assert b'name="thumbnail"' in body and b"JPEG" in body and b'name="caption"' in body
    assert b'name="duration"' in body
