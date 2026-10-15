from __future__ import annotations

import os

import pytest

from app.planner import apply_generic_impersonation_opts, apply_youtube_runtime_opts
from app.settings import load_settings


def _clean(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "BOT_TOKEN",
        "LOGS_CHAT_ID",
        "OUTPUT_FOLDER",
        "LOGS_DIR",
        "MAX_FILESIZE",
        "WORKERS",
        "MAX_QUEUE",
        "UPLOAD_WORKERS",
        "JOB_TIMEOUT_SECONDS",
        "PENDING_TTL_SECONDS",
        "YTDLP_CONCURRENT_FRAGMENTS",
        "COOKIES_FILE",
        "LOG_LEVEL",
        "YTDLP_JS_RUNTIMES",
        "YTDLP_REMOTE_COMPONENTS",
        "YTDLP_YOUTUBE_PLAYER_CLIENTS",
        "YTDLP_INSTAGRAM_IMPERSONATE",
        "YTDLP_INSTAGRAM_RETRIES",
        "YTDLP_INSTAGRAM_FRAGMENT_RETRIES",
        "YTDLP_INSTAGRAM_SOCKET_TIMEOUT",
        "YTDLP_GENERIC_IMPERSONATE",
        "METADATA_WORKERS",
        "METADATA_TIMEOUT_SECONDS",
    ):
        monkeypatch.delenv(key, raising=False)


def test_invalid_configuration_is_rejected(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clean(monkeypatch)
    monkeypatch.setenv("BOT_TOKEN", "123:test-token-value-abcdefghijklmnop")
    monkeypatch.setenv("WORKERS", "0")
    with pytest.raises(RuntimeError, match="WORKERS"):
        load_settings(tmp_path)


def test_operational_configuration(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    _clean(monkeypatch)
    monkeypatch.setenv("BOT_TOKEN", "123:test-token-value-abcdefghijklmnop")
    monkeypatch.setenv("MAX_QUEUE", "7")
    settings = load_settings(tmp_path)
    assert settings.max_queue == 7
    assert settings.max_filesize == 50 * 1024 * 1024
    assert os.fspath(settings.output_dir).endswith(os.fspath(tmp_path / "data" / "downloads"))


def test_ytdlp_and_metadata_settings_are_loaded_from_local_env(tmp_path, monkeypatch) -> None:
    _clean(monkeypatch)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "BOT_TOKEN=123:test-token-value-abcdefghijklmnop",
                "YTDLP_JS_RUNTIMES=node:/custom/node",
                "YTDLP_REMOTE_COMPONENTS=",
                "YTDLP_YOUTUBE_PLAYER_CLIENTS=default,android,ios",
                "YTDLP_INSTAGRAM_IMPERSONATE=",
                "YTDLP_INSTAGRAM_RETRIES=3",
                "YTDLP_INSTAGRAM_FRAGMENT_RETRIES=4",
                "YTDLP_INSTAGRAM_SOCKET_TIMEOUT=25",
                "YTDLP_GENERIC_IMPERSONATE=chrome",
                "METADATA_WORKERS=3",
                "METADATA_TIMEOUT_SECONDS=45",
            ]
        ),
        encoding="utf-8",
    )

    settings = load_settings(tmp_path)

    assert settings.ytdlp_js_runtimes == "node:/custom/node"
    assert settings.ytdlp_remote_components == ""
    assert settings.ytdlp_youtube_player_clients == "default,android,ios"
    assert settings.ytdlp_instagram_impersonate is None
    assert settings.ytdlp_instagram_retries == 3
    assert settings.ytdlp_instagram_fragment_retries == 4
    assert settings.ytdlp_instagram_socket_timeout == 25
    assert settings.ytdlp_generic_impersonate == "chrome"
    assert settings.metadata_workers == 3
    assert settings.metadata_timeout_seconds == 45
    options = apply_youtube_runtime_opts(
        {},
        "https://www.youtube.com/watch?v=example",
        settings.ytdlp_js_runtimes,
        settings.ytdlp_remote_components,
        player_client="android",
        youtube_language="ru",
    )
    assert "remote_components" not in options
    assert options["format_sort"] == ["lang"]
    assert options["format_sort_force"] is True
    assert options["extractor_args"] == {"youtube": {"player_client": ["android"], "lang": ["ru"]}}
    generic_options = apply_generic_impersonation_opts(
        {},
        "https://example.com/video",
        settings.ytdlp_generic_impersonate,
    )
    assert "impersonate" in generic_options
    assert "impersonate" not in apply_generic_impersonation_opts(
        {},
        "https://www.youtube.com/watch?v=example",
        settings.ytdlp_generic_impersonate,
    )


def test_invalid_metadata_configuration_is_rejected(tmp_path, monkeypatch) -> None:
    _clean(monkeypatch)
    monkeypatch.setenv("BOT_TOKEN", "123:test-token-value-abcdefghijklmnop")
    monkeypatch.setenv("METADATA_WORKERS", "0")
    with pytest.raises(RuntimeError, match="METADATA_WORKERS"):
        load_settings(tmp_path)


def test_pot_provider_and_local_api_settings(tmp_path, monkeypatch) -> None:
    from app import planner

    _clean(monkeypatch)
    monkeypatch.setenv("BOT_TOKEN", "123:test-token-value-abcdefghijklmnop")
    monkeypatch.delenv("YTDLP_POT_BASE_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_API_URL", raising=False)
    settings = load_settings(tmp_path)
    assert settings.ytdlp_pot_base_url == "" and settings.telegram_api_url == ""
    assert settings.ytdlp_youtube_player_clients == "default,tv,web_safari,mweb"

    monkeypatch.setenv("YTDLP_POT_BASE_URL", "http://bgutil-pot:4416/")
    monkeypatch.setenv("TELEGRAM_API_URL", "http://telegram-bot-api:8081")
    settings = load_settings(tmp_path)
    assert settings.ytdlp_pot_base_url == "http://bgutil-pot:4416"

    monkeypatch.setenv("TELEGRAM_API_URL", "telegram-bot-api:8081")
    with pytest.raises(RuntimeError, match="TELEGRAM_API_URL"):
        load_settings(tmp_path)

    try:
        planner.set_pot_base_url(settings.ytdlp_pot_base_url)
        options = apply_youtube_runtime_opts(
            {}, "https://www.youtube.com/watch?v=x", "node", "", player_client="mweb"
        )
        assert options["extractor_args"]["youtubepot-bgutilhttp"] == {"base_url": ["http://bgutil-pot:4416"]}
        assert options["extractor_args"]["youtube"]["player_client"] == ["mweb"]
        other = apply_youtube_runtime_opts({}, "https://vimeo.com/1", "node", "")
        assert "extractor_args" not in other
    finally:
        planner.set_pot_base_url("")


def test_cookies_from_base64_variable(tmp_path, monkeypatch) -> None:
    import base64
    import tempfile

    _clean(monkeypatch)
    monkeypatch.setenv("BOT_TOKEN", "123:test-token-value-abcdefghijklmnop")
    monkeypatch.setattr(tempfile, "gettempdir", lambda: os.fspath(tmp_path))
    raw = b"# Netscape HTTP Cookie File\n.youtube.com\tTRUE\t/\tTRUE\t2000000000\tSID\tabc\n"
    monkeypatch.setenv("COOKIES_B64", base64.b64encode(raw).decode())
    settings = load_settings(tmp_path)
    assert settings.cookies_file is not None and settings.cookies_file.read_bytes() == raw
    assert (settings.cookies_file.stat().st_mode & 0o777) == 0o600
    monkeypatch.setenv("COOKIES_B64", "!!!")
    with pytest.raises(RuntimeError, match="COOKIES_B64"):
        load_settings(tmp_path)
    monkeypatch.delenv("COOKIES_B64")
