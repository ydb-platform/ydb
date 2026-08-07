"""HTTP helpers for sandbox / Allure (OAuth on sandbox hosts)."""

from __future__ import annotations

import json
import urllib.request
from typing import Any
from urllib.parse import urlparse

from .yav import sandbox_oauth_token

SANDBOX_HOSTS = frozenset(
    {
        "proxy.sandbox.yandex-team.ru",
        "sandbox.yandex-team.ru",
    }
)


def needs_oauth(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return False
    return host in SANDBOX_HOSTS or host.endswith(".sandbox.yandex-team.ru")


def fetch_bytes(url: str, *, timeout: float = 45.0, oauth: str | None = None) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "ydb-perf-duty-agent/1.0"},
    )
    token = oauth if oauth is not None else sandbox_oauth_token()
    if token and needs_oauth(url):
        req.add_header("Authorization", f"OAuth {token}")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_url(url: str, *, timeout: float = 45.0, oauth: str | None = None) -> str:
    raw = fetch_bytes(url, timeout=timeout, oauth=oauth)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1", errors="replace")


def fetch_json(url: str, *, timeout: float = 45.0, oauth: str | None = None) -> Any:
    return json.loads(fetch_url(url, timeout=timeout, oauth=oauth))
