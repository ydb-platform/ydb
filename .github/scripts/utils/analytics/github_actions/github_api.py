"""GitHub REST GET with retry. No YDB imports."""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

RETRYABLE_STATUS = frozenset({429, 502, 503, 504})


def _error_snippet(exc: HTTPError) -> str:
    try:
        return exc.read()[:300].decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        return str(exc)


def _should_retry(exc: HTTPError, snippet: str) -> bool:
    if exc.code in RETRYABLE_STATUS:
        return True
    if exc.code != 403:
        return False
    if "rate limit" in snippet.lower():
        return True
    retry_after = exc.headers.get("Retry-After") if exc.headers else None
    return bool(retry_after)


def github_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    token: Optional[str] = None,
    retries: int = 5,
    timeout: int = 60,
) -> Any:
    token = token or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN environment variable is required")
    query = urlencode({key: value for key, value in (params or {}).items() if value is not None})
    full = f"{url}?{query}" if query else url
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    backoff = 2.0
    last_error: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            request = Request(full, headers=headers)
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            last_error = exc
            snippet = _error_snippet(exc)
            if _should_retry(exc, snippet) and attempt < retries:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                sleep_for = float(retry_after) if retry_after and str(retry_after).isdigit() else backoff
                time.sleep(sleep_for)
                backoff = min(backoff * 2, 30)
                continue
            raise RuntimeError(f"GitHub API {exc.code} for {url}: {snippet}") from exc
        except (URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    raise RuntimeError(f"GitHub API request failed for {url}: {last_error}")
