"""GitHub REST GET with retry and a rate-limit budget. No YDB imports."""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

RETRYABLE_STATUS = frozenset({429, 502, 503, 504})

# Stop while there is still headroom, so a caller that has to finish a few more
# requests to leave its state consistent can do so.
RATE_LIMIT_RESERVE = 50


class RateLimitExhausted(RuntimeError):
    """Raised when the token's remaining request budget hits the reserve.

    Callers must treat this as "stop and keep the watermark", not as a per-request
    failure: continuing would silently export a partial window.
    """

    def __init__(self, remaining: int, reset_at: Optional[int]) -> None:
        super().__init__(
            f"GitHub rate limit budget exhausted (remaining={remaining}, reset_at={reset_at})"
        )
        self.remaining = remaining
        self.reset_at = reset_at


class NotFound(RuntimeError):
    """GitHub 404: the run or job is gone, not a retryable miss."""


def _int_header(headers: Any, name: str) -> Optional[int]:
    if not headers:
        return None
    raw = headers.get(name)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _error_snippet(exc: HTTPError) -> str:
    try:
        return exc.read()[:300].decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        return str(exc)


def _is_rate_limited(exc: HTTPError, snippet: str) -> bool:
    """True only for a primary rate limit, where the budget really is zero.

    Secondary (abuse) limits keep a non-zero remaining count and carry
    Retry-After, so they stay on the ordinary retry path.
    """
    if exc.code not in (403, 429):
        return False
    remaining = _int_header(exc.headers, "x-ratelimit-remaining")
    return remaining is not None and remaining <= 0


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
                payload = json.loads(response.read().decode("utf-8"))
            remaining = _int_header(response.headers, "x-ratelimit-remaining")
            if remaining is not None and remaining <= RATE_LIMIT_RESERVE:
                raise RateLimitExhausted(remaining, _int_header(response.headers, "x-ratelimit-reset"))
            return payload
        except HTTPError as exc:
            last_error = exc
            snippet = _error_snippet(exc)
            if exc.code == 404:
                raise NotFound(f"GitHub API 404 for {url}: {snippet}") from exc
            if _is_rate_limited(exc, snippet):
                raise RateLimitExhausted(
                    _int_header(exc.headers, "x-ratelimit-remaining") or 0,
                    _int_header(exc.headers, "x-ratelimit-reset"),
                ) from exc
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
