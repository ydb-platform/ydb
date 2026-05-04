"""
Test failure classification for CI analytics.

In each **test run row** (``test_results``), ``error_type`` may already be e.g. ``TIMEOUT``,
``XFAILED``, ``NOT_LAUNCHED`` — but **not** ``VERIFY``. ``VERIFY`` is detected only from text
(snippet, stderr, log) and written when we classify for storage / summaries.

Pipeline (shared by ``test_history_fast`` and ``generate-summary``):

1. Build a :class:`FailureRow` (status, snippet, ``error_type`` from the run row, stderr/log URLs).
2. Prefetch stderr/log only where :func:`should_prefetch_debug_files_for_verify` is true, via
   :func:`prefetch_text_cache_and_urls_for_failure_rows` or :func:`prefetch_texts_by_urls`.
3. :func:`classify_failure_row` → ``TIMEOUT`` | ``XFAILED`` | ``VERIFY`` | ``SANITIZER`` | ``""``.
4. :func:`merge_classified_error_type`: if the classifier returned a tag, use it; otherwise keep
   ``error_type`` from the run row (and drop blacklisted values such as ``REGULAR``).

Low-level helpers (:func:`classify_error_type`, HTTP helpers) remain for tests or custom callers.
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_FETCH_TIMEOUT_SEC = 5
DEFAULT_FETCH_MAX_BYTES = 1024 * 1024
_DEFAULT_FETCH_MAX_WORKERS = 30
DEFAULT_FETCH_MAX_ATTEMPTS = 3
DEFAULT_FETCH_RETRY_DELAY_SEC = 0.5

# After classify+merge: drop these tags from stored error_type (case-insensitive). Keep NOT_LAUNCHED etc.
_ERROR_TYPE_BLACKLIST = frozenset({"REGULAR"})

# ---------------------------------------------------------------------------
# Text and URL helpers
# ---------------------------------------------------------------------------


def _normalize_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    return str(value)


def normalize_fetch_url(url):
    """Utf8 cells from YDB may be bytes; urlopen requires str."""
    return _normalize_text(url).strip()


# ---------------------------------------------------------------------------
# Merge classifier output with ``error_type`` from the test run row (storage policy)
# ---------------------------------------------------------------------------


def _apply_error_type_blacklist(merged_error_type):
    t = _normalize_text(merged_error_type).strip()
    if not t:
        return ""
    if t.upper() in _ERROR_TYPE_BLACKLIST:
        return ""
    return t


def merge_classified_error_type(classified, source_error_type):
    """
    Persisted ``error_type``: non-empty classifier result wins; else ``error_type`` from the
    test run row (``test_results``). That column does not carry VERIFY (VERIFY comes only from
    :func:`classify_failure_row`). Values in :data:`_ERROR_TYPE_BLACKLIST` are stored as empty.

    ``source_error_type`` is the run row's ``error_type`` before merge.
    """
    if classified:
        merged = classified
    else:
        merged = _normalize_text(source_error_type).strip()
    return _apply_error_type_blacklist(merged)


# ---------------------------------------------------------------------------
# Tag heuristics (also used outside this module, e.g. HTML templates)
# ---------------------------------------------------------------------------


def is_sanitizer_issue(error_text):
    """
    Detect if a test failure is caused by a sanitizer.
    Returns True if the error text contains sanitizer-specific patterns.
    """
    error_text = _normalize_text(error_text)
    if not error_text:
        return False

    sanitizer_patterns = [
        r'(ERROR|WARNING|SUMMARY): (AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)',
        r'==\d+==\s*(ERROR|WARNING|SUMMARY): (AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)',
        r'runtime error:',
        r'==\d+==.*runtime error:',
        r'detected memory leaks',
        r'==\d+==.*detected memory leaks',
    ]

    for pattern in sanitizer_patterns:
        if re.search(pattern, error_text, re.IGNORECASE | re.MULTILINE):
            return True

    return False


def is_timeout_issue(source_error_type):
    return _normalize_text(source_error_type).upper() == "TIMEOUT"


def is_xfailed_issue(source_error_type):
    """Expected failure marker in ``error_type`` (same field as TIMEOUT); used in classify_error_type."""
    return _normalize_text(source_error_type).upper() == "XFAILED"


def is_verify_issue(error_text):
    error_text = _normalize_text(error_text)
    if not error_text:
        return False

    return bool(re.search(r'\bVERIFY\s+failed\b', error_text, re.IGNORECASE))


def is_verify_classification(status_description=None, stderr_text=None, log_text=None):
    """
    Detect VERIFY only from text: ``status_description`` first, then fetched ``stderr_text``,
    then ``log_text``. ``stderr_text`` / ``log_text``: None means URL was absent or not fetched;
    otherwise body string (may be empty after fetch).
    """
    if is_verify_issue(status_description):
        return True
    if stderr_text is not None and is_verify_issue(stderr_text):
        return True
    if log_text is not None and is_verify_issue(log_text):
        return True
    return False


def is_not_launched_issue(source_error_type, status_name=None):
    if _normalize_text(source_error_type).upper() != "NOT_LAUNCHED":
        return False

    return _normalize_text(status_name).upper() in ("SKIP", "SKIPPED", "MUTE")


# ---------------------------------------------------------------------------
# Core classification (status + fields → tag string)
# ---------------------------------------------------------------------------


def is_failure_like_status(status):
    """Statuses for which we classify ``error_type`` (aligned with ``test_results`` / HTML summary)."""
    return _normalize_text(status).strip().lower() in ("failure", "mute", "error")


def _classify_failure_branch(status, status_description, source_error_type, stderr_text=None, log_text=None):
    """TIMEOUT/XFAILED from the run row's ``error_type``; VERIFY only from text; SANITIZER heuristic on snippet."""
    if is_timeout_issue(source_error_type):
        return "TIMEOUT"
    if is_xfailed_issue(source_error_type):
        return "XFAILED"
    if is_verify_classification(status_description, stderr_text, log_text):
        return "VERIFY"
    if is_sanitizer_issue(status_description):
        return "SANITIZER"
    return ""


def classify_error_type(status, status_description, source_error_type, stderr_text=None, log_text=None):
    if not is_failure_like_status(status):
        return ""
    return _classify_failure_branch(status, status_description, source_error_type, stderr_text, log_text)


# ---------------------------------------------------------------------------
# Prefetch: when to fetch, which URLs, read from cache
# ---------------------------------------------------------------------------


def should_prefetch_debug_files_for_verify(status, status_description, source_error_type):
    """True if stderr/log fetch might reveal VERIFY (snippet alone does not finish classification)."""
    if not is_failure_like_status(status):
        return False
    return _classify_failure_branch(status, status_description, source_error_type, None, None) == ""


def urls_for_debug_prefetch_if_needed(need_prefetch, stderr_url, log_url):
    """Raw stderr and log URLs to download when prefetch is required for this row."""
    if not need_prefetch:
        return []
    out = []
    if normalize_fetch_url(stderr_url):
        out.append(stderr_url)
    if normalize_fetch_url(log_url):
        out.append(log_url)
    return out


def debug_file_texts_from_cache(need_prefetch, stderr_url, log_url, fetch_cache):
    """
    Map stderr/log URLs to fetched text. ``None`` for a component means URL absent or fetch failed;
    empty string means successful fetch with empty body.
    """
    if not need_prefetch:
        return None, None
    se = normalize_fetch_url(stderr_url)
    stderr_text = fetch_cache.get(se) if se else None
    lg = normalize_fetch_url(log_url)
    log_text = fetch_cache.get(lg) if lg else None
    return stderr_text, log_text


# ---------------------------------------------------------------------------
# HTTP: download log bodies into a URL → text cache
# ---------------------------------------------------------------------------


def fetch_text_by_url(
    url,
    timeout_sec=DEFAULT_FETCH_TIMEOUT_SEC,
    max_bytes=DEFAULT_FETCH_MAX_BYTES,
    max_attempts=DEFAULT_FETCH_MAX_ATTEMPTS,
    retry_delay_sec=DEFAULT_FETCH_RETRY_DELAY_SEC,
):
    """Return response text, empty string for empty body, or ``None`` if the URL is invalid or fetch failed."""
    url = normalize_fetch_url(url)
    if not url:
        return None

    for attempt in range(max_attempts):
        try:
            with urllib_request.urlopen(url, timeout=timeout_sec) as response:
                data = response.read(max_bytes + 1)
            return data[:max_bytes].decode("utf-8", errors="replace")
        except (urllib_error.URLError, TimeoutError, ValueError, OSError):
            if attempt < max_attempts - 1:
                time.sleep(retry_delay_sec)
            continue

    return None


def prefetch_texts_by_urls(urls, existing_cache=None, max_workers=_DEFAULT_FETCH_MAX_WORKERS):
    cache = existing_cache if existing_cache is not None else {}
    seen = set()
    unique_urls = []
    for raw in urls:
        url = normalize_fetch_url(raw)
        if not url or url in cache or url in seen:
            continue
        seen.add(url)
        unique_urls.append(url)
    if not unique_urls:
        return cache

    workers = min(max_workers, len(unique_urls))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_url = {pool.submit(fetch_text_by_url, url): url for url in unique_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                cache[url] = future.result()
            except Exception:
                cache[url] = None
    return cache


# ---------------------------------------------------------------------------
# FailureRow: normalized row + per-row classification
# ---------------------------------------------------------------------------


@dataclass
class FailureRow:
    """Normalized inputs for failure classification (YDB row or HTML summary test)."""

    status: Any
    status_description: Any
    # error_type from test_results / report row (TIMEOUT, XFAILED, …); VERIFY is inferred from text only
    source_error_type: Any
    stderr_url: Any
    log_url: Any


def failure_row_from_ydb(row: Dict[str, Any]) -> FailureRow:
    return FailureRow(
        status=row.get("status"),
        status_description=row.get("status_description"),
        source_error_type=row.get("error_type"),
        stderr_url=row.get("stderr"),
        log_url=row.get("log"),
    )


def failure_row_from_test_result(test: Any, status_str: str) -> FailureRow:
    """``status_str`` must be failure|error|mute tokens used by :func:`classify_error_type`."""
    return FailureRow(
        status=status_str,
        status_description=getattr(test, "status_description", None),
        source_error_type=getattr(test, "error_type", None),
        stderr_url=getattr(test, "stderr_url", None),
        log_url=getattr(test, "log_url", None),
    )


def classify_failure_row(row: FailureRow, fetch_cache: Optional[Dict[str, Any]]) -> str:
    """
    Full classification for one failure using optional prefetch cache (same dict as
    :func:`prefetch_texts_by_urls`). Pass ``{}`` if nothing was fetched.
    """
    cache = fetch_cache if fetch_cache is not None else {}
    need = should_prefetch_debug_files_for_verify(
        row.status, row.status_description, row.source_error_type
    )
    stderr_text, log_text = debug_file_texts_from_cache(
        need, row.stderr_url, row.log_url, cache
    )
    return classify_error_type(
        row.status,
        row.status_description,
        row.source_error_type,
        stderr_text=stderr_text,
        log_text=log_text,
    )


# ---------------------------------------------------------------------------
# Batch prefetch for many FailureRow instances
# ---------------------------------------------------------------------------


def prefetch_text_cache_and_urls_for_failure_rows(
    failure_rows: Sequence[FailureRow],
    existing_cache: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[Any]]:
    """
    Merge stderr/log URLs for all rows, run :func:`prefetch_texts_by_urls`, return ``(cache, url_list)``.
    Ignore ``url_list`` if you only need the cache: ``cache, _ = prefetch_text_cache_and_urls_for_failure_rows(...)``.
    """
    urls = []
    for fr in failure_rows:
        urls.extend(
            urls_for_debug_prefetch_if_needed(
                should_prefetch_debug_files_for_verify(
                    fr.status, fr.status_description, fr.source_error_type
                ),
                fr.stderr_url,
                fr.log_url,
            )
        )
    return prefetch_texts_by_urls(urls, existing_cache=existing_cache), urls
