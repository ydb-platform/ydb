"""Classify test failures: union row ``error_type`` (TIMEOUT, XFAILED, …) with VERIFY/SANITIZER from text.

VERIFY/SANITIZER use snippet + stderr + log. All applicable tags accumulate — a test can carry
multiple badges simultaneously (e.g. TIMEOUT + SANITIZER). CSV storage and HTML badges both
keep all applicable tags.

Pipeline overview
-----------------
1. Build ``FailureRow`` objects for every failure-like row (status failure|mute|error).
2. Call ``prefetch_text_cache_and_urls_for_failure_rows`` to download stderr/log in parallel.
3. For each row call ``get_debug_texts_from_cache`` to retrieve cached text.
4. Call ``build_error_type_csv_for_storage`` (DB write) or individual ``is_*_classification``
   functions (HTML badges) with the fetched texts.

Adding a new classifier
-----------------------
- Add an ``is_<name>_classification(status_description, stderr_text, log_text)`` function.
- In ``build_error_type_csv_for_storage`` add ``tags.add("<NAME>")`` where appropriate.
- In ``generate-summary.py`` add ``test.is_<name>_issue`` field and set it in the badge loop.
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
DEFAULT_PREFETCH_MAX_WORKERS = 30
# Used by test_history_fast.py --full-day-refresh which processes an order of magnitude more rows.
DEFAULT_PREFETCH_MAX_WORKERS_FULL_REFRESH = 200
DEFAULT_FETCH_MAX_ATTEMPTS = 3
DEFAULT_FETCH_RETRY_DELAY_SEC = 1.0

# After classify+merge: drop these tags from stored error_type (case-insensitive).
_ERROR_TYPE_BLACKLIST = frozenset({"REGULAR"})

# Order of known tags in comma-separated ``error_type`` written to storage.
_STORAGE_TAG_ORDER = ("TIMEOUT", "XFAILED", "NOT_LAUNCHED", "VERIFY", "SANITIZER")
_KNOWN_STORAGE_TAGS = frozenset(_STORAGE_TAG_ORDER)

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
    """YDB Utf8 cells may arrive as bytes; urlopen requires str."""
    return _normalize_text(url).strip()


def source_has_tag(source_error_type, tag: str) -> bool:
    """True if ``error_type`` is a single tag or comma-separated list containing ``tag`` (case-insensitive)."""
    want = _normalize_text(tag).strip().upper()
    if not want:
        return False
    for part in re.split(r"\s*,\s*", _normalize_text(source_error_type)):
        if part.strip().upper() == want:
            return True
    return False


# ---------------------------------------------------------------------------
# Tag heuristics (used both for storage and HTML badges)
# ---------------------------------------------------------------------------


def is_sanitizer_issue(error_text):
    """True if ``error_text`` matches sanitizer / UBSan / leak patterns."""
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
    return source_has_tag(source_error_type, "TIMEOUT")


def is_xfailed_issue(source_error_type):
    """Expected failure marker in ``error_type`` (same field as TIMEOUT)."""
    return source_has_tag(source_error_type, "XFAILED")


def is_verify_issue(error_text):
    error_text = _normalize_text(error_text)
    if not error_text:
        return False
    return bool(re.search(r'\bVERIFY\s+failed\b', error_text, re.IGNORECASE))


def is_not_launched_issue(source_error_type, status_name=None):
    if not source_has_tag(source_error_type, "NOT_LAUNCHED"):
        return False
    return _normalize_text(status_name).upper() in ("SKIP", "SKIPPED", "MUTE")


# ---------------------------------------------------------------------------
# Multi-source classification helpers (snippet → stderr → log)
#
# Each function accepts the three text sources in priority order.
# ``None`` means the source was not fetched (URL missing or fetch failed);
# ``""`` means the fetch succeeded but the file was empty.
# ---------------------------------------------------------------------------


def is_verify_classification(status_description=None, stderr_text=None, log_text=None):
    """VERIFY from snippet, then stderr, then log."""
    if is_verify_issue(status_description):
        return True
    if stderr_text is not None and is_verify_issue(stderr_text):
        return True
    if log_text is not None and is_verify_issue(log_text):
        return True
    return False


def is_sanitizer_classification(status_description=None, stderr_text=None, log_text=None):
    """Sanitizer from snippet, then stderr, then log."""
    if is_sanitizer_issue(status_description):
        return True
    if stderr_text is not None and is_sanitizer_issue(stderr_text):
        return True
    if log_text is not None and is_sanitizer_issue(log_text):
        return True
    return False


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------


def is_failure_like_status(status):
    """Statuses for which we classify ``error_type`` (aligned with ``test_results`` / HTML summary)."""
    return _normalize_text(status).strip().lower() in ("failure", "mute", "error")


# ---------------------------------------------------------------------------
# Storage formatting
# ---------------------------------------------------------------------------


def format_error_type_tags_csv(tag_set):
    """Format a set of canonical UPPER tags as a stable comma-separated string."""
    if not tag_set:
        return ""
    clean = {t.strip().upper() for t in tag_set if t and t.strip().upper() not in _ERROR_TYPE_BLACKLIST}
    if not clean:
        return ""
    out = [k for k in _STORAGE_TAG_ORDER if k in clean]
    out.extend(sorted(t for t in clean if t not in _KNOWN_STORAGE_TAGS))
    return ",".join(out)


def build_error_type_csv_for_storage(
    status,
    status_description,
    source_error_type,
    stderr_text,
    log_text,
    status_name_for_not_launched=None,
):
    """Return comma-separated tags for storage.

    Starts from ``source_error_type`` (already-known tags from the upstream row),
    then overlays text-derived tags for failure-like statuses.  Blacklisted tags
    (e.g. ``REGULAR``) are stripped from both sources.
    """
    # Seed from upstream field (covers TIMEOUT, XFAILED, and any other raw tags).
    tags = set()
    for part in re.split(r"\s*,\s*", _normalize_text(source_error_type).strip()):
        p = part.strip().upper()
        if p and p not in _ERROR_TYPE_BLACKLIST:
            tags.add(p)

    # Overlay text-derived tags only for failure-like statuses.
    if is_failure_like_status(status):
        if is_verify_classification(status_description, stderr_text, log_text):
            tags.add("VERIFY")
        if is_sanitizer_classification(status_description, stderr_text, log_text):
            tags.add("SANITIZER")
        if status_name_for_not_launched is not None and is_not_launched_issue(
            source_error_type, status_name_for_not_launched
        ):
            tags.add("NOT_LAUNCHED")

    return format_error_type_tags_csv(tags)


# ---------------------------------------------------------------------------
# FailureRow: normalised carrier for classification inputs
# ---------------------------------------------------------------------------


@dataclass
class FailureRow:
    """Inputs for failure classification: status, texts, and debug-file URLs."""

    status: Any
    status_description: Any
    # Tags from the upstream row (TIMEOUT, XFAILED, …); VERIFY/SANITIZER are inferred from text.
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
    """``status_str`` must be failure|error|mute tokens used by :func:`is_failure_like_status`."""
    return FailureRow(
        status=status_str,
        status_description=getattr(test, "status_description", None),
        source_error_type=getattr(test, "error_type", None),
        stderr_url=getattr(test, "stderr_url", None),
        log_url=getattr(test, "log_url", None),
    )


# ---------------------------------------------------------------------------
# Prefetch: batch HTTP download of stderr/log into a URL → text cache
# ---------------------------------------------------------------------------


def _urls_to_prefetch(fr: FailureRow) -> List[Any]:
    """Return the list of URLs (0, 1, or 2) that should be fetched for this row."""
    if not is_failure_like_status(fr.status):
        return []
    urls = []
    if normalize_fetch_url(fr.stderr_url):
        urls.append(fr.stderr_url)
    if normalize_fetch_url(fr.log_url):
        urls.append(fr.log_url)
    return urls


def get_debug_texts_from_cache(fr: FailureRow, fetch_cache: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Return ``(stderr_text, log_text)`` from ``fetch_cache`` for ``fr``.

    Returns ``None`` for a source when its URL is empty or was not fetched
    (non-failure status, or URL missing).  Returns ``""`` when the fetch
    succeeded but the file was empty.
    """
    if not is_failure_like_status(fr.status):
        return None, None
    se = normalize_fetch_url(fr.stderr_url)
    stderr_text = fetch_cache.get(se) if se else None
    lg = normalize_fetch_url(fr.log_url)
    log_text = fetch_cache.get(lg) if lg else None
    return stderr_text, log_text


def fetch_text_by_url(
    url,
    timeout_sec=DEFAULT_FETCH_TIMEOUT_SEC,
    max_bytes=DEFAULT_FETCH_MAX_BYTES,
    max_attempts=DEFAULT_FETCH_MAX_ATTEMPTS,
    retry_delay_sec=DEFAULT_FETCH_RETRY_DELAY_SEC,
):
    """Return response text, ``""`` for empty body, ``None`` on permanent failure."""
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

    return None


def prefetch_texts_by_urls(urls, existing_cache=None, max_workers=DEFAULT_PREFETCH_MAX_WORKERS):
    """Download ``urls`` in parallel; returns updated ``cache`` dict."""
    cache = existing_cache if existing_cache is not None else {}
    seen: set = set()
    unique_urls = []
    for raw in urls:
        url = normalize_fetch_url(raw)
        if not url or url in cache or url in seen:
            continue
        seen.add(url)
        unique_urls.append(url)
    if not unique_urls:
        return cache

    total = len(unique_urls)
    workers = min(max_workers, total)
    step = max(500, total // 20) if total > 500 else total
    print(
        f"[prefetch] {total} unique stderr/log URL(s), {workers} workers "
        f"(timeout {DEFAULT_FETCH_TIMEOUT_SEC}s each, retries {DEFAULT_FETCH_MAX_ATTEMPTS})...",
        flush=True,
    )
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_url = {pool.submit(fetch_text_by_url, url): url for url in unique_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                cache[url] = future.result()
            except Exception:
                cache[url] = None
            done += 1
            if done < total and step > 0 and done % step == 0:
                print(f"[prefetch] progress {done}/{total}", flush=True)
    print(f"[prefetch] finished {total} URL(s) in {time.time() - t0:.1f}s", flush=True)
    return cache


def prefetch_text_cache_and_urls_for_failure_rows(
    failure_rows: Sequence[FailureRow],
    existing_cache: Optional[Dict[str, Any]] = None,
    max_workers: Optional[int] = None,
) -> Tuple[Dict[str, Any], List[Any]]:
    """Collect URLs from ``failure_rows``, prefetch them, return ``(cache, url_list)``."""
    urls: List[Any] = []
    for fr in failure_rows:
        urls.extend(_urls_to_prefetch(fr))

    fetch_kw: Dict[str, Any] = {"existing_cache": existing_cache}
    if max_workers is not None:
        fetch_kw["max_workers"] = max_workers
    return prefetch_texts_by_urls(urls, **fetch_kw), urls
