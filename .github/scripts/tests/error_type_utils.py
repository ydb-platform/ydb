"""Classify test failures: union row ``error_type`` (TIMEOUT, XFAILED, …) with VERIFY/SANITIZER from text.

VERIFY/SANITIZER use snippet + stderr + log. Failure rows with stderr/log URLs prefetch both;
there is no skip-when-TIMEOUT shortcut. CSV storage and HTML badges both keep all applicable tags."""

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
DEFAULT_FETCH_MAX_ATTEMPTS = 10
DEFAULT_FETCH_RETRY_DELAY_SEC = 0.5

# After classify+merge: drop these tags from stored error_type (case-insensitive). Keep NOT_LAUNCHED etc.
_ERROR_TYPE_BLACKLIST = frozenset({"REGULAR"})

# Order of known tags in comma-separated ``error_type`` written to storage
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
    """Utf8 cells from YDB may be bytes; urlopen requires str."""
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
# Tag heuristics (also used outside this module, e.g. HTML templates)
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


def is_verify_classification(status_description=None, stderr_text=None, log_text=None):
    """VERIFY from snippet, then stderr, then log. ``None`` = missing URL or failed fetch."""
    if is_verify_issue(status_description):
        return True
    if stderr_text is not None and is_verify_issue(stderr_text):
        return True
    if log_text is not None and is_verify_issue(log_text):
        return True
    return False


def is_sanitizer_classification(status_description=None, stderr_text=None, log_text=None):
    """Same source order as :func:`is_verify_classification` but sanitizer patterns."""
    if is_sanitizer_issue(status_description):
        return True
    if stderr_text is not None and is_sanitizer_issue(stderr_text):
        return True
    if log_text is not None and is_sanitizer_issue(log_text):
        return True
    return False


def is_not_launched_issue(source_error_type, status_name=None):
    if not source_has_tag(source_error_type, "NOT_LAUNCHED"):
        return False

    return _normalize_text(status_name).upper() in ("SKIP", "SKIPPED", "MUTE")


# ---------------------------------------------------------------------------
# Core classification (status + fields → tag string)
# ---------------------------------------------------------------------------


def is_failure_like_status(status):
    """Statuses for which we classify ``error_type`` (aligned with ``test_results`` / HTML summary)."""
    return _normalize_text(status).strip().lower() in ("failure", "mute", "error")


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
    """Comma-separated tags for storage: existing ``source_error_type`` plus field + text-derived tags."""
    tags = set()
    raw = _normalize_text(source_error_type).strip()
    for part in re.split(r"\s*,\s*", raw):
        p = part.strip().upper()
        if not p or p in _ERROR_TYPE_BLACKLIST:
            continue
        tags.add(p)
    if is_failure_like_status(status):
        if is_verify_classification(status_description, stderr_text, log_text):
            tags.add("VERIFY")
        if is_sanitizer_classification(status_description, stderr_text, log_text):
            tags.add("SANITIZER")
    if is_timeout_issue(source_error_type):
        tags.add("TIMEOUT")
    if is_xfailed_issue(source_error_type):
        tags.add("XFAILED")
    if status_name_for_not_launched is not None and is_not_launched_issue(
        source_error_type, status_name_for_not_launched
    ):
        tags.add("NOT_LAUNCHED")
    return format_error_type_tags_csv(tags)


# ---------------------------------------------------------------------------
# Prefetch: when to fetch, which URLs, read from cache
# ---------------------------------------------------------------------------


def should_prefetch_debug_files(status, stderr_url, log_url):
    """Whether to prefetch stderr/log: failure-like status and at least one URL (snippet-only otherwise)."""
    if not is_failure_like_status(status):
        return False
    return bool(normalize_fetch_url(stderr_url) or normalize_fetch_url(log_url))


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
    """Stderr and log from ``fetch_cache``; ``None`` if missing/failed, ``""`` if empty body."""
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


def prefetch_texts_by_urls(urls, existing_cache=None, max_workers=DEFAULT_PREFETCH_MAX_WORKERS):
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
    """``status_str`` must be failure|error|mute tokens used by :func:`is_failure_like_status`."""
    return FailureRow(
        status=status_str,
        status_description=getattr(test, "status_description", None),
        source_error_type=getattr(test, "error_type", None),
        stderr_url=getattr(test, "stderr_url", None),
        log_url=getattr(test, "log_url", None),
    )


# ---------------------------------------------------------------------------
# Batch prefetch for many FailureRow instances
# ---------------------------------------------------------------------------


def prefetch_text_cache_and_urls_for_failure_rows(
    failure_rows: Sequence[FailureRow],
    existing_cache: Optional[Dict[str, Any]] = None,
    max_workers: Optional[int] = None,
) -> Tuple[Dict[str, Any], List[Any]]:
    """Prefetch merged URLs from ``failure_rows``; returns ``(cache, url_list)``. Optional ``max_workers``."""
    urls = []
    for fr in failure_rows:
        urls.extend(
            urls_for_debug_prefetch_if_needed(
                should_prefetch_debug_files(fr.status, fr.stderr_url, fr.log_url),
                fr.stderr_url,
                fr.log_url,
            )
        )
    fetch_kw: Dict[str, Any] = {"existing_cache": existing_cache}
    if max_workers is not None:
        fetch_kw["max_workers"] = max_workers
    return prefetch_texts_by_urls(urls, **fetch_kw), urls
