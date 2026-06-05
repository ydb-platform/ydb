"""Classify CI test failures: merge upstream ``error_type`` tags with VERIFY/SANITIZER from text."""

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request

DEFAULT_FETCH_TIMEOUT_SEC = 5
DEFAULT_FETCH_TAIL_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_PREFETCH_MAX_WORKERS = 30
DEFAULT_PREFETCH_MAX_WORKERS_FULL_REFRESH = 200
DEFAULT_FETCH_MAX_ATTEMPTS = 3
DEFAULT_FETCH_RETRY_DELAY_SEC = 1.0
DEFAULT_PREFETCH_RETRY_PASSES = 2
DEFAULT_PREFETCH_RETRY_DELAY_SEC = 5.0

# Backfill mode (test_history_fast): many URLs point to deleted log files,
# so use shorter timeout and fewer attempts to avoid long hangs.
BACKFILL_FETCH_TIMEOUT_SEC = 2
BACKFILL_FETCH_MAX_ATTEMPTS = 1
BACKFILL_PREFETCH_RETRY_PASSES = 0

# Sentinel stored in the fetch cache when a URL was attempted but all retries failed.
# Distinguishes "fetch error" from "URL absent" (None / key not present).
_FETCH_FAILED = object()

_ERROR_TYPE_BLACKLIST = frozenset({"REGULAR"})
_STORAGE_TAG_ORDER = (
    "TIMEOUT",
    "XFAILED",
    "NOT_LAUNCHED",
    "VERIFY",
    "SANITIZER",
    "POSSIBLE_OOM",
)
_KNOWN_STORAGE_TAGS = frozenset(_STORAGE_TAG_ORDER)

# Test runner reports `Process exit_code = -9` when killed by SIGKILL (typical OOM-killer).
_POSSIBLE_OOM_RE = re.compile(r"\bProcess exit_code\s*=\s*-9\b")


def _normalize_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value if isinstance(value, str) else str(value)


def normalize_fetch_url(url):
    """YDB Utf8 cells may arrive as bytes; urlopen requires str."""
    return _normalize_text(url).strip()


def source_has_tag(source_error_type, tag: str) -> bool:
    """True if comma-separated ``error_type`` contains ``tag`` (case-insensitive)."""
    want = _normalize_text(tag).strip().upper()
    if not want:
        return False
    for part in re.split(r"\s*,\s*", _normalize_text(source_error_type)):
        if part.strip().upper() == want:
            return True
    return False


def is_failure_like_status(status):
    """failure|mute|error — statuses for which we classify error_type."""
    return _normalize_text(status).strip().lower() in ("failure", "mute", "error")


def is_timeout_issue(source_error_type):
    return source_has_tag(source_error_type, "TIMEOUT")


def is_xfailed_issue(source_error_type):
    return source_has_tag(source_error_type, "XFAILED")


def is_not_launched_issue(source_error_type, status_name=None):
    if not source_has_tag(source_error_type, "NOT_LAUNCHED"):
        return False
    return _normalize_text(status_name).upper() in ("SKIP", "SKIPPED", "MUTE")


def _is_verify_issue(text):
    text = _normalize_text(text)
    return bool(text and re.search(r'\bVERIFY\s+failed\b', text, re.IGNORECASE))


def _is_sanitizer_issue(text):
    text = _normalize_text(text)
    if not text:
        return False
    patterns = [
        r'(ERROR|WARNING|SUMMARY): (AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)',
        r'==\d+==\s*(ERROR|WARNING|SUMMARY): (AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)',
        r'runtime error:',
        r'==\d+==.*runtime error:',
        r'detected memory leaks',
        r'==\d+==.*detected memory leaks',
    ]
    return any(re.search(p, text, re.IGNORECASE | re.MULTILINE) for p in patterns)


# Public classification helpers used in generate-summary.py for badge flags.
# Accept (snippet, stderr_text, log_text); None means source not fetched.

def is_verify_classification(status_description=None, stderr_text=None, log_text=None):
    return (
        _is_verify_issue(status_description)
        or (stderr_text is not None and _is_verify_issue(stderr_text))
        or (log_text is not None and _is_verify_issue(log_text))
    )


def is_sanitizer_classification(status_description=None, stderr_text=None, log_text=None):
    return (
        _is_sanitizer_issue(status_description)
        or (stderr_text is not None and _is_sanitizer_issue(stderr_text))
        or (log_text is not None and _is_sanitizer_issue(log_text))
    )


def is_possible_oom_classification(status_description=None, stderr_text=None, log_text=None):
    """True if snippet or logs show SIGKILL exit (likely kernel OOM-killer)."""
    for text in (status_description, stderr_text, log_text):
        if text is not None and _POSSIBLE_OOM_RE.search(_normalize_text(text)):
            return True
    return False


def build_error_type_csv_for_storage(
    status,
    status_description,
    source_error_type,
    stderr_text,
    log_text,
    status_name_for_not_launched=None,
):
    """Return stable comma-separated error_type tags for DB storage.

    Seeds from upstream ``source_error_type``, then adds text-derived tags
    (VERIFY, SANITIZER, POSSIBLE_OOM) for failure-like statuses. Blacklisted tags stripped.
    """
    tags = set()
    for part in re.split(r"\s*,\s*", _normalize_text(source_error_type).strip()):
        p = part.strip().upper()
        if p and p not in _ERROR_TYPE_BLACKLIST:
            tags.add(p)

    if is_failure_like_status(status):
        if is_verify_classification(status_description, stderr_text, log_text):
            tags.add("VERIFY")
        if is_sanitizer_classification(status_description, stderr_text, log_text):
            tags.add("SANITIZER")
        if is_possible_oom_classification(status_description, stderr_text, log_text):
            tags.add("POSSIBLE_OOM")
        if status_name_for_not_launched is not None and is_not_launched_issue(
            source_error_type, status_name_for_not_launched
        ):
            tags.add("NOT_LAUNCHED")

    if not tags:
        return ""
    clean = {t for t in tags if t not in _ERROR_TYPE_BLACKLIST}
    out = [k for k in _STORAGE_TAG_ORDER if k in clean]
    out.extend(sorted(t for t in clean if t not in _KNOWN_STORAGE_TAGS))
    return ",".join(out)


@dataclass
class FailureRow:
    """Inputs for classification: status, snippet, upstream tags, and debug-file URLs."""
    status: Any
    status_description: Any
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


def _link_url_from_report(links, *keys):
    for key in keys:
        vals = links.get(key)
        if isinstance(vals, list) and vals:
            return vals[0]
    return None


def failure_row_from_report_result(result: Dict[str, Any], status_str: str) -> FailureRow:
    links = result.get("links") or {}
    return FailureRow(
        status=status_str,
        status_description=result.get("rich-snippet"),
        source_error_type=result.get("error_type"),
        stderr_url=_link_url_from_report(links, "stderr"),
        log_url=_link_url_from_report(links, "log", "Log"),
    )


def _failure_status_str_for_report(result: Dict[str, Any]) -> Optional[str]:
    status = _normalize_text(result.get("status")).upper()
    if status == "FAILED":
        return "failure"
    if status == "ERROR":
        return "error"
    if status == "MUTE":
        return "mute"
    return None


def enrich_error_types_in_results(
    results: Sequence[Dict[str, Any]],
    public_dir: Optional[str] = None,
    public_dir_url: Optional[str] = None,
) -> None:
    """Classify failure-like test rows and write merged tags into ``error_type``."""
    failure_rows = []
    targets = []
    for result in results:
        status_str = _failure_status_str_for_report(result)
        if not status_str:
            continue
        failure_rows.append(failure_row_from_report_result(result, status_str))
        targets.append(result)

    if not failure_rows:
        return

    cache = prefetch_text_cache_for_failure_rows(
        failure_rows,
        local_dir=public_dir,
        local_url_prefix=public_dir_url,
    )
    for result, fr in zip(targets, failure_rows):
        stderr_text, log_text = get_debug_texts_from_cache(fr, cache)
        result["error_type"] = build_error_type_csv_for_storage(
            fr.status,
            fr.status_description,
            fr.source_error_type,
            stderr_text,
            log_text,
            status_name_for_not_launched=result.get("status"),
        )


def get_debug_texts_from_cache(fr: FailureRow, fetch_cache: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Return (stderr_text, log_text) from cache. None if URL missing, not fetched, or fetch failed."""
    if not is_failure_like_status(fr.status):
        return None, None
    se = normalize_fetch_url(fr.stderr_url)
    lg = normalize_fetch_url(fr.log_url)

    def _resolve(url: str) -> Optional[str]:
        if not url:
            return None
        val = fetch_cache.get(url)
        # _FETCH_FAILED sentinel → classify as if text unavailable (same as absent URL)
        return None if val is _FETCH_FAILED else val

    return _resolve(se), _resolve(lg)


def _fetch_text_slice(url: str, byte_range: Optional[str], max_bytes: int, timeout: int = DEFAULT_FETCH_TIMEOUT_SEC) -> str:
    req = urllib_request.Request(url)
    if byte_range:
        req.add_header("Range", byte_range)
    with urllib_request.urlopen(req, timeout=timeout) as resp:
        data = resp.read(max_bytes)
    return data.decode("utf-8", errors="replace")


def _fetch_text_by_url(url, timeout: int = DEFAULT_FETCH_TIMEOUT_SEC, attempts: int = DEFAULT_FETCH_MAX_ATTEMPTS):
    """Return response text, "" for empty body, _FETCH_FAILED sentinel on failure."""
    for attempt in range(attempts):
        try:
            return _fetch_text_slice(
                url,
                byte_range=f"bytes=-{DEFAULT_FETCH_TAIL_MAX_BYTES}",
                max_bytes=DEFAULT_FETCH_TAIL_MAX_BYTES,
                timeout=timeout,
            )
        except (urllib_error.URLError, TimeoutError, ValueError, OSError):
            if attempt < attempts - 1:
                time.sleep(DEFAULT_FETCH_RETRY_DELAY_SEC)
    return _FETCH_FAILED


def _fetch_urls_parallel(urls: List[str], cache: Dict[str, Any], workers: int,
                         fetch_timeout: int = DEFAULT_FETCH_TIMEOUT_SEC,
                         fetch_attempts: int = DEFAULT_FETCH_MAX_ATTEMPTS) -> None:
    """Fetch *urls* in parallel, writing results into *cache* in-place."""
    future_to_url = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_url = {
            pool.submit(_fetch_text_by_url, url, fetch_timeout, fetch_attempts): url
            for url in urls
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                cache[url] = future.result()
            except Exception:
                cache[url] = _FETCH_FAILED


def _read_local_for_url(url: str, local_dir: str, local_url_prefix: str) -> Optional[str]:
    """If *url* is under *local_url_prefix*, read the file from *local_dir* instead.

    Useful when log files are already on disk but not yet uploaded to remote
    storage (e.g. generate-summary.py runs before s3cmd sync in CI).
    Returns file text on success, None if the file is not found locally.
    """
    if not url or not local_dir or not local_url_prefix:
        return None
    prefix = local_url_prefix.rstrip('/') + '/'
    if not url.startswith(prefix):
        return None
    rel = url[len(prefix):]
    # Prevent path traversal: strip leading slashes and reject '..' components.
    rel = rel.lstrip('/')
    local_path = os.path.normpath(os.path.join(local_dir, rel))
    if not local_path.startswith(os.path.normpath(local_dir) + os.sep):
        return None
    try:
        size = os.path.getsize(local_path)
        with open(local_path, 'rb') as f:
            if size > DEFAULT_FETCH_TAIL_MAX_BYTES:
                f.seek(size - DEFAULT_FETCH_TAIL_MAX_BYTES)
            return f.read(DEFAULT_FETCH_TAIL_MAX_BYTES).decode('utf-8', errors='replace')
    except OSError:
        return None


def prefetch_text_cache_for_failure_rows(
    failure_rows: Sequence[FailureRow],
    existing_cache: Optional[Dict[str, Any]] = None,
    max_workers: Optional[int] = None,
    local_dir: Optional[str] = None,
    local_url_prefix: Optional[str] = None,
    fetch_timeout: int = DEFAULT_FETCH_TIMEOUT_SEC,
    fetch_attempts: int = DEFAULT_FETCH_MAX_ATTEMPTS,
    retry_passes: int = DEFAULT_PREFETCH_RETRY_PASSES,
) -> Dict[str, Any]:
    """Download stderr/log URLs from failure_rows in parallel; return url→text cache.

    If *local_dir* and *local_url_prefix* are provided, URLs that start with
    *local_url_prefix* are resolved to local files under *local_dir* before any
    HTTP fetch is attempted.  This avoids network round-trips when the files are
    already on disk (e.g. CI log files copied by transform_build_results.py but
    not yet uploaded to S3).

    After the HTTP pass, any URL that still failed is retried up to
    *retry_passes* times with a short delay between passes.

    *fetch_timeout* and *fetch_attempts* tune per-URL behaviour: lower values
    speed up backfill runs where many URLs point to already-deleted log files.
    """
    cache = existing_cache if existing_cache is not None else {}
    seen: set = set()
    urls_to_fetch = []
    local_hit = 0
    for fr in failure_rows:
        if not is_failure_like_status(fr.status):
            continue
        for raw in (fr.stderr_url, fr.log_url):
            url = normalize_fetch_url(raw)
            if not url or url in cache or url in seen:
                continue
            seen.add(url)
            if local_dir and local_url_prefix:
                text = _read_local_for_url(url, local_dir, local_url_prefix)
                if text is not None:
                    cache[url] = text
                    local_hit += 1
                    continue
            urls_to_fetch.append(url)

    if local_hit:
        print(f"[prefetch] {local_hit} URL(s) resolved from local disk", flush=True)

    if not urls_to_fetch:
        print("prefetch: no urls to fetch", flush=True)
        return cache

    total = len(urls_to_fetch)
    workers = min(max_workers or DEFAULT_PREFETCH_MAX_WORKERS, total)
    t0 = time.time()
    print(f"[prefetch] {total} URL(s), {workers} workers, timeout={fetch_timeout}s, attempts={fetch_attempts}...", flush=True)
    _fetch_urls_parallel(urls_to_fetch, cache, workers, fetch_timeout, fetch_attempts)

    for retry_pass in range(1, retry_passes + 1):
        failed_urls = [u for u in urls_to_fetch if cache.get(u) is _FETCH_FAILED]
        if not failed_urls:
            break
        print(
            f"[prefetch] retry pass {retry_pass}/{retry_passes}: "
            f"{len(failed_urls)} URL(s) failed, retrying in {DEFAULT_PREFETCH_RETRY_DELAY_SEC:.0f}s...",
            flush=True,
        )
        time.sleep(DEFAULT_PREFETCH_RETRY_DELAY_SEC)
        retry_workers = min(workers, len(failed_urls))
        _fetch_urls_parallel(failed_urls, cache, retry_workers, fetch_timeout, fetch_attempts)

    failed = sum(1 for u in urls_to_fetch if cache.get(u) is _FETCH_FAILED)
    print(f"[prefetch] done {total} URL(s) in {time.time() - t0:.1f}s, failed={failed}", flush=True)
    return cache
