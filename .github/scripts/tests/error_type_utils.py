import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib import error as urllib_error
from urllib import request as urllib_request


DEFAULT_FETCH_TIMEOUT_SEC = 5
DEFAULT_FETCH_MAX_BYTES = 1024 * 1024
_DEFAULT_FETCH_MAX_WORKERS = 30
DEFAULT_FETCH_MAX_ATTEMPTS = 3
DEFAULT_FETCH_RETRY_DELAY_SEC = 0.5

# After classify+merge: drop these tags from stored error_type (case-insensitive). Keep NOT_LAUNCHED etc.
_ERROR_TYPE_BLACKLIST = frozenset({"REGULAR"})


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


def _apply_error_type_blacklist(merged_error_type):
    t = _normalize_text(merged_error_type).strip()
    if not t:
        return ""
    if t.upper() in _ERROR_TYPE_BLACKLIST:
        return ""
    return t


def merge_classified_error_type(classified, source_error_type):
    """Classifier tag wins; else keep upstream (e.g. NOT_LAUNCHED). Blacklisted upstream values become empty."""
    if classified:
        merged = classified
    else:
        merged = _normalize_text(source_error_type)
    return _apply_error_type_blacklist(merged)


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


def is_verify_issue(error_text):
    error_text = _normalize_text(error_text)
    if not error_text:
        return False

    return bool(re.search(r'\bVERIFY\s+failed\b', error_text, re.IGNORECASE))


def is_verify_classification(source_error_type, status_description=None, error_file_text=None):
    if _normalize_text(source_error_type).upper() == "VERIFY":
        return True

    status_desc = _normalize_text(status_description)
    if error_file_text is None:
        return is_verify_issue(status_desc)

    file_text = _normalize_text(error_file_text).strip()
    verify_text = file_text if file_text else status_desc
    return is_verify_issue(verify_text)


def _failure_like_status(status):
    return _normalize_text(status).strip().lower() in ("failure", "mute", "error")


def _classify_failure_branch(status, status_description, source_error_type, error_file_text):
    """SANITIZER / TIMEOUT / VERIFY / '' for rows that already passed the failure-like status gate."""
    if is_sanitizer_issue(status_description):
        return "SANITIZER"
    if is_timeout_issue(source_error_type):
        return "TIMEOUT"
    if is_verify_classification(source_error_type, status_description, error_file_text):
        return "VERIFY"
    return ""


def classify_error_type(status, status_description, source_error_type, error_file_text=None):
    if not _failure_like_status(status):
        return ""
    return _classify_failure_branch(status, status_description, source_error_type, error_file_text)


def should_prefetch_stderr_for_verify(status, status_description, source_error_type):
    """True only if stderr might change outcome (VERIFY not already fixed from type/snippet alone)."""
    if not _failure_like_status(status):
        return False
    return _classify_failure_branch(status, status_description, source_error_type, None) == ""


def fetch_text_by_url(
    url,
    timeout_sec=DEFAULT_FETCH_TIMEOUT_SEC,
    max_bytes=DEFAULT_FETCH_MAX_BYTES,
    max_attempts=DEFAULT_FETCH_MAX_ATTEMPTS,
    retry_delay_sec=DEFAULT_FETCH_RETRY_DELAY_SEC,
):
    url = normalize_fetch_url(url)
    if not url:
        return ""

    for attempt in range(max_attempts):
        try:
            with urllib_request.urlopen(url, timeout=timeout_sec) as response:
                data = response.read(max_bytes + 1)
            return data[:max_bytes].decode("utf-8", errors="replace")
        except (urllib_error.URLError, TimeoutError, ValueError, OSError):
            if attempt < max_attempts - 1:
                time.sleep(retry_delay_sec)
            continue

    return ""


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
                cache[url] = ""
    return cache


def is_not_launched_issue(source_error_type, status_name=None):
    if _normalize_text(source_error_type).upper() != "NOT_LAUNCHED":
        return False

    return _normalize_text(status_name).upper() in ("SKIP", "SKIPPED", "MUTE")
