import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib import error as urllib_error
from urllib import request as urllib_request


DEFAULT_FETCH_TIMEOUT_SEC = 5
DEFAULT_FETCH_MAX_BYTES = 1024 * 1024
DEFAULT_FETCH_MAX_WORKERS = 100
DEFAULT_FETCH_MAX_ATTEMPTS = 3
DEFAULT_FETCH_RETRY_DELAY_SEC = 0.5


def _normalize_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    return str(value)


def is_sanitizer_issue(error_text):
    """
    Detect if a test failure is caused by a sanitizer.
    Returns True if the error text contains sanitizer-specific patterns.
    """
    error_text = _normalize_text(error_text)
    if not error_text:
        return False

    sanitizer_patterns = [
        # Main sanitizer patterns with severity levels (covers most cases)
        r'(ERROR|WARNING|SUMMARY): (AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)',
        # Process ID prefixed patterns (format: ==PID==SEVERITY: SANITIZER)
        r'==\d+==\s*(ERROR|WARNING|SUMMARY): (AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)',
        # UndefinedBehaviorSanitizer runtime errors
        r'runtime error:',
        r'==\d+==.*runtime error:',
        # Memory leak detection (specific LeakSanitizer output)
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


def is_verify_classification(source_error_type, status_description=None, verify_source_text=None):
    if _normalize_text(source_error_type).upper() == "VERIFY":
        return True

    verify_text = verify_source_text if verify_source_text is not None else status_description
    return is_verify_issue(verify_text)


def fetch_text_by_url(
    url,
    timeout_sec=DEFAULT_FETCH_TIMEOUT_SEC,
    max_bytes=DEFAULT_FETCH_MAX_BYTES,
    max_attempts=DEFAULT_FETCH_MAX_ATTEMPTS,
    retry_delay_sec=DEFAULT_FETCH_RETRY_DELAY_SEC,
):
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


def prefetch_texts_by_urls(urls, existing_cache=None, max_workers=DEFAULT_FETCH_MAX_WORKERS):
    cache = existing_cache if existing_cache is not None else {}
    unique_urls = [url for url in set(urls) if url and url not in cache]
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


def classify_error_type(status, status_description, source_error_type, verify_source_text=None):
    status_norm = _normalize_text(status).strip().lower()
    if status_norm not in ("failure", "mute", "error"):
        return ""

    if is_sanitizer_issue(status_description):
        return "SANITIZER"

    if is_timeout_issue(source_error_type):
        return "TIMEOUT"

    if is_verify_classification(source_error_type, status_description, verify_source_text):
        return "VERIFY"

    return ""

