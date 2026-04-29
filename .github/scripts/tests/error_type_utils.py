import re


def is_sanitizer_issue(error_text):
    """
    Detect if a test failure is caused by a sanitizer.
    Returns True if the error text contains sanitizer-specific patterns.
    """
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
    return (source_error_type or "").upper() == "TIMEOUT"


def is_not_launched_issue(source_error_type, status_name=None):
    if (source_error_type or "").upper() != "NOT_LAUNCHED":
        return False

    return (status_name or "").upper() in ("SKIP", "SKIPPED", "MUTE")


def classify_error_type(status, status_description, source_error_type):
    status_norm = (status or "").strip().lower()
    if status_norm not in ("failure", "mute"):
        return ""

    if is_sanitizer_issue(status_description or ""):
        return "SANITIZER"

    if is_timeout_issue(source_error_type):
        return "TIMEOUT"

    return ""

