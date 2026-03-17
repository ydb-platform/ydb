import re

ANY = re.compile(".*")


def _text_matches_pattern(pattern, text):
    # This is needed for compatibility with old Python versions
    try:
        pattern_class = re.Pattern
    except AttributeError:
        pattern_class = re._pattern_type
    if isinstance(pattern, str):
        if pattern == text:
            return True
    elif isinstance(pattern, pattern_class):
        if pattern.search(text):
            return True
    return False
