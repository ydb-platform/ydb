def join(base, path):
    return ensure_trailing_separator(base) + _ensure_no_leading_separator(path)


def ensure_trailing_separator(url: str) -> str:
    if not url.endswith('/'):
        return url + '/'
    return url


def ensure_leading_separator(url: str) -> str:
    if not url.startswith('/'):
        return '/' + url
    return url


def _ensure_no_leading_separator(url: str) -> str:
    if url.startswith('/'):
        return url[1:]
    return url
