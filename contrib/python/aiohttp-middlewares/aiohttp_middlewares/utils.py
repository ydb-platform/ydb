"""
=========================
aiohttp_middlewares.utils
=========================

Various utility functions for ``aiohttp_middlewares`` library.

"""

from yarl import URL

from aiohttp_middlewares.annotations import Url, Urls


def match_path(item: Url, path: str) -> bool:
    """Check whether current path is equal to given URL str or regexp.

    :param item: URL to compare with request path.
    :param path: Request path string.
    """
    if isinstance(item, URL):
        item = str(item)

    if isinstance(item, str):
        return item == path

    try:
        return bool(item.match(path))
    except (AttributeError, TypeError):
        return False


def match_request(urls: Urls, method: str, path: str) -> bool:
    """Check whether request method and path matches given URLs or not."""
    found = [item for item in urls if match_path(item, path)]
    if not found:
        return False

    if not isinstance(urls, dict):
        return True

    found_item = urls[found[0]]
    method = method.lower()
    if isinstance(found_item, str):
        return found_item.lower() == method

    return any(True for item in found_item if item.lower() == method)
