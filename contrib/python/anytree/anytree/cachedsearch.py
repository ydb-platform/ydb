"""
Node Searching with Cache.

.. note:: These functions require https://pypi.org/project/fastcache/, otherwise caching is not active.
"""

from . import search

# fastcache is optional
try:
    from fastcache import clru_cache as _cache
except ImportError:
    from functools import wraps

    # dummy decorator which does NOT cache
    def _cache(size):
        # pylint: disable=W0613
        def decorator(func):
            @wraps(func)
            def wrapped(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapped

        return decorator


CACHE_SIZE = 32


@_cache(CACHE_SIZE)
def findall(node, filter_=None, stop=None, maxlevel=None, mincount=None, maxcount=None):
    """Identical to :any:`search.findall` but cached."""
    return search.findall(node, filter_=filter_, stop=stop, maxlevel=maxlevel, mincount=mincount, maxcount=maxcount)


@_cache(CACHE_SIZE)
def findall_by_attr(node, value, name="name", maxlevel=None, mincount=None, maxcount=None):
    """Identical to :any:`search.findall_by_attr` but cached."""
    return search.findall_by_attr(node, value, name=name, maxlevel=maxlevel, mincount=mincount, maxcount=maxcount)


@_cache(CACHE_SIZE)
def find(node, filter_=None, stop=None, maxlevel=None):
    """Identical to :any:`search.find` but cached."""
    return search.find(node, filter_=filter_, stop=stop, maxlevel=maxlevel)


@_cache(CACHE_SIZE)
def find_by_attr(node, value, name="name", maxlevel=None):
    """Identical to :any:`search.find_by_attr` but cached."""
    return search.find_by_attr(node, value, name=name, maxlevel=maxlevel)
