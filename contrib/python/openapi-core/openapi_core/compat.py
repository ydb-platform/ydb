"""OpenAPI core python 2.7 compatibility module"""
try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache

try:
    from functools import partialmethod
except ImportError:
    from backports.functools_partialmethod import partialmethod

__all__ = ['lru_cache', 'partialmethod']
