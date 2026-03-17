'''
Iterative JSON parser.

Main API:

- ``ijson.parse``: iterator returning parsing events with the object tree context,
  see ``ijson.common.parse`` for docs.

- ``ijson.items``: iterator returning Python objects found under a specified prefix,
  see ``ijson.common.items`` for docs.

Top-level ``ijson`` module exposes method from the pure Python backend. There's
also two other backends using the C library yajl in ``ijson.backends`` that have
the same API and are faster under CPython.
'''
from ijson.common import JSONError, IncompleteJSONError, ObjectBuilder, compat

from ijson.utils import coroutine, sendable_list
from .version import __version__

def get_backend(backend):
    """Import the backend named ``backend``"""
    import importlib
    return importlib.import_module('ijson.backends.' + backend)

def _default_backend():
    import os
    if 'IJSON_BACKEND' in os.environ:
        return get_backend(os.environ['IJSON_BACKEND'])
    for backend in ('yajl2_c', 'yajl2_cffi', 'yajl2', 'yajl', 'python'):
        try:
            return get_backend(backend)
        except ImportError:
            continue
    raise ImportError('no backends available')
backend = _default_backend()
del _default_backend

basic_parse = backend.basic_parse
basic_parse_coro = backend.basic_parse_coro
parse = backend.parse
parse_coro = backend.parse_coro
items = backend.items
items_coro = backend.items_coro
kvitems = backend.kvitems
kvitems_coro = backend.kvitems_coro
if compat.IS_PY35:
    basic_parse_async = backend.basic_parse_async
    parse_async = backend.parse_async
    items_async = backend.items_async
    kvitems_async = backend.kvitems_async
backend = backend.backend