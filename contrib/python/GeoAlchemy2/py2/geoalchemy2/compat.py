"""
Python 2 and 3 compatibility:

    - Py3k `memoryview()` made an alias for Py2k `buffer()`
    - Py3k `bytes()` made an alias for Py2k `str()`
"""
try:
    import __builtin__ as builtins
except ImportError:
    import builtins

import sys

if sys.version_info[0] == 2:
    PY3 = False
    buffer = getattr(builtins, 'buffer')
    bytes = str
    str = getattr(builtins, 'unicode')
else:
    PY3 = True
    # Python 2.6 flake8 workaround
    buffer = getattr(builtins, 'memoryview')
    bytes = bytes
    str = str
