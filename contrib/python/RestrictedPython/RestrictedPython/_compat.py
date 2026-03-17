import platform
import sys


_version = sys.version_info
IS_PY310_OR_GREATER = _version.major == 3 and _version.minor >= 10
IS_PY311_OR_GREATER = _version.major == 3 and _version.minor >= 11
IS_PY312_OR_GREATER = _version.major == 3 and _version.minor >= 12
IS_PY313_OR_GREATER = _version.major == 3 and _version.minor >= 13
IS_PY314_OR_GREATER = _version.major == 3 and _version.minor >= 14

IS_CPYTHON = platform.python_implementation() == 'CPython'
