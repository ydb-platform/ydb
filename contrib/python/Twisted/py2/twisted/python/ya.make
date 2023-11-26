# subset of twisted.python module to resolve cycle dependency between Automat and twisted library

PY2_LIBRARY()

LICENSE(MIT)

NO_LINT()

NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/python/incremental
    contrib/python/zope.interface
)

PY_SRCS(
    NAMESPACE twisted.python
    _oldstyle.py
    compat.py
    components.py
    deprecate.py
    filepath.py
    modules.py
    reflect.py
    runtime.py
    util.py
    win32.py
    zippath.py
)

END()
