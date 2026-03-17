PY23_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(LGPL-2.1-or-later)

PEERDIR(
    contrib/libs/geos/capi
    library/python/ctypes
)

ADDINCL(
    contrib/libs/geos/capi
)

SRCS(
    syms.c
)

PY_REGISTER(
    contrib.libs.geos.capi.ctypes.syms
)

PY_SRCS(
    __init__.py
)

END()
