PY2_LIBRARY()

VERSION(Service-proxy-version)

LICENSE(MIT)

PEERDIR(
    contrib/python/cffi
)

SRCDIR(
    contrib/python/cffi/py2/gen
)

PY_SRCS(
    MAIN main.py
)

NO_LINT()

END()
