PY3_LIBRARY()

VERSION(Service-proxy-version)

LICENSE(MIT)

PEERDIR(
    contrib/python/cffi
)

SRCDIR(
    contrib/python/cffi/py3/gen
)

PY_SRCS(
    MAIN main.py
)

NO_LINT()

END()
