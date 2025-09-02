PY2_PROGRAM(cffigen)

VERSION(Service-proxy-version)

LICENSE(MIT)

PEERDIR(
    contrib/python/cffi
)

PY_SRCS(
    MAIN main.py
)

NO_LINT()

INDUCED_DEPS(
    cpp ${ARCADIA_ROOT}/contrib/python/cffi/py2/cffi/_cffi_include.h
)

END()
