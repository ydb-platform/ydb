PY3_PROGRAM(cffigen)

VERSION(Service-proxy-version)

LICENSE(MIT)

PEERDIR(
    contrib/python/cffi/py3/gen/lib
)

INDUCED_DEPS(cpp ${ARCADIA_ROOT}/contrib/python/cffi/py3/cffi/_cffi_include.h)

END()

RECURSE(
    lib
)
