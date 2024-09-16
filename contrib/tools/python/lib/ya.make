LIBRARY()

PROVIDES(python)

VERSION(2.7.18)

LICENSE(PSF-2.0)

INCLUDE(${ARCADIA_ROOT}/contrib/tools/python/pyconfig.inc)

PEERDIR(
    certs
    contrib/tools/python/base
    contrib/tools/python/include
)

SUPPRESSIONS(lsan.supp)

SRCS(
    bootstrap.c
    python_frozen_modules.rodata
)

END()
