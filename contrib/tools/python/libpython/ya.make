LIBRARY()

VERSION(2.7.18)

LICENSE(PSF-2.0)

USE_PYTHON2()

NO_COMPILER_WARNINGS()

INCLUDE(${ARCADIA_ROOT}/contrib/tools/python/pyconfig.inc)

PEERDIR(
    ${PYTHON_DIR}/lib
)

SRCDIR(
    ${PYTHON_SRC_DIR}
)

CFLAGS(
    ${PYTHON_FLAGS}
)

SRCS(
    GLOBAL Modules/python.c
)

END()
