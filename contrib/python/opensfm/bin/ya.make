PY3_PROGRAM(opensfm)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

NO_LINT()

NO_CHECK_IMPORTS()

PEERDIR(
    contrib/python/opensfm
)

PY_SRCS(
    MAIN run_opensfm.py
)

END()
