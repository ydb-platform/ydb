PY2_PROGRAM(ptipython)

LICENSE(BSD-3-Clause)

VERSION(0.41)

PEERDIR(
    contrib/python/ipython
    contrib/python/ptpython
)

PY_SRCS(
    TOP_LEVEL
    __main__.py
)

NO_CHECK_IMPORTS()

END()
