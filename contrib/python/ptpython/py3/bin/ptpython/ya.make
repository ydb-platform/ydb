PY3_PROGRAM(ptpython)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/ptpython
)

PY_MAIN(main:main)

PY_SRCS(
    TOP_LEVEL
    main.py
)

NO_CHECK_IMPORTS()

END()
