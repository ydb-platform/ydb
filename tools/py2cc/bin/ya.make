PY2_PROGRAM(py2cc)

#ENABLE(PYBUILD_NO_PYC)

ENABLE(USE_LIGHT_PY2CC)
DISABLE(PYTHON_SQLITE3)

PEERDIR(
    library/python/runtime
    library/python/runtime/main
)

NO_CHECK_IMPORTS()

NO_PYTHON_INCLUDES()

NO_PYTHON_COVERAGE()

NO_IMPORT_TRACING()

SRCDIR(
    tools/py2cc
)

PY_SRCS(
    __main__.py
)

END()
