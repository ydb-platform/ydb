PY3_PROGRAM_BIN(py3cc)

ENABLE(PYBUILD_NO_PYC)

DISABLE(PYTHON_SQLITE3)

PEERDIR(
    library/python/runtime_py3
    library/python/runtime_py3/main
)

NO_CHECK_IMPORTS()

NO_PYTHON_INCLUDES()

NO_PYTHON_COVERAGE()

NO_IMPORT_TRACING()

SRCDIR(
    tools/py3cc
)

PY_SRCS(
    MAIN main.py
)

END()
