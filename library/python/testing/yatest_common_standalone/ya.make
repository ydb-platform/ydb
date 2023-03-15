PY23_LIBRARY()

PY_SRCS(
    __init__.py
)

STYLE_PYTHON()

PEERDIR(
    library/python/testing/yatest_common
)

PY_CONSTRUCTOR(library.python.testing.yatest_common_standalone)

END()
