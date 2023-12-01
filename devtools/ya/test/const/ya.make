PY23_LIBRARY()

PEERDIR(
    build/plugins/lib/test_const
)

STYLE_PYTHON()

PY_SRCS(
    NAMESPACE test.const
    __init__.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
