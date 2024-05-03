PY23_TEST()

SRCDIR(util/system)

PY_SRCS(
    NAMESPACE util.system
    types_ut.pyx
)

TEST_SRCS(
    test_system.py
)

END()
