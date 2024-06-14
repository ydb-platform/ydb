PY23_TEST()

SRCDIR(util/memory)

PY_SRCS(
    NAMESPACE util.memory
    blob_ut.pyx
)

TEST_SRCS(
    test_memory.py
)

END()
