PY3TEST()

PEERDIR(
    contrib/python/annotated-types
    contrib/python/pytest
)

TEST_SRCS(
    __init__.py
    test_grouped_metadata.py
    test_main.py
)

END()
