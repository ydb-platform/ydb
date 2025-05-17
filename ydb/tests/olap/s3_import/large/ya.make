PY3TEST()

TAG(ya:manual)

SIZE(LARGE)

TEST_SRCS (
    test_large_import.py
)

PEERDIR (
    contrib/python/boto3
    ydb/public/sdk/python
    ydb/tests/olap/lib
)

END()
