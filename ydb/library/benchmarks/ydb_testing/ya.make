PY3TEST()

SIZE(MEDIUM)

TEST_SRCS(
    test.py
)

PEERDIR(
    ydb/public/sdk/python
    contrib/python/requests
    contrib/python/ydb/py3
)

END()
