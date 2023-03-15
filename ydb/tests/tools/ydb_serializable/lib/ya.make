PY3_LIBRARY()

PEERDIR(
    contrib/python/tornado/tornado-4
    ydb/tests/library
    ydb/public/sdk/python
)

PY_SRCS(__init__.py)

END()
