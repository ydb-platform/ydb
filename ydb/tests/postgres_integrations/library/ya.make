PY3_LIBRARY()


ALL_PY_SRCS()

PEERDIR(
    contrib/python/requests
    contrib/python/tornado/tornado-4
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

REQUIREMENTS(ram:10)

END()
