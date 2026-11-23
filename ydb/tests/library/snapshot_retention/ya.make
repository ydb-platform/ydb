PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/requests
    ydb/core/protos
    ydb/tests/library
    ydb/tests/library/clients
    ydb/tests/oss/ydb_sdk_import
)

END()
