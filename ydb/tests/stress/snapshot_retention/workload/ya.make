PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
