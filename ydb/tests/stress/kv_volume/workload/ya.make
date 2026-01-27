PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

BUNDLE(
    ydb/apps/ydb NAME ydb_cli
)
PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/kv_volume/protos
    ydb/public/sdk/python
    library/python/resource
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
