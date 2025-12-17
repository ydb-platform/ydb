PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

BUNDLE(
    ydb/apps/ydb NAME ydb_cli
)
RESOURCE(ydb_cli ydb_cli)

BUNDLE(
    ydb/tools/tsserver NAME tsserver
)
RESOURCE(tsserver tsserver)
PEERDIR(
    ydb/tests/stress/common
    ydb/public/sdk/python
    library/python/resource
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
