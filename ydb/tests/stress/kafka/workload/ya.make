PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

BUNDLE(
    ydb/apps/ydb NAME ydb_cli
)

RESOURCE(
    ydb_cli ydb_cli
)

PEERDIR(
    library/python/monlib
    library/python/resource
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
