PY3_LIBRARY()

PY_SRCS(
    __init__.py
    boto_stress.py
)
BUNDLE(
    ydb/apps/ydb NAME ydb_cli
)
RESOURCE(ydb_cli ydb_cli)
PEERDIR(
    ydb/tests/stress/common

    contrib/python/boto3
    contrib/python/botocore
    library/python/monlib
    library/python/resource
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    ydb/tests/library
)

END()
