PY3_LIBRARY()

PY_SRCS(
    __init__.py
    test_base.py
)

PEERDIR(
    contrib/python/boto3
    contrib/python/botocore
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

END()
