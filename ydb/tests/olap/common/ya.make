PY3_LIBRARY()

PY_SRCS (
    s3_client.py
    ydb_client.py
)

PEERDIR(
    contrib/python/boto3
    contrib/python/requests
    library/recipes/common
    ydb/public/sdk/python
)

END()