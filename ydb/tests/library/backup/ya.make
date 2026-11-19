PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/boto3
    contrib/python/grpcio
    library/python/port_manager
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/tests/library
)

END()
