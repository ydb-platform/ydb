PY3_PROGRAM(federation_discovery)

PY_SRCS(
    MAIN main.py
)

PEERDIR(
    contrib/python/grpcio
    ydb/public/api/grpc
    ydb/public/api/protos
)

END()
