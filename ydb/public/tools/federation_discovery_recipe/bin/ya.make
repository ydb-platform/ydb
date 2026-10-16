PY3_PROGRAM(federation_discovery)

PY_SRCS(
    MAIN main.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    contrib/python/grpcio
    ydb/public/api/grpc
    ydb/public/api/protos
)

END()
