PY3_PROGRAM(iam_grpc_emulator)

PY_SRCS(
    MAIN main.py
)

PEERDIR(
    contrib/python/grpcio
    ydb/public/api/client/yc_public/iam
)

END()
