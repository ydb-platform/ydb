PY3_PROGRAM(iam_grpc_emulator)

PY_SRCS(
    MAIN main.py
)

PEERDIR(
    contrib/python/grpcio
    ydb/public/api/client/yc_private/iam
    ydb/public/api/client/yc_private/accessservice
    ydb/public/api/client/yc_private/servicecontrol
    ydb/public/api/client/yc_private/operation
)

END()
