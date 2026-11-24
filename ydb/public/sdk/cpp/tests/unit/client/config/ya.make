UNITTEST()

SIZE(MEDIUM)

SRCS(
    config_ut.cpp
)

PEERDIR(
    contrib/libs/grpc
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/config
    ydb/public/sdk/cpp/src/client/driver
)

END()
