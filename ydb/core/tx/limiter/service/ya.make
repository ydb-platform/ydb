LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/core/tx/limiter/usage
    ydb/core/protos
)

END()
