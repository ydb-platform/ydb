LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/library/slide_limiter/usage
    ydb/core/protos
)

END()
