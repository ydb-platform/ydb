LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/library/disk_limiter/usage
    ydb/core/protos
)

END()
