LIBRARY()

PEERDIR(
    contrib/libs/grpc
    ydb/library/actors/core
    ydb/library/grpc/server
    ydb/core/base
)

SRCS(
    grpc_streaming.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
