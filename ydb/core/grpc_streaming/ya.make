LIBRARY()

PEERDIR(
    contrib/libs/grpc
    library/cpp/actors/core
    library/cpp/grpc/server
    ydb/core/base
)

SRCS(
    grpc_streaming.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
