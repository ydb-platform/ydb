LIBRARY()

SRCS(
    path_aliasing.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/path_aliasing/context
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
