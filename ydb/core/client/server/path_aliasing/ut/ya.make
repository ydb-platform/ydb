UNITTEST()

SRCS(
    console_paths_ut.cpp
    path_aliasing_ut.cpp
)

PEERDIR(
    ydb/core/client/server/path_aliasing
    ydb/core/path_aliasing/context
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/public/api/protos
)

END()
