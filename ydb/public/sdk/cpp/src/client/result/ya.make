LIBRARY()

SRCS(
    out.cpp
    proto_accessor.cpp
    result.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    ydb/public/sdk/cpp/src/client/value
    ydb/public/sdk/cpp/src/client/proto
)

END()
