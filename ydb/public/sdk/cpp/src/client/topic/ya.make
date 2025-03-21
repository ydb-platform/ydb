LIBRARY()

SRCS(
    out.cpp
    proto_accessor.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic/codecs
    ydb/public/sdk/cpp/src/client/topic/common
    ydb/public/sdk/cpp/src/client/topic/impl
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic

    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/table
    
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
