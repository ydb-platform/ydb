LIBRARY()

OWNER(g:kikimr)

SRCS(
    utils.cpp
    private_client.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/json
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/client/ydb_table
)

END()
