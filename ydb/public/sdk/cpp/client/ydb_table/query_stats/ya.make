LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/draft/ydb_query
)

END()
