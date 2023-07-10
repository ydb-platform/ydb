LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_query
)

END()
