LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_common_client/impl
)

END()
