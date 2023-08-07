LIBRARY()

SRCS(
    exec_query.cpp
    exec_query.h
    client_session.cpp
)

PEERDIR(
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_proto
)

END()
