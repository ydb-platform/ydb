LIBRARY()

SRCS(
    exec_query.cpp  # gn: into ydb/public/sdk/cpp/src/client/query
    exec_query.h  # gn: into ydb/public/sdk/cpp/src/client/query
    client_session.cpp  # gn: into ydb/public/sdk/cpp/src/client/query
    session_state_handler.cpp  # gn: into ydb/public/sdk/cpp/src/client/query
)

PEERDIR(
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/impl/session
    ydb/public/sdk/cpp/src/client/impl/observability
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/types
)

END()
