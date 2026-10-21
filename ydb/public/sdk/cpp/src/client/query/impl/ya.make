LIBRARY()

# Work around LLVM 20 ThinLTO codegen crashes in StreamExecuteQuery.
NO_LTO()

SRCS(
    exec_query.cpp
    exec_query.h
    client_session.cpp
    session_state_handler.cpp
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
