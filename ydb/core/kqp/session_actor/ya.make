LIBRARY()

SRCS(
    kqp_log_query.cpp
    kqp_query_state.cpp
    kqp_query_stats.cpp
    kqp_response.cpp
    kqp_session_actor.cpp
    kqp_user_facing_tracing.cpp
    kqp_temp_tables_manager.cpp
    kqp_worker_actor.cpp
    kqp_worker_common.cpp
)

PEERDIR(
    ydb/core/docapi
    ydb/core/kqp/common
    ydb/core/kqp/federated_query
    ydb/library/security
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/core/tx/schemeshard
    ydb/services/workload_manager/service
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

YQL_LAST_ABI_VERSION()

END()
