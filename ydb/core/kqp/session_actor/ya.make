LIBRARY()

SRCS(
    kqp_response.cpp
    kqp_session_actor.cpp
    kqp_worker_actor.cpp
    kqp_worker_common.cpp
    kqp_query_state.cpp
    kqp_query_stats.cpp
    kqp_temp_tables_manager.cpp
)

PEERDIR(
    ydb/core/docapi
    ydb/core/kqp/common
    ydb/core/kqp/federated_query
    ydb/public/lib/operation_id
    ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
