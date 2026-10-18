LIBRARY()

SRCS(
    kqp_log_query.cpp
    kqp_query_state.cpp
    kqp_query_stats.cpp
    kqp_response.cpp  # gn: worker_common
    kqp_session_actor.cpp
    kqp_temp_tables_manager.cpp
    kqp_worker_actor.cpp
    kqp_worker_common.cpp  # gn: worker_common
)
# gn: worker_common headers kqp_worker_common.h kqp_worker_settings.h

PEERDIR(
    ydb/core/kqp/tracing
    ydb/core/docapi
    ydb/core/kqp/common
    ydb/core/kqp/topics
    ydb/core/kqp/query_data
    ydb/core/kqp/federated_query
    ydb/library/security
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/core/tx/schemeshard
    ydb/services/workload_manager/service
)

YQL_LAST_ABI_VERSION()

END()
