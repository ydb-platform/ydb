LIBRARY()

SRCS(
    kqp_run_script_actor.cpp  # gn: into ydb/core/kqp/script_executions/table_queries
    kqp_run_script_actor_impl.cpp  # gn: into ydb/core/kqp/script_executions/table_queries
    kqp_script_lease_watcher_actor.cpp  # gn: into ydb/core/kqp/script_executions/table_queries
    kqp_script_result_handler.cpp  # gn: into ydb/core/kqp/script_executions/table_queries
)

PEERDIR(
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/fq/libs/checkpointing/events
    ydb/core/fq/libs/common
    ydb/core/kqp/common/events
    ydb/core/kqp/executer_actor
    ydb/core/kqp/script_executions/common
    ydb/core/kqp/script_executions/proto
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/actors/wilson
    ydb/library/yql/providers/pq/proto
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
