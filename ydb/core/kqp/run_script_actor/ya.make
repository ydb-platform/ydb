LIBRARY()

SRCS(
    kqp_run_script_actor_impl.cpp
    kqp_run_script_actor.cpp
    kqp_script_lease_watcher_actor.cpp
    kqp_script_result_handler.cpp
)

PEERDIR(
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/fq/libs/checkpointing/events
    ydb/core/fq/libs/common
    ydb/core/kqp/common/events
    ydb/core/kqp/executer_actor
    ydb/core/kqp/proxy_service/proto
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/yql/providers/pq/proto
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
