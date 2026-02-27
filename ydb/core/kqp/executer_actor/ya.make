LIBRARY()

SRCS(
    kqp_data_executer.cpp
    kqp_scan_executer.cpp
    kqp_scheme_executer.cpp
    kqp_executer_impl.cpp
    kqp_executer_stats.cpp
    kqp_literal_executer.cpp
    kqp_locks_helper.cpp
    kqp_partition_helper.cpp
    kqp_planner.cpp
    kqp_planner_strategy.cpp
    kqp_streaming_helper.cpp
    kqp_table_resolver.cpp
    kqp_tasks_graph.cpp
    kqp_tasks_validate.cpp
    kqp_partitioned_executer.cpp
    shard_key_ranges.cpp
)

PEERDIR(
    library/cpp/containers/absl_flat_hash
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/client/minikql_compile
    ydb/core/formats
    ydb/core/fq/libs/checkpointing
    ydb/core/kqp/common
    ydb/core/kqp/compute_actor
    ydb/core/kqp/executer_actor/shards_resolver
    ydb/core/kqp/federated_query/actors
    ydb/core/kqp/gateway/local_rpc
    ydb/core/kqp/query_compiler
    ydb/core/kqp/rm_service
    ydb/core/kqp/topics
    ydb/core/protos
    ydb/core/sys_view/common
    ydb/core/tx/long_tx_service/public
    ydb/core/ydb_convert
    ydb/library/actors/core
    ydb/library/mkql_proto
    ydb/library/mkql_proto/protos
    ydb/library/plan2svg
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/tasks
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/pq/proto
    ydb/services/metadata/abstract
)

GENERATE_ENUM_SERIALIZATION(
    kqp_executer.h
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
