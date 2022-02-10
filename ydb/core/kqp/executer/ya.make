LIBRARY()

OWNER(
    spuchin
    g:kikimr
)

SRCS(
    kqp_data_executer.cpp 
    kqp_scan_executer.cpp 
    kqp_executer_impl.cpp 
    kqp_executer_stats.cpp 
    kqp_literal_executer.cpp 
    kqp_locks_helper.cpp 
    kqp_partition_helper.cpp 
    kqp_planner.cpp 
    kqp_planner_strategy.cpp 
    kqp_shards_resolver.cpp
    kqp_result_channel.cpp 
    kqp_table_resolver.cpp 
    kqp_tasks_graph.cpp
    kqp_tasks_validate.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/client/minikql_compile
    ydb/core/formats
    ydb/core/kqp/common
    ydb/core/kqp/compile
    ydb/core/kqp/rm
    ydb/core/protos
    ydb/core/ydb_convert
    ydb/library/mkql_proto
    ydb/library/mkql_proto/protos
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/tasks
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
