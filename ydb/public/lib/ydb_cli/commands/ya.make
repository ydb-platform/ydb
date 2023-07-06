LIBRARY(clicommands)

SRCS(
    interactive/interactive_cli.cpp
    interactive/interactive_cli.h
    interactive/term_io.cpp
    interactive/term_io.h
    benchmark_utils.cpp
    click_bench.cpp
    kv_workload.cpp
    stock_workload.cpp
    topic_operations_scenario.cpp
    topic_read_scenario.cpp
    topic_write_scenario.cpp
    topic_readwrite_scenario.cpp
    tpch.cpp
    ydb_sdk_core_access.cpp
    ydb_command.cpp
    ydb_profile.cpp
    ydb_root_common.cpp
    ydb_service_auth.cpp
    ydb_service_discovery.cpp
    ydb_service_export.cpp
    ydb_service_import.cpp
    ydb_service_monitoring.cpp
    ydb_service_operation.cpp
    ydb_service_scheme.cpp
    ydb_service_scripting.cpp
    ydb_service_topic.cpp
    ydb_service_table.cpp
    ydb_tools.cpp
    ydb_workload.cpp
    ydb_yql.cpp
)

PEERDIR(
    library/cpp/histogram/hdr
    library/cpp/protobuf/json
    library/cpp/regex/pcre
    library/cpp/threading/local_executor
    ydb/library/backup
    ydb/library/workload
    ydb/public/lib/operation_id
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/commands/topic_workload
    ydb/public/lib/ydb_cli/commands/transfer_workload
    ydb/public/lib/ydb_cli/dump
    ydb/public/lib/ydb_cli/import
    ydb/public/lib/ydb_cli/topic
    ydb/public/sdk/cpp/client/draft
    ydb/public/sdk/cpp/client/ydb_coordination
    ydb/public/sdk/cpp/client/ydb_discovery
    ydb/public/sdk/cpp/client/ydb_export
    ydb/public/sdk/cpp/client/ydb_import
    ydb/public/sdk/cpp/client/ydb_monitoring
    ydb/public/sdk/cpp/client/ydb_operation
    ydb/public/sdk/cpp/client/ydb_persqueue_public
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_types/credentials/login
)

RESOURCE(
    click_bench_queries.sql click_bench_queries.sql
    click_bench_schema.sql click_bench_schema.sql
    tpch_schema.sql tpch_schema.sql
)

END()

RECURSE_FOR_TESTS(
    topic_workload/ut
)
