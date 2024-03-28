LIBRARY(clicommands)

SRCS(
    interactive/interactive_cli.cpp
    interactive/line_reader.cpp
    benchmark_utils.cpp
    click_bench.cpp
    topic_operations_scenario.cpp
    topic_read_scenario.cpp
    topic_write_scenario.cpp
    topic_readwrite_scenario.cpp
    tpch.cpp
    query_workload.cpp
    ydb_admin.cpp
    ydb_sdk_core_access.cpp
    ydb_command.cpp
    ydb_dynamic_config.cpp
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
    ydb_sql.cpp
    ydb_tools.cpp
    ydb_workload.cpp
    ydb_yql.cpp
)

PEERDIR(
    contrib/libs/fmt
    contrib/restricted/patched/replxx
    library/cpp/histogram/hdr
    library/cpp/protobuf/json
    library/cpp/regex/pcre
    library/cpp/threading/local_executor
    ydb/library/backup
    ydb/library/workload
    ydb/library/yaml_config/public
    ydb/public/lib/operation_id
    ydb/public/lib/stat_visualization
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
    tpch_queries.sql tpch_queries.sql
    click_bench_canonical/q0.result q0.result
    click_bench_canonical/q1.result q1.result
    click_bench_canonical/q2.result q2.result
    click_bench_canonical/q3.result q3.result
    click_bench_canonical/q4.result q4.result
    click_bench_canonical/q5.result q5.result
    click_bench_canonical/q6.result q6.result
    click_bench_canonical/q7.result q7.result
    click_bench_canonical/q8.result q8.result
    click_bench_canonical/q9.result q9.result
    click_bench_canonical/q10.result q10.result
    click_bench_canonical/q11.result q11.result
    click_bench_canonical/q12.result q12.result
    click_bench_canonical/q13.result q13.result
    click_bench_canonical/q14.result q14.result
    click_bench_canonical/q15.result q15.result
    click_bench_canonical/q16.result q16.result
    click_bench_canonical/q17.result q17.result
    click_bench_canonical/q18.result q18.result
    click_bench_canonical/q19.result q19.result
    click_bench_canonical/q20.result q20.result
    click_bench_canonical/q21.result q21.result
    click_bench_canonical/q22.result q22.result
    click_bench_canonical/q23.result q23.result
    click_bench_canonical/q24.result q24.result
    click_bench_canonical/q25.result q25.result
    click_bench_canonical/q26.result q26.result
    click_bench_canonical/q27.result q27.result
    click_bench_canonical/q28.result q28.result
    click_bench_canonical/q29.result q29.result
    click_bench_canonical/q30.result q30.result
    click_bench_canonical/q31.result q31.result
    click_bench_canonical/q32.result q32.result
    click_bench_canonical/q33.result q33.result
    click_bench_canonical/q34.result q34.result
    click_bench_canonical/q35.result q35.result
    click_bench_canonical/q36.result q36.result
    click_bench_canonical/q37.result q37.result
    click_bench_canonical/q38.result q38.result
    click_bench_canonical/q39.result q39.result
    click_bench_canonical/q40.result q40.result
    click_bench_canonical/q41.result q41.result
    click_bench_canonical/q42.result q42.result
)

END()

RECURSE_FOR_TESTS(
    topic_workload/ut
)
