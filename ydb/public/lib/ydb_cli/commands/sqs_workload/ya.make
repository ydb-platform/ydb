LIBRARY(sqs_workload)

SRCS(
    sqs_workload.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_read_scenario.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_reader.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_run.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_run_read.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_run_write.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_scenario.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_stats.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_stats_collector.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_write_scenario.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_writer.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_init.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_init_scenario.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_clean.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_workload_clean_scenario.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    http_client.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    sqs_client_wrapper.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    message_groups_locker.cpp  # gn: into ydb/public/lib/ydb_cli/commands
)

PEERDIR(
    yql/essentials/public/issue
    yql/essentials/public/issue/protos
    ydb/library/backup
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/api/protos/annotations
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/types/operation
    ydb/public/sdk/cpp/src/client/types/status
    ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_json
    library/cpp/containers/concurrent_hash
    library/cpp/unified_agent_client
    library/cpp/histogram/hdr
    contrib/libs/fmt
)

END()

RECURSE_FOR_TESTS(
    ut
)
