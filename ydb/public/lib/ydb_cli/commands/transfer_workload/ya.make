LIBRARY(transfer_workload)

ADDINCL(
    ydb/public/sdk/cpp
)

SRCS(
    transfer_workload.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transfer_workload_topic_to_table.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transfer_workload_topic_to_table_init.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transfer_workload_topic_to_table_clean.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transfer_workload_topic_to_table_run.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transfer_workload_defines.cpp  # gn: into ydb/public/lib/ydb_cli/commands
)

PEERDIR(
    yql/essentials/public/issue
    yql/essentials/public/issue/protos
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/api/protos/annotations
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/types/operation
    ydb/public/sdk/cpp/src/client/types/status    
)

END()
