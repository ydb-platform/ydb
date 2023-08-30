LIBRARY(transfer_workload)

SRCS(
    transfer_workload.cpp
    transfer_workload_topic_to_table.cpp
    transfer_workload_topic_to_table_init.cpp
    transfer_workload_topic_to_table_clean.cpp
    transfer_workload_topic_to_table_run.cpp
    transfer_workload_defines.cpp
)

PEERDIR(
    ydb/library/yql/public/issue
    ydb/library/yql/public/issue/protos
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/api/protos/annotations
    ydb/public/lib/operation_id
    ydb/public/lib/operation_id/protos
    ydb/public/sdk/cpp/client/draft
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_types/operation
    ydb/public/sdk/cpp/client/ydb_types/status    
)

END()
