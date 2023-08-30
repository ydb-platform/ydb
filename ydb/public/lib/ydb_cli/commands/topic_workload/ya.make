LIBRARY(topic_workload)

SRCS(
    topic_workload_clean.cpp
    topic_workload_describe.cpp
    topic_workload_init.cpp
    topic_workload_params.cpp
    topic_workload_run_read.cpp
    topic_workload_run_write.cpp
    topic_workload_run_full.cpp
    topic_workload_stats.cpp
    topic_workload_stats_collector.cpp
    topic_workload_writer.cpp
    topic_workload_reader.cpp
    topic_workload_reader_transaction_support.cpp
    topic_workload.cpp
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

RECURSE_FOR_TESTS(
    ut
)
