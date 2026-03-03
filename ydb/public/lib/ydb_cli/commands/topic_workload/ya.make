LIBRARY(topic_workload)

SRCS(
    topic_workload_clean.cpp
    topic_workload_configurator.cpp
    topic_workload_describe.cpp
    topic_workload_init.cpp
    topic_workload_params.cpp
    topic_workload_run_read.cpp
    topic_workload_run_write.cpp
    topic_workload_run_full.cpp
    topic_workload_stats.cpp
    topic_workload_stats_collector.cpp
    topic_workload_writer.cpp
    topic_workload_writer_producer.cpp
    topic_workload_keyed_writer.cpp
    topic_workload_keyed_writer_producer.cpp
    topic_workload_reader.cpp
    topic_workload_reader_transaction_support.cpp
    topic_workload.cpp
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
    library/cpp/containers/concurrent_hash
    library/cpp/unified_agent_client
    library/cpp/histogram/hdr
)

END()

RECURSE_FOR_TESTS(
    ut
)
