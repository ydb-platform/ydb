LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/histogram/hdr
    library/cpp/monlib/dynamic_counters/percentile
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/base
    ydb/core/blobstorage/pdisk
    ydb/core/control
    ydb/core/keyvalue
    ydb/core/jaeger_tracing
    ydb/core/kqp/common
    ydb/core/kqp/rm_service
    ydb/core/tx/columnshard
    ydb/core/tx/datashard
    ydb/library/workload/abstract
    ydb/library/workload/kv
    ydb/library/workload/stock
    ydb/public/lib/base
    ydb/public/lib/operation_id
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/services/kesus
    ydb/services/metadata
    ydb/services/persqueue_cluster_discovery
    ydb/services/ydb
)

SRCS(
    aggregated_result.cpp
    archive.cpp
    config_examples.cpp
    keyvalue_write.cpp
    kqp.cpp
    memory.cpp
    pdisk_log.cpp
    pdisk_read.cpp
    pdisk_write.cpp
    service_actor.cpp
    group_write.cpp
    vdisk_write.cpp
    yql_single_query.cpp

    ycsb/actors.h
    ycsb/bulk_mkql_upsert.cpp
    ycsb/common.h
    ycsb/common.cpp
    ycsb/defs.h
    ycsb/info_collector.h
    ycsb/info_collector.cpp
    ycsb/kqp_select.cpp
    ycsb/kqp_upsert.cpp
    ycsb/test_load_actor.cpp
    ycsb/test_load_actor.h
    ycsb/test_load_read_iterator.cpp
)

GENERATE_ENUM_SERIALIZATION(percentile.h)

END()

RECURSE_FOR_TESTS(
    ut_ycsb
)
