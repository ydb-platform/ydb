LIBRARY()

OWNER(fomichev)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/monlib/dynamic_counters/percentile 
    library/cpp/monlib/service/pages 
    ydb/core/base
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/base
    ydb/core/blobstorage/pdisk
    ydb/core/control
    ydb/core/keyvalue
    ydb/library/workload
    ydb/public/lib/base
    ydb/public/lib/operation_id
    ydb/public/sdk/cpp/client/ydb_proto
)

SRCS(
    defs.h
    test_load_actor.cpp
    test_load_actor.h
    test_load_gen.h
    test_load_interval_gen.h
    test_load_keyvalue_write.cpp
    test_load_memory.cpp
    test_load_pdisk_read.cpp
    test_load_pdisk_write.cpp
    test_load_pdisk_log.cpp
    test_load_quantile.h
    test_load_size_gen.h
    test_load_speed.h
    test_load_time_series.h
    test_load_vdisk_write.cpp
    test_load_write.cpp
    test_load_kqp.cpp
)

END()
