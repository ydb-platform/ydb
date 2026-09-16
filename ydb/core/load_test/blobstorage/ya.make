LIBRARY()

SRCS(
    ../group_write.cpp
    ../pdisk_log.cpp
    ../pdisk_read.cpp
    ../pdisk_write.cpp
    ../vdisk_write.cpp
)

PEERDIR(
    library/cpp/monlib/service/pages
    library/cpp/time_provider
    ydb/core/base
    ydb/core/blobstorage/backpressure
    ydb/core/blobstorage/base
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/vdisk/common
    ydb/core/control/lib
    ydb/core/jaeger_tracing
    ydb/core/load_test/common
    ydb/core/util
    ydb/library/actors/util
    ydb/library/yverify_stream
)

END()

RECURSE_FOR_TESTS(
    ut
)
