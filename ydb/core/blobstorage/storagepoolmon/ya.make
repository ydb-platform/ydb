LIBRARY()

SRCS(
    storagepool_counters.h
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/core/base
    ydb/core/blobstorage/base
)

END()

RECURSE_FOR_TESTS(
    ut
)
