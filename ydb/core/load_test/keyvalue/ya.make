LIBRARY()

SRCS(
    ../keyvalue_write.cpp
)

PEERDIR(
    library/cpp/histogram/hdr
    library/cpp/monlib/service/pages
    library/cpp/time_provider
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/pdisk
    ydb/core/control/lib
    ydb/core/keyvalue
    ydb/core/load_test/common
)

END()
