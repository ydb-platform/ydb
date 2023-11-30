LIBRARY()

SRCS(
    basic_test.cpp
    objectwithstate.cpp
)

PEERDIR(
    ydb/library/actors/protos
    ydb/core/base
    ydb/core/blobstorage/pdisk
    ydb/library/pdisk_io
    library/cpp/deprecated/atomic
)

END()
