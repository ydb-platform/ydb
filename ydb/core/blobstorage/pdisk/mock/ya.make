LIBRARY()

SRCS(
    pdisk_mock.cpp
    pdisk_mock.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/blobstorage/pdisk
)

END()
