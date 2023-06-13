LIBRARY()

SRCS(
    pdisk_mock.cpp
    pdisk_mock.h
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/blobstorage/pdisk
)

END()
