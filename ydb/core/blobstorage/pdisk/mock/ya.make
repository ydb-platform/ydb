LIBRARY()

SRCS(
    pdisk_mock.cpp
    pdisk_mock.h
    subsystem.cpp
    subsystem.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/blobstorage/pdisk
)

END()
