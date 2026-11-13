LIBRARY()

SRCS(mock.cpp)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/dsproxy/mock
    ydb/core/blobstorage/subsystem/interface
)

END()
