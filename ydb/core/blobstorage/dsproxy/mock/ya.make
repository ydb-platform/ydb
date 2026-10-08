LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/contracts
)

SRCS(
    defs.h
    dsproxy_mock.cpp
    dsproxy_mock.h
)

END()
