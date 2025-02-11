LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/vdisk/common
)

SRCS(
    defs.h
    dsproxy_mock.cpp
    dsproxy_mock.h
    dsproxy_mock_cp.cpp
    dsproxy_mock_cp.h
)

END()
