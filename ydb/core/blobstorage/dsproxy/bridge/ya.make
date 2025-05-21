LIBRARY()

    SRCS(
        bridge.cpp
        bridge.h
        defs.h
    )

    PEERDIR(
        ydb/core/blobstorage/dsproxy
        ydb/core/blobstorage/groupinfo
    )

END()
