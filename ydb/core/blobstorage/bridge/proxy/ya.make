LIBRARY()

    SRCS(
        bridge_proxy.cpp
        bridge_proxy.h
        defs.h
    )

    PEERDIR(
        ydb/core/blobstorage/dsproxy
        ydb/core/blobstorage/groupinfo
    )

END()
