LIBRARY()

    SRCS(
        bridge_proxy.cpp  # gn: into ydb/core/blobstorage/dsproxy
        bridge_proxy.h
        defs.h
    )

    PEERDIR(
        ydb/core/blobstorage/dsproxy
        ydb/core/blobstorage/groupinfo
    )

END()
