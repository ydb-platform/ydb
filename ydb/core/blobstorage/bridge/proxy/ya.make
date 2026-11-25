LIBRARY()

    SRCS(
        bridge_proxy.cpp
        bridge_proxy.h
        bridge_proxy_counters.h
        defs.h
    )

    PEERDIR(
        ydb/core/blobstorage/dsproxy
        ydb/core/blobstorage/groupinfo
    )

END()
