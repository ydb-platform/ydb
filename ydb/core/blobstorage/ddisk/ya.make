LIBRARY()

    SRCS(
        ddisk.cpp
        ddisk.h
        ddisk_actor.cpp
        ddisk_actor.h
    )

    PEERDIR(
        ydb/core/protos
        ydb/core/blobstorage/vdisk/common
    )

END()
