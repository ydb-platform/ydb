LIBRARY()

    SRCS(
        ddisk.cpp
        ddisk.h
        ddisk_actor.cpp
        ddisk_actor.h
        ddisk_actor_connect.cpp
        ddisk_actor_read_write.cpp
    )

    PEERDIR(
        ydb/core/protos
        ydb/core/blobstorage/vdisk/common
    )

END()
