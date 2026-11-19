LIBRARY()
    SRCS(
        defs.h
        mirror3dc_repair.cpp
        syncer.cpp
        syncer.h
    )

    PEERDIR(
        ydb/core/base
        ydb/core/blobstorage/vdisk/common
    )
END()
