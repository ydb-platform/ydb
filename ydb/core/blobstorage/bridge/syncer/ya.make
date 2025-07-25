LIBRARY()
    SRCS(
        defs.h
        syncer.cpp
        syncer.h
    )

    PEERDIR(
        ydb/core/base
        ydb/core/blobstorage/vdisk/common
    )
END()
