LIBRARY()
    SRCS(
        defs.h
        group_state.cpp
        group_state.h
        mirror3dc_repair.cpp
        syncer.cpp
        syncer.h
    )

    PEERDIR(
        ydb/core/base
        ydb/core/blobstorage/vdisk/common
    )
END()
