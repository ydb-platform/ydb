LIBRARY()

PEERDIR(
    ydb/core/blobstorage/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/hulldb/bulksst_add
    ydb/core/protos
)

SRCS(
    hulldb_recovery.cpp
    hulldb_recovery.h
)

END()

