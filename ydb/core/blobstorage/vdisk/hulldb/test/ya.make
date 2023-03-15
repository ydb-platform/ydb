LIBRARY()

PEERDIR(
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/hulldb/generic
    ydb/core/protos
)

SRCS(
    defs.h
    testhull_index.h
    testhull_index.cpp
)

END()
