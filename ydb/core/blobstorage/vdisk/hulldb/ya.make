LIBRARY()

PEERDIR(
    ydb/core/blobstorage/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/hulldb/barriers
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/hulldb/compstrat
    ydb/core/blobstorage/vdisk/hulldb/fresh
    ydb/core/blobstorage/vdisk/hulldb/generic
    ydb/core/blobstorage/vdisk/hulldb/recovery
    ydb/core/blobstorage/vdisk/hulldb/bulksst_add
    ydb/core/protos
)

SRCS(
    blobstorage_hullgcmap.h
    hull_ds_all.h
    hull_ds_all_snap.h
)

END()

RECURSE(
    barriers
    base
    bulksst_add
    cache_block
    compstrat
    fresh
    generic
    recovery
    test
)
