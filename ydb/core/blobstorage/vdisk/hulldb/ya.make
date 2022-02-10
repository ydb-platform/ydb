LIBRARY() 
 
OWNER( 
    fomichev 
    g:kikimr 
) 
 
PEERDIR( 
    ydb/core/blobstorage/base 
    ydb/core/blobstorage/vdisk/common 
    ydb/core/blobstorage/vdisk/hulldb/barriers 
    ydb/core/blobstorage/vdisk/hulldb/base 
    ydb/core/blobstorage/vdisk/hulldb/compstrat 
    ydb/core/blobstorage/vdisk/hulldb/fresh 
    ydb/core/blobstorage/vdisk/hulldb/generic 
    ydb/core/blobstorage/vdisk/hullop 
    ydb/core/protos 
) 
 
SRCS( 
    blobstorage_hullgcmap.h 
    hulldb_bulksst_add.cpp 
    hulldb_bulksst_add.h 
    hulldb_bulksstloaded.h 
    hulldb_bulksstmngr.cpp 
    hulldb_bulksstmngr.h 
    hulldb_recovery.cpp 
    hulldb_recovery.h 
    hull_ds_all.h 
    hull_ds_all_snap.h 
    hullds_cache_block.cpp 
    hullds_cache_block.h 
) 
 
END() 
 
RECURSE( 
    barriers 
    base 
    compstrat 
    fresh 
    generic 
    test 
    ut 
) 
 
RECURSE_FOR_TESTS( 
    ut 
) 
