LIBRARY()

PEERDIR(
    ydb/core/blobstorage/vdisk/hulldb/barriers
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/hulldb/generic
    ydb/core/blobstorage/vdisk/hulldb/cache_block
    ydb/core/blobstorage/vdisk/hulldb/recovery
    ydb/core/blobstorage/vdisk/hulldb/bulksst_add
    ydb/core/blobstorage/vdisk/hulldb/compstrat
    ydb/core/blobstorage/vdisk/hullop/hullcompdelete
    ydb/core/blobstorage/vdisk/synclog
    ydb/core/protos
)

SRCS(
    blobstorage_buildslice.h
    blobstorage_hull.cpp
    blobstorage_hull.h
    blobstorage_hullactor.h
    blobstorage_hullactor.cpp
    blobstorage_hullcommit.h
    blobstorage_hullcompactdeferredqueue.h
    blobstorage_hullcompact.h
    blobstorage_hullcompactworker.h
    blobstorage_hullload.h
    blobstorage_hulllog.cpp
    blobstorage_hulllog.h
    blobstorage_hulllogcutternotify.cpp
    blobstorage_hulllogcutternotify.h
    blobstorage_readbatch.h
    defs.h
    hullop_compactfreshappendix.cpp
    hullop_compactfreshappendix.h
    hullop_delayedresp.h
    hullop_entryserialize.cpp
    hullop_entryserialize.h
)

END()

RECURSE(
    hullcompdelete
)

RECURSE_FOR_TESTS(
    ut
)
