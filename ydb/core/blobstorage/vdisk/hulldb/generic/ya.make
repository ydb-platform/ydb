LIBRARY()

PEERDIR(
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/hulldb/fresh
    ydb/core/blobstorage/vdisk/protos
    ydb/core/protos
)

SRCS(
    blobstorage_hulldatamerger.h
    blobstorage_hullmergeits.h
    blobstorage_hulloptlsn.cpp
    blobstorage_hulloptlsn.h
    blobstorage_hullrecmerger.h
    blobstorage_hullwritesst.h
    defs.h
    hulldb_bulksstloaded.h
    hulldb_bulksstmngr.cpp
    hulldb_bulksstmngr.h
    hullds_idx.cpp
    hullds_idx.h
    hullds_idxsnap.cpp
    hullds_idxsnap.h
    hullds_idxsnap_it.h
    hullds_leveledssts.h
    hullds_sst.cpp
    hullds_sst.h
    hullds_sst_it.h
    hullds_sstslice.cpp
    hullds_sstslice.h
    hullds_sstslice_it.h
    hullds_sstvec.cpp
    hullds_sstvec.h
    hullds_sstvec_it.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
