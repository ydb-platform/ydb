LIBRARY()

PEERDIR(
    library/cpp/actors/wilson
    library/cpp/deprecated/atomic
    library/cpp/lwtrace
    ydb/core/protos
)

SRCS(
    batched_vec.h
    blobstorage_events.h
    blobstorage_oos_defs.h
    blobstorage_vdiskid.cpp
    blobstorage_vdiskid.h
    blobstorage_syncstate.cpp
    blobstorage_syncstate.h
    defs.h
    html.cpp
    html.h
    ptr.h
    vdisk_lsn.h
    vdisk_sync_common.h
    vdisk_priorities.h
    utility.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
