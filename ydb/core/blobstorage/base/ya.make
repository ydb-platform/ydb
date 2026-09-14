LIBRARY()

PEERDIR(
    contrib/libs/xxhash
    ydb/library/actors/wilson
    library/cpp/deprecated/atomic
    library/cpp/lwtrace
    ydb/core/base
    ydb/core/protos
    ydb/library/actors/util
)

SRCS(
    batched_vec.h
    blobstorage_checksum.cpp
    blobstorage_checksum.h
    blobstorage_events.cpp
    blobstorage_events.h
    blobstorage_host_record.h
    infer_pdisk_slot_count_settings.h
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
    vdisk_priorities.cpp
    utility.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
