LIBRARY()

SRCS(
    blob_recovery.cpp
    blob_recovery.h
    blob_recovery_process.cpp
    blob_recovery_queue.cpp
    blob_recovery_request.cpp
    defs.h
    restore_corrupted_blob_actor.cpp
    restore_corrupted_blob_actor.h
    scrub_actor.cpp
    scrub_actor.h
    scrub_actor_huge.cpp
    scrub_actor_huge_blob_merger.h
    scrub_actor_impl.h
    scrub_actor_mon.cpp
    scrub_actor_pdisk.cpp
    scrub_actor_snapshot.cpp
    scrub_actor_sst.cpp
    scrub_actor_sst_blob_merger.h
    scrub_actor_unreadable.cpp
)

PEERDIR(
    ydb/core/blobstorage/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/skeleton
)

END()
