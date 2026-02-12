LIBRARY()

    SRCS(
        ddisk.cpp
        ddisk.h
        ddisk_actor.cpp
        ddisk_actor.h
        ddisk_actor_boot.cpp
        ddisk_actor_chunks.cpp
        ddisk_actor_connect.cpp
        ddisk_actor_persistent_buffer.cpp
        ddisk_actor_read_write.cpp
        ddisk_actor_sync.cpp
        persistent_buffer_space_allocator.cpp
        segment_manager.cpp
    )

    PEERDIR(
        ydb/core/protos
        ydb/core/blobstorage/vdisk/common
    )

END()

RECURSE_FOR_TESTS(
    ut
)
