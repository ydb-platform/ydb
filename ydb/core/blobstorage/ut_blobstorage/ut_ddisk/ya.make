UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
        ddisk.cpp
        persistent_buffer_space_allocator.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
