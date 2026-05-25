UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
        ddisk.cpp
        nbs_dbg_like_load_tablet_ut.cpp
        persistent_buffer_space_allocator.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
        ydb/core/load_test
    )

END()
