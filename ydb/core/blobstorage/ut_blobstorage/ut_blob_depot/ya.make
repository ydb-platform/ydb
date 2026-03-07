UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    FORK_TESTS()
    FORK_TEST_FILES()
    FORK_SUBTESTS()
    SPLIT_FACTOR(20)


    SRCS(
        blob_depot.cpp
        blob_depot_test_functions.cpp
        blob_depot_event_managers.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
