UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)

    SRCS(
        blob_depot_fat.cpp
        blob_depot_test_functions.cpp
        blob_depot_event_managers.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
