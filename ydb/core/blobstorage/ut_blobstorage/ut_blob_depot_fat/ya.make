UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

    SRCS(
        blob_depot_fat.cpp
        blob_depot_test_functions.cpp
        blob_depot_event_managers.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
