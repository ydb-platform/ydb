UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        SIZE(MEDIUM)
    ELSE()
        SIZE(SMALL)
    ENDIF()

    SRCS(
        blob_depot.cpp
        blob_depot_test_functions.cpp
        blob_depot_event_managers.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
