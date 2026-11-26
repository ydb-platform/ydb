UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    IF (SANITIZER_TYPE)
        SIZE(MEDIUM)
    ELSE()
        SIZE(SMALL)
    ENDIF()

    SRCS(
        blob_depot.cpp
        blob_depot_move_data.cpp
        blob_depot_test_functions.cpp
        blob_depot_event_managers.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

    IF (OS_WINDOWS)
        CFLAGS(
            -DKIKIMR_DISABLE_S3_OPS
        )
    ELSE()
        PEERDIR(
            ydb/core/wrappers/ut_helpers
            ydb/library/actors/http
            ydb/library/aws_init
        )
    ENDIF()

    YQL_LAST_ABI_VERSION()

END()
