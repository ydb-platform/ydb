UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        FORK_SUBTESTS()
        SIZE(LARGE)
        TAG(ya:fat)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()

    SRCS(
        comp_defrag.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
