UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        FORK_SUBTESTS()
    ENDIF()

    SIZE(MEDIUM)
    TIMEOUT(600)

    SRCS(
        comp_defrag.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
