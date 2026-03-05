UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    FORK_SUBTESTS()

    SIZE(MEDIUM)
    IF (SANITIZER_TYPE)
        REQUIREMENTS(cpu:1)
    ENDIF()
    REQUIREMENTS(cpu:2)

    SRCS(
        donor.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
