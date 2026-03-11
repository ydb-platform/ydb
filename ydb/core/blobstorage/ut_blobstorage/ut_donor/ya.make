UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    FORK_SUBTESTS()

    SIZE(MEDIUM)
    IF (SANITIZER_TYPE)
        REQUIREMENTS(cpu:1)
    ELSE()
        REQUIREMENTS(cpu:2)
    ENDIF()

    SRCS(
        donor.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
