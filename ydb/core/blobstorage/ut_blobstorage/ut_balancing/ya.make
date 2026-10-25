UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)
    IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
        REQUIREMENTS(cpu:1)
    ELSE()
        REQUIREMENTS(cpu:2)
    ENDIF()

    FORK_SUBTESTS()

    SRCS(
        balancing.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
