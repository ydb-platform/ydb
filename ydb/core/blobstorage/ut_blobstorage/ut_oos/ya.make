UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    FORK_SUBTESTS()

    SIZE(MEDIUM)
    IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
        REQUIREMENTS(cpu:1 ram:16)
    ELSE()
        REQUIREMENTS(cpu:2 ram:16)
    ENDIF()

    SRCS(
        out_of_space.cpp
        fresh_space_projection.cpp
        huge_space_admission.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()
