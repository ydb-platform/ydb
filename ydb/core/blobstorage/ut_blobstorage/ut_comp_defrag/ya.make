UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
    IF (SANITIZER_TYPE)
        SIZE(LARGE)
        INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
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
