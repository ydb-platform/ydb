UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
    SPLIT_FACTOR(11)
ENDIF()

SRCS(
    vdisk_internals.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
