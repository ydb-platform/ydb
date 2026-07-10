UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()
SPLIT_FACTOR(11)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ENDIF()

SRCS(
    vdisk_internals.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
