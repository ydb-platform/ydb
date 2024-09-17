UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(LARGE)

TIMEOUT(3600)

TAG(ya:fat)

SRCS(
    replication.cpp
    replication_huge.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ENDIF()

END()
