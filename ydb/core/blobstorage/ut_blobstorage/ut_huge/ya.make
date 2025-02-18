UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SRCS(
    huge.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    REQUIREMENTS(ram:32)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
