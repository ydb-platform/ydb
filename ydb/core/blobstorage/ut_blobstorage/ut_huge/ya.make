UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SRCS(
    huge.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

REQUIREMENTS(ram:32 cpu:4)
IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
