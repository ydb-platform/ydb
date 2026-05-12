UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SRCS(
    huge.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

END()
