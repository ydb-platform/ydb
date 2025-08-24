UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SRCS(
    phantom_blobs.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
# REQUIREMENTS(ram:32)

END()
