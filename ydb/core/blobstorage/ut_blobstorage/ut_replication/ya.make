UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(LARGE)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

SRCS(
    replication.cpp
    replication_huge.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
