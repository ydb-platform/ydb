UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(MEDIUM)

REQUIREMENTS(ram:32 cpu:4)

SRCS(
    emergency_recovery.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
