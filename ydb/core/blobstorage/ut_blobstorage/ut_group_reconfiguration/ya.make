UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

FORK_SUBTESTS()

#    SIZE(MEDIUM)
#    TIMEOUT(600)
SIZE(LARGE)

TIMEOUT(3600)

TAG(ya:fat)

SRCS(
    race.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

REQUIREMENTS(
    cpu:4
    ram:32
)

END()
