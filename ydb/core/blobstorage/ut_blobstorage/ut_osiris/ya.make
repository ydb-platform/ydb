UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(LARGE)

TIMEOUT(3600)

TAG(ya:fat)

SRCS(
    osiris.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

REQUIREMENTS(
    cpu:1
    ram:32
)

END()
