UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    put_request_history.cpp
)

PEERDIR(
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()
