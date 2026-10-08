UNITTEST()

FORK_SUBTESTS()
SIZE(SMALL)

SRCS(runtime_ut.cpp)

PEERDIR(
    ydb/core/testlib/basics/core
    ydb/core/blobstorage/subsystem/mock
    yql/essentials/sql/pg_dummy
)

END()
