UNITTEST_FOR(ydb/core/memory_controller)

SIZE(SMALL)

TIMEOUT(60)

PEERDIR(
    ydb/library/yql/sql/pg_dummy
    ydb/core/testlib
)

SRCS(
    memory_controller_ut.cpp
    memtable_collection_ut.cpp
)

END()
