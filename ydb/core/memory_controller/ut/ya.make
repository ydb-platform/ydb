UNITTEST_FOR(ydb/core/memory_controller)

SIZE(SMALL)

TIMEOUT(60)

PEERDIR(
    ydb/library/yql/sql/pg_dummy
    ydb/core/testlib
    ydb/core/tx/datashard/ut_common
    ydb/core/tablet_flat
    library/cpp/testing/unittest
)

SRCS(
    memory_controller_ut.cpp
    memtable_collection_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
