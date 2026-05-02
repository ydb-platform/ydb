UNITTEST_FOR(ydb/core/tx/columnshard/engines/reader/simple_reader/iterator)

SIZE(MEDIUM)

TIMEOUT(600)

SRCS(
    ut_streaming.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/library/actors/testlib
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()