UNITTEST_FOR(ydb/core/tx/columnshard/engines/storage/optimizer)

SIZE(SMALL)

PEERDIR(
    ydb/core/testlib/actors
    ydb/core/testlib/basics
    ydb/core/tx/columnshard
    ydb/core/tx/columnshard/test_helper
    ydb/library/actors/core
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_lcbuckets_skip_level.cpp
    ut_tiling.cpp
)

END()
