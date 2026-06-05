UNITTEST_FOR(ydb/core/tx/columnshard/engines/storage/optimizer)

SIZE(SMALL)

PEERDIR(
    ydb/core/tx/columnshard
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_tiling.cpp
)

END()
