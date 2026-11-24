UNITTEST_FOR(ydb/core/tablet_flat)

SIZE(MEDIUM)

SRCS(
    memtable_allocation_ut.cpp
    util_pool_ut.cpp
    util_string_ut.cpp
)

PEERDIR(
    yql/essentials/public/decimal
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
