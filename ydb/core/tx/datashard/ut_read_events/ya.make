UNITTEST_FOR(ydb/core/tx/datashard)

FORK_SUBTESTS()
SIZE(SMALL)

SRCS(
    read_events_ut.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/tx/datashard
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
