UNITTEST_FOR(ydb/core/config/init)

SRCS(
    init_ut.cpp
)

PEERDIR(
    ydb/core/config/init
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

END()
