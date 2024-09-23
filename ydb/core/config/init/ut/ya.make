UNITTEST_FOR(ydb/core/config/init)

SRCS(
    init_ut.cpp
)

PEERDIR(
    ydb/core/config/init
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

END()
