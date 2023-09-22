UNITTEST_FOR(ydb/library/yql/dq/opt)

SRCS(
    dq_cbo_ut.cpp
)

PEERDIR(
    ydb/library/yql/dq/opt
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/public/udf/service/stub
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
