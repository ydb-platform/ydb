UNITTEST_FOR(ydb/library/yql/sql/v0)

SRCS(
    sql_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/sql/pg_dummy
)

TIMEOUT(300)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

END()
