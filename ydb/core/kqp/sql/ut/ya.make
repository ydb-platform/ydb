UNITTEST_FOR(ydb/core/kqp/sql)

SRCS(
    translator_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
)

END()
