UNITTEST_FOR(ydb/library/yql/core/cbo)

TAG(ya:manual)

SRCS(
    cbo_optimizer_ut.cpp
)

PEERDIR(
    ydb/library/yql/core/cbo
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/public/udf/service/stub
)

SIZE(SMALL)

END()
