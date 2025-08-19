UNITTEST_FOR(ydb/library/yql/providers/s3/provider)

TAG(ya:manual)

SRCS(
    yql_s3_listing_strategy_ut.cpp
)

PEERDIR(
    ydb/library/yql/minikql/dom
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
)

END()
