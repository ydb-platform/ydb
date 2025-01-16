UNITTEST_FOR(ydb/library/yql/providers/s3/provider)

SRCS(
    yql_s3_listing_strategy_ut.cpp
)

PEERDIR(
    yql/essentials/minikql/dom
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
)

END()
