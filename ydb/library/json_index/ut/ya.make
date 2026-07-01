UNITTEST_FOR(ydb/library/json_index)

SIZE(SMALL)

PEERDIR(
    yql/essentials/minikql/jsonpath/parser
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    json_index_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
