UNITTEST_FOR(ydb/library/yql/providers/dq/provider)

TAG(ya:manual)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/public/udf/service/stub
)

SRCS(
    yql_dq_provider_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
