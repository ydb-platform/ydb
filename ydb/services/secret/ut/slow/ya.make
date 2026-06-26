UNITTEST_FOR(ydb/services/secret)

SIZE(LARGE)

TAG(
    ya:fat
)

PEERDIR(
    ydb/services/secret
    ydb/services/secret/ut/common
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

SRCS(
    describe_schema_secrets_service_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
