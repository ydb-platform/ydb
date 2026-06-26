UNITTEST_FOR(ydb/services/scheme_secret)

SIZE(LARGE)

TAG(
    ya:fat
)

PEERDIR(
    ydb/services/scheme_secret
    ydb/services/scheme_secret/ut/common
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

SRCS(
    describe_schema_secrets_service_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
