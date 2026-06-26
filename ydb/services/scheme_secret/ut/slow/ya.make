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
    service_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
