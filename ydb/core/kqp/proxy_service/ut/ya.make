UNITTEST_FOR(ydb/core/kqp/proxy_service)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_proxy_ut.cpp
)

PEERDIR(
    ydb/core/kqp/proxy_service
    ydb/core/kqp/ut/common
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
