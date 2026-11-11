UNITTEST_FOR(ydb/core/kqp/host)

SIZE(MEDIUM)
FORK_SUBTESTS()

SRCS(kqp_lazy_provider_session_ut.cpp)

PEERDIR(
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

END()
