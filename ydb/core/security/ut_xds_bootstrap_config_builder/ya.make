UNITTEST_FOR(ydb/core/security)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    xds_bootstrap_config_builder_ut.cpp
)

END()
