UNITTEST_FOR(ydb/core/driver_lib/run)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    auto_config_initializer_ut.cpp
)

END()
