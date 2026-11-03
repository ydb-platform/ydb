UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    state_server_recovery_ut.cpp
)

PEERDIR(
    ydb/core/test_tablet
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
