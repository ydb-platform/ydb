UNITTEST_FOR(ydb/public/lib/idx_test)

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

FORK_SUBTESTS()

PEERDIR(
    ydb/public/lib/idx_test
)

SRCS(
    idx_test_data_provider_ut.cpp
)

END()
