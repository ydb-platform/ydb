UNITTEST_FOR(ydb/public/lib/idx_test)

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    ydb/public/lib/idx_test
)

SRCS(
    idx_test_data_provider_ut.cpp
)

END()
