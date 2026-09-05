UNITTEST_FOR(ydb/core/kqp/opt/rbo/verification/benchmark_policy)

SIZE(SMALL)

SRCS(
    coverage_policy_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
)

DATA(
    arcadia/ydb/core/kqp/opt/rbo/verification/benchmark_ut/coverage_policy.json
)

END()
