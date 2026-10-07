UNITTEST()

FORK_SUBTESTS()

SPLIT_FACTOR(60)

SIZE(SMALL)

SRCS(
    ut_top_cpu_usage.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/tx/schemeshard/common
)

END()
