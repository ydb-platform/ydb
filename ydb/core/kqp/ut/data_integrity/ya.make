UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

SIZE(SMALL)

SRCS(
    kqp_data_integrity_trails_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
)

YQL_LAST_ABI_VERSION()

END()
