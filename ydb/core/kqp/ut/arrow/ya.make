UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SPLIT_FACTOR(5)
SIZE(MEDIUM)

SRCS(
    kqp_arrow_in_channels_ut.cpp
    kqp_types_arrow_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    #ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
