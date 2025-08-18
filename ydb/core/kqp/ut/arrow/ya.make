UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SPLIT_FACTOR(5)
SIZE(MEDIUM)

SRCS(
    kqp_arrow_in_channels_ut.cpp
    kqp_types_arrow_ut.cpp
    kqp_result_set_format_arrow.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
    ydb/public/sdk/cpp/src/client/arrow
)

YQL_LAST_ABI_VERSION()

END()
