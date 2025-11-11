UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SPLIT_FACTOR(5)
SIZE(MEDIUM)

SRCS(
    kqp_arrow_in_channels_ut.cpp
    kqp_types_arrow_ut.cpp
    kqp_result_set_formats_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/public/sdk/cpp/src/client/arrow
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
