UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SIZE(SMALL)

SRCS(
    kqp_is_query_allowed_to_log_ut.cpp
    kqp_mask_literals_ut.cpp
    kqp_tli_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
