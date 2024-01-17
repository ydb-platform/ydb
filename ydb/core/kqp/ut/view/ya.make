UNITTEST_FOR(ydb/core/kqp)

OWNER(g:kikimr)

SIZE(MEDIUM)

SRCS(
    view_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/library/yql/sql

    ydb/core/testlib/basics/default
)

YQL_LAST_ABI_VERSION()

END()
