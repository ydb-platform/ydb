UNITTEST_FOR(ydb/core/kqp)

SIZE(MEDIUM)

SRCS(
    view_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/utils/log

    ydb/core/testlib/basics/default
)

DATA(arcadia/ydb/core/kqp/ut/view/input)

YQL_LAST_ABI_VERSION()

END()
