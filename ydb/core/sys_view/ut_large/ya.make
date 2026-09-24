UNITTEST_FOR(ydb/core/sys_view)

FORK_SUBTESTS()

SIZE(LARGE)
TAG(ya:fat)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    library/cpp/testing/unittest
    ydb/core/kqp/ut/common
    ydb/core/persqueue/ut/common
    ydb/core/testlib/pg
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_large.cpp
    ut_common.cpp
)

END()
