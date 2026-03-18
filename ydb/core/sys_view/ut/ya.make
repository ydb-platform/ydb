UNITTEST_FOR(ydb/core/sys_view)

FORK_SUBTESTS()

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/yson/node
    ydb/core/base
    ydb/core/kqp/ut/common
    ydb/core/persqueue/ut/common
    ydb/core/testlib/pg
    ydb/library/testlib/common
    ydb/public/sdk/cpp/src/client/draft
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_auth.cpp
    ut_kqp.cpp
    ut_tli.cpp
    ut_common.cpp
    ut_counters.cpp
    ut_labeled.cpp
    ut_registry.cpp
    ut_show_create.cpp
)

END()
