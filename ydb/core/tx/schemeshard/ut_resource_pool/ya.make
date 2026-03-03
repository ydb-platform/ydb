UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_resource_pool.cpp
)

END()
