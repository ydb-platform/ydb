UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
TIMEOUT(3600)
REQUIREMENTS(cpu:1)
REQUIREMENTS(ram:16)

# The benchmark fixture pulls in enough debug information to exceed DWARF32
# relocation limits in relwithdebinfo builds. Strip only this test executable.
IF (OS_LINUX)
    LDFLAGS(-Wl,--strip-debug)
ENDIF()

DEPENDS(
    ydb/apps/ydb
)

SRCS(
    kqp_analyzed_correctness_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/rbo/statistics/helpers
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

DATA(
    arcadia/ydb/core/kqp/ut/rbo/statistics/data
    arcadia/ydb/core/kqp/ut/rbo/data/yql-tpch
    arcadia/ydb/core/kqp/ut/rbo/data/yql-tpcds
)

YQL_LAST_ABI_VERSION()

END()
