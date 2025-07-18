UNITTEST_FOR(ydb/core/tx/conveyor_composite/service)

FORK_SUBTESTS()

SPLIT_FACTOR(60)
SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/base
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/counters
    yql/essentials/sql/pg_dummy
    yql/essentials/core/arrow_kernels/request
    ydb/core/testlib/default
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing

    yql/essentials/udfs/common/json2
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_simple.cpp
)

END()
