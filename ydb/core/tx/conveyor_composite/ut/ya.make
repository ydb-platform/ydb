UNITTEST_FOR(ydb/core/tx/conveyor_composite/service)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/predictor
    ydb/core/kqp/runtime
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/testlib/default
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    yql/essentials/core/arrow_kernels/request
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/json2
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_config.cpp
    ut_config_subscription.cpp
    ut_runtime_update.cpp
    ut_scheduler.cpp
    ut_simple.cpp
)

END()
