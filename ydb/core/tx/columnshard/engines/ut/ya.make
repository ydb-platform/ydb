UNITTEST_FOR(ydb/core/tx/columnshard/engines)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/base
    ydb/core/formats/arrow
    ydb/core/kqp/ut/common
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines/predicate
    yql/essentials/sql/pg_dummy
    yql/essentials/core/arrow_kernels/request
    ydb/core/testlib/default
    ydb/core/tx/columnshard
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/formats/arrow/accessor/abstract
    ydb/core/formats/arrow/program
    ydb/core/tx/columnshard/engines/storage/indexes/min_max

    yql/essentials/udfs/common/json2
)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    ut_program.cpp
    ut_snapshot_holders.cpp
    ut_scan_snapshot_guard.cpp
    ut_script.cpp
    ut_minmax_index.cpp
    ut_predicate_ranges_builder.cpp
    helper.cpp
)

END()