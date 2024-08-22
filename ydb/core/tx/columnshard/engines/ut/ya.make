UNITTEST_FOR(ydb/core/tx/columnshard/engines)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/base
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/counters
    #ydb/library/yql/sql/pg_dummy
    ydb/library/yql/core/arrow_kernels/request
    ydb/core/testlib/default
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing

    ydb/library/yql/udfs/common/json2
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_insert_table.cpp
    ut_logs_engine.cpp
    ut_program.cpp
    helper.cpp
)

END()
