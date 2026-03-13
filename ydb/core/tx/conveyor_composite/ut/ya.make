UNITTEST_FOR(ydb/core/tx/conveyor_composite/service)

FORK_SUBTESTS()

SPLIT_FACTOR(60)
SIZE(MEDIUM)

<<<<<<< HEAD
IF (SANITIZER_TYPE)
=======
REQUIREMENTS(cpu:4)
IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
>>>>>>> 7bf789f021c (Main: Optimisation for medium and small tests cpu requirments (without split and fork) (#35835))
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
