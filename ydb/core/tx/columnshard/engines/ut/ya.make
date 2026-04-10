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
    ydb/core/tx/datashard
    yql/essentials/sql/pg_dummy
    yql/essentials/core/arrow_kernels/request
    ydb/core/testlib/default
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/formats/arrow/accessor/abstract

    yql/essentials/udfs/common/json2
)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    ut_program.cpp
    ut_snapshot_holders.cpp
    ut_script.cpp
<<<<<<< HEAD
    ut_predicate_ranges_builder.cpp
=======
    ut_minmax_serialization.cpp
>>>>>>> 688cd417bbb (fix nulls handling in minmax index (#37328))
    helper.cpp
)

END()