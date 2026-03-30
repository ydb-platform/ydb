UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(80)

<<<<<<< HEAD
IF (SANITIZER_TYPE OR WITH_VALGRIND)
=======
REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE)
>>>>>>> 1b958331553 (Main:  Optimisation for medium and small tests cpu requirments + split factor (#35969))
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/persqueue/writer
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_cdc_stream_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
