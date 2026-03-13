UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

<<<<<<< HEAD
IF (SANITIZER_TYPE OR WITH_VALGRIND)
=======
REQUIREMENTS(cpu:2)
IF (WITH_VALGRIND)
>>>>>>> 7bf789f021c (Main: Optimisation for medium and small tests cpu requirments (without split and fork) (#35835))
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_flowcontrol_ut.cpp
    kqp_scan_ut.cpp
    kqp_split_ut.cpp
    kqp_point_consolidation_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
