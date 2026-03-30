UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

<<<<<<< HEAD
IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
=======

REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE == "thread")
>>>>>>> 1b958331553 (Main:  Optimisation for medium and small tests cpu requirments + split factor (#35969))
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    library/cpp/json
)

SRCS(
    ut_continuous_backup_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
