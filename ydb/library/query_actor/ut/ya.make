UNITTEST_FOR(ydb/library/query_actor)

<<<<<<< HEAD
=======
IF (SANITIZER_TYPE OR WITH_VALGRIND)
    REQUIREMENTS(cpu:2)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

>>>>>>> 7bf789f021c (Main: Optimisation for medium and small tests cpu requirments (without split and fork) (#35835))
PEERDIR(
    ydb/core/testlib
    yql/essentials/sql/pg_dummy
)

SRCS(
    query_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
