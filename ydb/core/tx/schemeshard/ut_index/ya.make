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
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
)

SRCS(
    ut_async_index.cpp
    ut_unique_index.cpp
    ut_vector_index.cpp
    ut_fulltext_index.cpp
)

YQL_LAST_ABI_VERSION()

END()
