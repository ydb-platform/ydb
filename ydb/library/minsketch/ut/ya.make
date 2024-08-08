UNITTEST_FOR(ydb/library/minsketch)

FORK_SUBTESTS()
IF (WITH_VALGRIND)
    SPLIT_FACTOR(30)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    count_min_sketch_ut.cpp
)

END()
