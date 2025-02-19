UNITTEST_FOR(yql/essentials/core/histogram)

FORK_SUBTESTS()
IF (WITH_VALGRIND)
    SPLIT_FACTOR(30)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    eq_width_histogram_ut.cpp
)

END()
