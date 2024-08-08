UNITTEST_FOR(ydb/library/time_series_vec)

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

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    library/cpp/threading/future
)

SRCS(
    time_series_vec_ut.cpp
)

END()
