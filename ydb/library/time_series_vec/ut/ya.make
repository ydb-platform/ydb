UNITTEST_FOR(ydb/library/time_series_vec)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    library/cpp/threading/future
)

SRCS(
    time_series_vec_ut.cpp
)

END()
