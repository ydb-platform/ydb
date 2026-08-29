UNITTEST_FOR(ydb/library/persqueue/topic_parser)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/library/persqueue/topic_parser
)

SRCS(
    consumer_helpers_ut.cpp
)

END()
