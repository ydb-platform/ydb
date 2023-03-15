UNITTEST_FOR(ydb/library/persqueue/topic_parser)

FORK_SUBTESTS()

TIMEOUT(30)

SIZE(SMALL)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/library/persqueue/topic_parser
)

SRCS(
    topic_names_converter_ut.cpp
)

END()
