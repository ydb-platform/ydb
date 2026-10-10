UNITTEST_FOR(ydb/library/persqueue/topic_parser)

FORK_SUBTESTS()

SIZE(SMALL)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/library/persqueue/topic_parser
)

SRCS(
    topic_names_converter_ut.cpp
)

END()
