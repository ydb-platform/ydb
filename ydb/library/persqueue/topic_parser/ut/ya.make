UNITTEST_FOR(ydb/library/persqueue/topic_parser)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/persqueue/public/nameresolver
    ydb/library/persqueue/topic_parser
)

SRCS(
    topic_names_converter_ut.cpp
)

END()
