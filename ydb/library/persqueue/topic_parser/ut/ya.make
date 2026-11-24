UNITTEST_FOR(ydb/library/persqueue/topic_parser)

SIZE(SMALL)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/library/persqueue/topic_parser
)

SRCS(
    topic_names_converter_ut.cpp
)

END()
