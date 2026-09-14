UNITTEST()

SRCS(
    utils_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/json
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/services/sqs_topic
)

END()
