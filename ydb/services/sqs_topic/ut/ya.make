UNITTEST()

SRCS(
    consumer_attributes_ut.cpp
    utils_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/services/sqs_topic
)

END()
