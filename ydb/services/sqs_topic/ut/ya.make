GTEST()

SRCS(
    utils_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    yql/essentials/sql/pg_dummy
    ydb/core/util/actorsys_test
    ydb/services/sqs_topic
)

END()
