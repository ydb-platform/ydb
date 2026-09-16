UNITTEST()

SRCS(
    path_aliasing_ut.cpp
    utils_ut.cpp
)

PEERDIR(
    ydb/core/grpc_services/local_rpc
    ydb/core/path_aliasing/context
    ydb/services/persqueue_v1
    ydb/services/sqs_topic/queue_url
    library/cpp/getopt
    library/cpp/json
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/services/sqs_topic
)

END()
