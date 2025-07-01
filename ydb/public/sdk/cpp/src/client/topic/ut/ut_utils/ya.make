LIBRARY()

SRCS(
    topic_sdk_test_setup.cpp
    topic_sdk_test_setup.h
)

PEERDIR(
    ydb/library/grpc/server
    library/cpp/testing/unittest
    library/cpp/threading/chunk_queue
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/topic

    ydb/public/sdk/cpp/tests/integration/topic/utils
)

YQL_LAST_ABI_VERSION()

END()
