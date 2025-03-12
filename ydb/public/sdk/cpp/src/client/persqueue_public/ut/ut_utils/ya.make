LIBRARY()

SRCS(
    data_plane_helpers.h
    data_plane_helpers.cpp
    sdk_test_setup.h
    test_utils.h
    test_server.h
    test_server.cpp
    ut_utils.h
    ut_utils.cpp
)

PEERDIR(
    ydb/library/grpc/server
    library/cpp/testing/unittest
    library/cpp/threading/chunk_queue
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()
