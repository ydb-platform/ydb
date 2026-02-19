LIBRARY()

SRCS(
    event_loop.cpp
    event_loop.h
    topic_sdk_test_setup.cpp
    topic_sdk_test_setup.h
    txusage_fixture.cpp
    txusage_fixture.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/tests/integration/topic/utils

    ydb/core/base
    ydb/core/persqueue/ut/common
    ydb/core/tx/schemeshard/ut_helpers

    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
