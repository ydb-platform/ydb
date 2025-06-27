LIBRARY()

SRCS(
    fixture.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/discovery
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/tests/integration/topic/utils
    library/cpp/testing/gtest
)

END()
