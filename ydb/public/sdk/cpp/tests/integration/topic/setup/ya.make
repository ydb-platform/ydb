LIBRARY()

SRCS(
    fixture.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/discovery
    ydb/public/sdk/cpp/src/client/topic
    library/cpp/testing/gtest
)

END()
