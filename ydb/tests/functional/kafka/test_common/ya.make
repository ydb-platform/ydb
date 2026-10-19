LIBRARY()

PEERDIR(
    contrib/libs/librdkafka/src-cpp
    library/cpp/testing/unittest
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/topic
)

ADDINCL(
    contrib/libs/librdkafka/src-cpp
    contrib/libs/librdkafka/include
)

SRCS(
    helpers.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    suites
)
