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

SRCDIR(ydb/tests/functional/kafka)

SRCS(
    helpers.cpp
    produce_ut.cpp
    consume_ut.cpp
    balance_ut.cpp
    transactions_ut.cpp
    admin_ut.cpp
    groups_offsets_ut.cpp
    protocol_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
