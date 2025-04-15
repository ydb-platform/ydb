UNITTEST_FOR(ydb/core/kafka_proxy)

ADDINCL(
    ydb/public/sdk/cpp
)

SIZE(medium)
SRCS(
    ut_kafka_functions.cpp
    ut_protocol.cpp
    ut_serialization.cpp
    kafka_test_client.cpp
    metarequest_ut.cpp
    actors_ut.cpp
)

PEERDIR(
    ydb/core/kafka_proxy
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils

)
YQL_LAST_ABI_VERSION()
END()
