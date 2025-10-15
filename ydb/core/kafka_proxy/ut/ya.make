UNITTEST_FOR(ydb/core/kafka_proxy)

ADDINCL(
    ydb/public/sdk/cpp
)

SIZE(medium)
SRCS(
    kafka_test_client.cpp
    ut_kafka_functions.cpp
    ut_protocol.cpp
    ut_serialization.cpp
    metarequest_ut.cpp
    ut_transaction_coordinator.cpp
    ut_transaction_actor.cpp
    ut_produce_actor.cpp
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
