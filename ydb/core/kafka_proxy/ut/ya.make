UNITTEST_FOR(ydb/core/kafka_proxy)

SRCS(
    ut_kafka_functions.cpp
    ut_serialization.cpp
)

PEERDIR(
    ydb/core/kafka_proxy
)

END()
