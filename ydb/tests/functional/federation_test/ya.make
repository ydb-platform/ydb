UNITTEST()

PEERDIR(
    ydb/public/sdk/cpp/src/client/federated_topic
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/driver
    ydb/tests/functional/kafka/test_common
    contrib/libs/grpc
    contrib/libs/librdkafka/src-cpp
)

TIMEOUT(350)

ADDINCL(
    contrib/libs/librdkafka/src-cpp
    contrib/libs/librdkafka/include
)

SRCS(
    federation_tests.cpp
    common_functions.cpp
    cluster_write_close_test.cpp
    kafka_compatibility_tests.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/federation_recipe/recipe.inc)

SIZE(MEDIUM)

END()
