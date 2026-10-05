UNITTEST()

PEERDIR(
    ydb/public/sdk/cpp/src/client/federated_topic
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/driver
    contrib/libs/grpc
    contrib/libs/librdkafka/src-cpp
)

TIMEOUT(350)

# One chunk per test case. A shared chunk dies at TIMEOUT(350) after the
# mirror tests, which kills DisableWriteOnClusterA. Medium max is 600s,
# so the cases cannot share one budget. Keep this factor >= the test count.
FORK_SUBTESTS()
SPLIT_FACTOR(8)

ADDINCL(
    contrib/libs/librdkafka/src-cpp
    contrib/libs/librdkafka/include
)

SRCS(
    federation_tests.cpp
    common_functions.cpp
    cluster_write_close_test.cpp
    ../kafka/test_common/helpers.cpp
    kafka_compatibility_tests.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/federation_recipe/recipe.inc)

SIZE(MEDIUM)

REQUIREMENTS(ram:16)

END()
