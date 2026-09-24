UNITTEST()

PEERDIR(
    ydb/public/sdk/cpp/src/client/federated_topic
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/driver
    contrib/libs/grpc
    ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_json
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-sqs
)

TIMEOUT(350)

SRCS(
    federation_tests.cpp
    common_functions.cpp
    cluster_write_close_test.cpp
    sqs_compatibility_tests.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/federation_recipe/recipe.inc)

SIZE(MEDIUM)

END()
