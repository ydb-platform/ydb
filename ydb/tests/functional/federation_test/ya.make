UNITTEST()

PEERDIR(
    ydb/public/sdk/cpp/src/client/federated_topic
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/driver
    contrib/libs/grpc
)

TIMEOUT(350)

SRCS(
    federation_tests.cpp
    common_functions.cpp
    cluster_write_close_test.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/federation_recipe/recipe.inc)

SIZE(MEDIUM)

END()
