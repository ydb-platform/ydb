UNITTEST()

ADDINCL(
    ydb/public/sdk/cpp
)

SRCS(
    dummy_provider_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/api/grpc
)


INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)

END()
