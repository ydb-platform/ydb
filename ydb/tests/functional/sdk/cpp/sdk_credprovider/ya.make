UNITTEST()

SRCS(
    dummy_provider_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/api/grpc
)


INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

REQUIREMENTS(ram:16)

END()
