GTEST(path_aliasing_it)

SIZE(LARGE)
TIMEOUT(1200)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
REQUIREMENTS(ram:32)

DEPENDS(
    ydb/apps/ydbd
    ydb/tests/functional/sdk/cpp/path_aliasing/recipe
)

USE_RECIPE(ydb/tests/functional/sdk/cpp/path_aliasing/recipe/path_aliasing_recipe)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/coordination
    ydb/public/sdk/cpp/src/client/discovery
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/import
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/rate_limiter
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/value
)

SRCS(path_aliasing_compact_it.cpp)

END()

RECURSE(recipe)
