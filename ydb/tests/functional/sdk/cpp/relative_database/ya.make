GTEST()

ENV(YDB_TEST_ROOT="sdk_tests")

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

DEPENDS(
    ydb/tests/functional/sdk/cpp/relative_database/recipe
)

USE_RECIPE(
    ydb/tests/functional/sdk/cpp/relative_database/recipe/recipe
)

REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/public/sdk/cpp/src/client/discovery
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/table
)

SRCS(
    relative_database_it.cpp
)

END()

RECURSE(
    recipe
)
