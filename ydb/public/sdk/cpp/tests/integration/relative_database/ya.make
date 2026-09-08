GTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/tests/integration/tests_common.inc)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

DEPENDS(
    ydb/apps/ydbd
    ydb/public/sdk/cpp/tests/integration/relative_database/recipe
)

USE_RECIPE(
    ydb/public/sdk/cpp/tests/integration/relative_database/recipe/recipe
)

FORK_SUBTESTS()
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
