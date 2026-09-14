GTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/tests/integration/tests_common.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

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
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
)

SRCS(
    relative_database.cpp
)

YQL_LAST_ABI_VERSION()

END()
