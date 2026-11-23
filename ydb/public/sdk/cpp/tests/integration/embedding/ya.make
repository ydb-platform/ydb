GTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/tests/integration/tests_common.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/value
)

SRCS(
    embedding_it.cpp
)

END()
