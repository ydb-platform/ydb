GTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/tests/integration/tests_common.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/jwt-cpp
    ydb/public/sdk/cpp/src/client/common_client
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/types/credentials/login
)

SRCS(
    login_it.cpp
)

END()
