GTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/tests/integration/tests_common.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/grpc
    library/cpp/http/server
    library/cpp/json
    library/cpp/testing/common
    ydb/public/api/client/yc_public/iam
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/types/core_facility
    ydb/public/sdk/cpp/tests/integration/iam/helpers
)

SRCS(
    iam_test_fixture.cpp
    jwt_it.cpp
    metadata_it.cpp
    oauth_it.cpp
)

END()
