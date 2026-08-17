GTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/tests/integration/tests_common.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/metrics
    ydb/public/sdk/cpp/src/client/impl/observability/error_category
)

YQL_LAST_ABI_VERSION()

END()
