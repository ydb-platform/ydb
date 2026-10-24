GTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/tests/integration/tests_common.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ELSE()
    REQUIREMENTS(ram:4 cpu:1)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    SPLIT_FACTOR(60)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/query
)

YQL_LAST_ABI_VERSION()

END()
