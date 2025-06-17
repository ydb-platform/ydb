GTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/tests/integration/tests_common.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SPLIT_FACTOR(60)
    SIZE(LARGE)
    TAG(ya:fat)
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
