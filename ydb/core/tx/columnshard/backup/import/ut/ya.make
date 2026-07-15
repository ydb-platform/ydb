UNITTEST_FOR(ydb/core/tx/columnshard)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR SANITIZER_TYPE == "memory")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    ydb/library/testlib/s3_recipe_helper
    ydb/public/lib/yson_value
    ydb/services/metadata
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

SRCS(
    ut_import.cpp
)

END()
