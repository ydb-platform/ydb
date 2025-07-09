PY3TEST()

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(4)

TEST_SRCS(
    test_sql_streaming.py
)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

DEPENDS(
    ydb/tests/tools/fqrun
    yql/essentials/tools/astdiff
    yql/essentials/tools/sql2yql
    yql/essentials/tests/common/test_framework/udfs_deps
)

DATA(
    arcadia/ydb/tests/fq/streaming_optimize/cfg
    arcadia/ydb/tests/fq/streaming_optimize/suites
)

PEERDIR(
    ydb/tests/fq/tools
    yql/essentials/tests/common/test_framework
)

END()
