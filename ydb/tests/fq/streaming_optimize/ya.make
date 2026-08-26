PY3TEST()


TEST_SRCS(
    test_sql_negative.py
    test_sql_streaming.py
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
    FORK_SUBTESTS()
ENDIF()

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

DEPENDS(
    ydb/tests/tools/kqprun
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
