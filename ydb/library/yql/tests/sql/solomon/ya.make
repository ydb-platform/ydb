PY2TEST()

TEST_SRCS(
    test.py
)

SIZE(MEDIUM)

NO_CHECK_IMPORTS()

DEPENDS(
    yql/essentials/tools/astdiff
    ydb/library/yql/tools/dqrun
    yql/essentials/udfs/test/test_import
)


DATA(
    arcadia/ydb/library/yql/tests/sql # python files
    arcadia/yql/essentials/mount
    arcadia/yql/essentials/cfg/tests
    arcadia/ydb/library/yql/tests/sql
)

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator_grpc/recipe.inc)

PEERDIR(
    library/python/testing/swag/lib
    yql/essentials/tests/common/test_framework
)

END()
