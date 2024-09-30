PY3TEST()

IF(ORIGINAL)

TEST_SRCS(
#    test_postgres_original.py
)

DATA(
    arcadia/ydb/library/yql/tests/postgresql/original/cases
)

ELSE()

TEST_SRCS(
#    test_postgres.py
)

DATA(
    arcadia/ydb/library/yql/tests/postgresql/cases
)

ENDIF()

DATA(
    arcadia/ydb/library/yql/tests/postgresql/patches
)

SIZE(MEDIUM)
TIMEOUT(600)

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

DEPENDS(
    ydb/library/yql/tests/postgresql/common
    ydb/library/yql/tools/pgrun
    ydb/library/yql/tools/pg-make-test
    ydb/library/yql/udfs/common/set
    ydb/library/yql/udfs/common/re2
)

END()
