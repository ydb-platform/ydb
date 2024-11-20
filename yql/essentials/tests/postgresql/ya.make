IF (NOT OPENSOURCE)

PY3TEST()

IF(ORIGINAL)

TEST_SRCS(
#    test_postgres_original.py
)

DATA(
    arcadia/yql/essentials/tests/postgresql/original/cases
)

ELSE()

TEST_SRCS(
#    test_postgres.py
)

DATA(
    arcadia/yql/essentials/tests/postgresql/cases
)

ENDIF()

DATA(
    arcadia/yql/essentials/tests/postgresql/patches
)

SIZE(MEDIUM)
TIMEOUT(600)

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

DEPENDS(
    yql/essentials/tests/postgresql/common
    yql/essentials/tools/pgrun
    yql/essentials/tools/pg-make-test
    yql/essentials/udfs/common/set
    yql/essentials/udfs/common/re2
)

END()

ENDIF()

