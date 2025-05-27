PY3TEST()

TEST_SRCS(
    test.py
)

DEPENDS(
    yql/essentials/tools/sql_functions_dump
)

DATA(
    arcadia/yql/essentials/data/language
)

END()
