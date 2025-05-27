PY3TEST()

TEST_SRCS(
    test.py
)

DEPENDS(
    yql/essentials/tools/types_dump
)

DATA(
    arcadia/yql/essentials/data/language
)

END()
