PY3TEST()

TEST_SRCS(
    test.py
)

DEPENDS(
    yql/essentials/tools/langver_dump
)

DATA(
    arcadia/yql/essentials/data/language
)

END()
