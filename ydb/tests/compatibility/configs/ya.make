PY3TEST()

SIZE(LARGE)
TAG(ya:fat)

DEPENDS(ydb/tests/library/compatibility/configs)

TEST_SRCS(
    test_defaults.py
)

END()
