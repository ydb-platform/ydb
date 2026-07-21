PY3TEST()

TEST_SRCS(
    test.py
)

DEPENDS(
    ydb/core/base/ut_backtrace/helper
)

END()

RECURSE(
    helper
)
