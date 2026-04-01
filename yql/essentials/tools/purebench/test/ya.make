PY3TEST()

SIZE(MEDIUM)
TIMEOUT(120)

TEST_SRCS(
    test.py
)

DEPENDS(
    yql/essentials/tools/purebench
)

END()
