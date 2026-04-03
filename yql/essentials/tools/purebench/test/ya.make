PY3TEST()

SIZE(MEDIUM)
TIMEOUT(180)

TEST_SRCS(
    test.py
)

DEPENDS(
    yql/essentials/tools/purebench
)

END()
