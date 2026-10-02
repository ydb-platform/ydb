PY3TEST()

SIZE(MEDIUM)
TIMEOUT(240)

TEST_SRCS(
    test.py
)

DEPENDS(
    yql/essentials/tools/purebench
)

END()
