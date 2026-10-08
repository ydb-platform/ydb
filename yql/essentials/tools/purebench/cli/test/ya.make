PY3TEST()

SIZE(MEDIUM)
TIMEOUT(240)

TEST_SRCS(
    test.py
    test_options.py
)

DEPENDS(
    yql/essentials/tools/purebench/cli
)

END()
