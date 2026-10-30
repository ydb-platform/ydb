PY3TEST()

SIZE(MEDIUM)
TIMEOUT(600)

TEST_SRCS(
    test.py
    test_options.py
)

DEPENDS(
    yql/essentials/tools/purebench/cli
)

END()
