PY3TEST()

TEST_SRCS(
    test_cte.py
)

SIZE(MEDIUM)

DEPENDS(
    ydb/tests/tools/kqprun
)

DATA(
    arcadia/ydb/tests/tools/kqprun/configuration/app_config.conf
)

END()
