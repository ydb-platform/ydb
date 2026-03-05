PY3TEST()

TEST_SRCS(
    test_cte.py
)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

DEPENDS(
    ydb/tests/tools/kqprun
)

DATA(
    arcadia/ydb/tests/tools/kqprun/configuration/app_config.conf
)

END()
