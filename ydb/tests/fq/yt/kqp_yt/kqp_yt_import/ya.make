PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_ctas.py
    test_yt_reading.py
)

PY_SRCS(
    conftest.py
    helpers.py
)

SIZE(MEDIUM)
IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
    REQUIREMENTS(cpu:1)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

DEPENDS(
    ydb/tests/tools/kqprun
)

DATA(
    arcadia/ydb/tests/fq/yt/kqp_yt/cfg
    arcadia/ydb/tests/fq/yt/kqp_yt/kqp_yt_import
)

PEERDIR(
    ydb/public/api/protos
    ydb/tests/fq/tools
    yql/essentials/tests/common/test_framework
)

END()
