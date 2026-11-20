PY3TEST()

STYLE_PYTHON()
TEST_SRCS(test_replication.py)

PEERDIR(
    library/python/port_manager
    library/python/testing/yatest_common
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)

DEPENDS(ydb/tests/tools/kqprun)

SIZE(MEDIUM)
REQUIREMENTS(ram:8 cpu:4)
TIMEOUT(300)

END()
