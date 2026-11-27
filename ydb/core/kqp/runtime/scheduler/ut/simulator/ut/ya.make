PY3TEST()

SIZE(MEDIUM)

TEST_SRCS(
    test_simulator.py
)

DEPENDS(
    ydb/core/kqp/runtime/scheduler/ut/simulator
    ydb/core/kqp/runtime/scheduler/ut/simulator/api
)

END()
