PY3TEST()

TEST_SRCS(
    test_cli.py
    test_plan.py
    test_trace.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
    ydb/core/kqp/opt/rbo/verification/inspector
)

DEPENDS(
    contrib/tools/z3
)

END()
