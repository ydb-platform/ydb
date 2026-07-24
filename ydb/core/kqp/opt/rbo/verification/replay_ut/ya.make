PY3TEST()

TEST_SRCS(
    test_model.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
    ydb/core/kqp/opt/rbo/verification/replay
)

DEPENDS(
    contrib/tools/z3
)

END()
