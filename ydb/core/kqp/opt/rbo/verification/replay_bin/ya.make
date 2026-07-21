PY3_PROGRAM(kqp_rbo_replay)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification/replay
)

END()
