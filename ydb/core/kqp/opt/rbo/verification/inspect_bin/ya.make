PY3_PROGRAM(kqp_rbo_inspect)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification/inspector
)

END()
