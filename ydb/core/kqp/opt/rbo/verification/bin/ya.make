PY3_PROGRAM(kqp_rbo_verify)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
)

END()
