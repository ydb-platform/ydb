PY3_PROGRAM(tstool)

PY_MAIN(tstool)

PY_SRCS(
    TOP_LEVEL
    tstool.py
)

PEERDIR(
    ydb/core/protos
)

END()
