PY3_PROGRAM(tstool)

PY_MAIN(tstool)

PY_SRCS(
    TOP_LEVEL
    tstool.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/core/protos
)

END()
