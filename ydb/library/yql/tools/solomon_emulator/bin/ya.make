PY3_PROGRAM(solomon_emulator)
REQUIREMENTS(cpu:2)


STYLE_PYTHON()

PEERDIR(
    ydb/library/yql/tools/solomon_emulator/lib
)

PY_SRCS(
    MAIN main.py
)

END()
