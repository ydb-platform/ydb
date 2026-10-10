PY3_PROGRAM(solomon_emulator)

STYLE_PYTHON()

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/library/yql/tools/solomon_emulator/lib
)

PY_SRCS(
    MAIN main.py
)

END()
