PY3_PROGRAM(include-sanitizer)

PY_SRCS(
    __init__.py
    main.py
)

PEERDIR(
    ydb/tools/include_sanitizer
)

PY_MAIN(ydb.tools.include_sanitizer.bin.main)

END()
