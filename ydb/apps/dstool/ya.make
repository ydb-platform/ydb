PY3_PROGRAM(ydb-dstool)

STRIP()

PY_MAIN(ydb.apps.dstool.main)

PY_SRCS(
    main.py
)

PEERDIR(
    ydb/apps/dstool/lib
)

END()
