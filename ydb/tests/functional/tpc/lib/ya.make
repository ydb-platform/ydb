PY3_LIBRARY()

    PY_SRCS (
        conftest.py
    )

    PEERDIR(
        ydb/tests/library
        ydb/tests/olap/load/lib
    )

END()
