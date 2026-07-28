PY3_PROGRAM()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
    PEERDIR(
        ydb/tests/library
        ydb/tests/stress/common
    )

    PY_SRCS(
        __main__.py
    )
END()
