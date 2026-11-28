PY3_PROGRAM(ydb-fill-tables)

    PY_MAIN(ydb.tests.olap.lib.fill_tables:main)

    PEERDIR(
        ydb/tests/olap/lib
    )

END()
