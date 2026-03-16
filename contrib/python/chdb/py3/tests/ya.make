PY3TEST()

LICENSE(
    Apache-2.0 AND
    LicenseRef-scancode-unknown-license-reference AND
    MIT
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/python/chdb
    contrib/python/pandas
    contrib/python/psutil
)

NO_LINT()

SIZE(MEDIUM)

TEST_SRCS(
    # test_basic.py
    test_complex_pyobj.py
    # test_conn_cursor.py
    # test_dbapi_persistence.py
    # test_dbapi.py
    test_early_gc.py
    test_final_join.py
    test_gc.py
    test_insert_vector.py
    test_issue104.py
    test_issue135.py
    test_issue229.py
    # test_issue251.py
    # test_issue31.py
    # test_issue60.py
    test_joindf.py
    test_materialize.py
    # test_on_df.py
    test_parallel.py
    test_query_json.py
    test_query_py.py
    test_signal_handler.py
    # test_state2_dataframe.py
    # test_stateful.py
    # test_statistics.py
    test_streaming_query.py
    # test_udf.py
    test_usedb.py
)

PY_SRCS(
    format_output.py
    timeout_decorator.py
    utils.py
)

REQUIREMENTS(disk_usage:10)

RESOURCE_FILES(
    PREFIX arcadia/contrib/python/chdb/tests/
    data/sample_2021-04-01_performance_mobile_tiles.parquet
    data/alltypes_dictionary.parquet
    queries.sql
)

END()
