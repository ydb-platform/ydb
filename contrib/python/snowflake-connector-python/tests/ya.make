PY3TEST()

PEERDIR(
    contrib/python/snowflake-connector-python
)

SRCDIR(contrib/python/snowflake-connector-python)

TEST_SRCS(
    test/__init__.py
    test/generate_test_files.py
    test/helpers.py
    test/lazy_var.py
    test/conftest.py

    # from contrib root dir
    # find test/unit -type f  | LC_ALL=C sort | sed -r 's/^/    /'
    test/unit/__init__.py
    test/unit/conftest.py
    test/unit/test_auth.py
    test/unit/test_auth_oauth.py
    test/unit/test_auth_okta.py
    test/unit/test_auth_webbrowser.py
    test/unit/test_binaryformat.py
    test/unit/test_bind_upload_agent.py
    test/unit/test_cache.py
    test/unit/test_connection.py
    test/unit/test_construct_hostname.py
    test/unit/test_converter.py
    test/unit/test_cursor.py
    test/unit/test_datetime.py
    test/unit/test_dbapi.py
    test/unit/test_encryption_util.py
    test/unit/test_errors.py
    test/unit/test_gcs_client.py
    test/unit/test_log_secret_detector.py
    test/unit/test_mfa_no_cache.py
    test/unit/test_oob_secret_detector.py
    test/unit/test_parse_account.py
    test/unit/test_proxies.py
    test/unit/test_put_get.py
    test/unit/test_renew_session.py
    test/unit/test_result_batch.py
    test/unit/test_retry_network.py
    test/unit/test_session_manager.py
    test/unit/test_split_statement.py
    test/unit/test_storage_client.py
    test/unit/test_telemetry.py
    test/unit/test_telemetry_oob.py
    # test/unit/test_linux_local_file_cache.py
    # test/unit/test_s3_util.py
    # test/unit/test_ocsp.py
)

NO_LINT()

END()

