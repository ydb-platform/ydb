PY2TEST()

PEERDIR(
    contrib/python/Flask-SQLAlchemy
    contrib/python/sqlalchemy/sqlalchemy-1.4
    contrib/python/mock
)

NO_LINT()

TEST_SRCS(
    conftest.py
    test_basic_app.py
    test_binds.py
    test_commit_on_teardown.py
    test_config.py
    test_meta_data.py
    test_model_class.py
    test_pagination.py
    test_query_class.py
    test_query_property.py
    test_regressions.py
    test_sessions.py
    test_signals.py
    test_sqlalchemy_includes.py
    test_table_name.py
    test_utils.py
)

END()
