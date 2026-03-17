PY3TEST()

PEERDIR(
    contrib/python/Flask-SQLAlchemy
    contrib/python/sqlalchemy/sqlalchemy-1.4
    contrib/python/mock
)

NO_LINT()

TEST_SRCS(
    conftest.py
    test_cli.py
    test_engine.py
    test_legacy_query.py
    test_metadata.py
    test_model_bind.py
    test_model_name.py
    test_model.py
    test_pagination.py
    test_record_queries.py
    test_session.py
    test_table_bind.py
    test_track_modifications.py
    test_view_query.py
)

END()
