PY3TEST()

PEERDIR(
    contrib/python/marshmallow-sqlalchemy
    contrib/python/sqlalchemy/sqlalchemy-1.4
    contrib/python/pytest-lazy-fixtures
)

TEST_SRCS(
    conftest.py
    test_conversion.py
    test_model_schema.py
    test_sqlalchemy_schema.py
    test_table_schema.py
)

NO_LINT()

END()
