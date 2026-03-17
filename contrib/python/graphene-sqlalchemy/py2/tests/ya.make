PY2TEST()

PEERDIR(
    contrib/deprecated/python/enum34
    contrib/python/graphene
    contrib/python/pytz
    contrib/python/sqlalchemy/sqlalchemy-1.3
    contrib/python/SQLAlchemy-Utils
    contrib/python/graphene-sqlalchemy
)

SRCDIR(contrib/python/graphene-sqlalchemy/py2/graphene_sqlalchemy/tests)

TEST_SRCS(
    conftest.py
    models.py
    test_converter.py
    test_mutations.py
    test_query.py
    test_reflected.py
    test_registry.py
    test_schema.py
    test_types.py
    test_utils.py
)

NO_LINT()

END()
