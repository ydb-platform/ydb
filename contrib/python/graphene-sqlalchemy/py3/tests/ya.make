PY3TEST()

PEERDIR(
    contrib/python/graphene
    contrib/python/pytz
    contrib/python/sqlalchemy/sqlalchemy-1.4
    contrib/python/SQLAlchemy-Utils
    contrib/python/graphene-sqlalchemy
)

SRCDIR(contrib/python/graphene-sqlalchemy/py3/graphene_sqlalchemy/tests)

TEST_SRCS(
    conftest.py
    models.py
    test_batching.py
    test_converter.py
    test_enums.py
    test_fields.py
    test_query_enums.py
    test_query.py
    test_reflected.py
    test_registry.py
    test_sort_enums.py
    test_types.py
    test_utils.py
    utils.py
)

NO_LINT()

END()
