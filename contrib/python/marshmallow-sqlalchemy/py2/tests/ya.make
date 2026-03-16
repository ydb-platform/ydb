PY2TEST()

PEERDIR(
    contrib/python/marshmallow-sqlalchemy
    contrib/python/sqlalchemy/sqlalchemy-1.3
)

TEST_SRCS(
    test_marshmallow_sqlalchemy.py
)

NO_LINT()

END()
