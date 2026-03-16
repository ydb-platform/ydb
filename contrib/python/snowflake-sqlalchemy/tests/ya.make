PY3TEST()

PEERDIR(
    contrib/python/snowflake-sqlalchemy
)

TEST_SRCS(
    __init__.py
    conftest.py
    util.py

    test_compiler.py
    test_copy.py
    test_create.py
    test_custom_types.py
    test_unit_core.py
    test_unit_cte.py
    test_unit_types.py
    test_unit_url.py
)

NO_LINT()

END()
