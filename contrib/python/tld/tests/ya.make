PY3TEST()

PEERDIR(
    contrib/python/tld
    contrib/python/Faker
)

NO_LINT()

SRCDIR(contrib/python/tld/tld)

TEST_SRCS(
    tests/__init__.py
    tests/base.py
    tests/test_commands.py
    tests/test_core.py
    tests/test_registry.py
)

DATA(
    arcadia/contrib/python/tld/tld/tests/res
)

END()
