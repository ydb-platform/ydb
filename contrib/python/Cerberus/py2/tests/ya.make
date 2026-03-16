PY2TEST()

PEERDIR(
    contrib/python/Cerberus
)

SRCDIR(
    contrib/python/Cerberus/py2/cerberus/tests
)

PY_SRCS(
    NAMESPACE cerberus.tests
    __init__.py
    conftest.py
)

TEST_SRCS(
    test_assorted.py
    test_customization.py
    test_errors.py
    test_legacy.py
    test_normalization.py
    test_registries.py
    test_schema.py
    test_utils.py
    test_validation.py
)

NO_LINT()

END()
