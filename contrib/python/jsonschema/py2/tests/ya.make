PY2TEST()

PEERDIR(
    contrib/python/jsonschema
    contrib/python/Twisted
)

IF (PYTHON2)
    PEERDIR(
        contrib/python/mock
    )
ENDIF()

SRCDIR(contrib/python/jsonschema/py2/jsonschema/tests)

PY_SRCS(
    NAMESPACE jsonschema.tests
    _helpers.py
)

TEST_SRCS(
    __init__.py
    test_cli.py
    test_exceptions.py
    test_format.py
    test_types.py
    test_validators.py
)

NO_LINT()

END()
