PY3TEST()

PEERDIR(
    contrib/python/marshmallow
    contrib/python/simplejson
    contrib/python/tzdata
)

TEST_SRCS(
    conftest.py
    test_decorators.py
    test_deserialization.py
    test_error_store.py
    test_exceptions.py
    test_fields.py
    test_options.py
    test_registry.py
    test_schema.py
    test_serialization.py
    test_utils.py
    test_validate.py
)

PY_SRCS(
    NAMESPACE tests
    base.py
    foo_serializer.py
)

NO_LINT()

END()
