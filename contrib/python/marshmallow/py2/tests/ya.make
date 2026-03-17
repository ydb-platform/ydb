PY2TEST()

PEERDIR(
    contrib/python/simplejson
    contrib/python/python-dateutil
    contrib/python/pytz
    contrib/python/marshmallow
)

TEST_SRCS(
    __init__.py
    base.py
    conftest.py
    foo_serializer.py
    test_decorators.py
    test_deserialization.py
    test_exceptions.py
    test_fields.py
    test_marshalling.py
    test_options.py
    test_registry.py
    test_schema.py
    test_serialization.py
    test_utils.py
    test_validate.py
)

NO_LINT()

END()
