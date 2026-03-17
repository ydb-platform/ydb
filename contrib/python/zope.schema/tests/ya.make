PY3TEST()

PEERDIR(
    contrib/python/zope.schema
)

SRCDIR(
    contrib/python/zope.schema/zope/schema
)

TEST_SRCS(
    # tests/__init__.py
    tests/states.py
    tests/test__bootstrapfields.py
    tests/test__bootstrapinterfaces.py
    tests/test__field.py
    #tests/test__messageid.py
    tests/test_accessors.py
    tests/test_equality.py
    tests/test_fieldproperty.py
    tests/test_interfaces.py
    tests/test_schema.py
    tests/test_states.py
    tests/test_vocabulary.py
)

NO_LINT()

END()
