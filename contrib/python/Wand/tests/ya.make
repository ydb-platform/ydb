PY3TEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/python/numpy
    contrib/python/Wand
)

DATA(
    arcadia/contrib/python/Wand/tests
)

TEST_SRCS(
    __init__.py
    _resource_test.py
    color_test.py
    conftest.py
    drawing_test.py
    exceptions_test.py
    image_methods_test.py
    image_properties_test.py
    image_test.py
    misc_test.py
    sequence_test.py
)

NO_LINT()

END()
