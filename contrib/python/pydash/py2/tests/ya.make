PY2TEST()

NO_LINT()

TEST_SRCS(
    __init__.py
    fixtures.py
    test_annotations.py
    test_arrays.py
    test_chaining.py
    test_collections.py
    test_functions.py
    test_numerical.py
    test_objects.py
    test_predicates.py
    test_strings.py
    test_utilities.py
)

PEERDIR(
    contrib/python/mock
    contrib/python/pydash
)

END()
