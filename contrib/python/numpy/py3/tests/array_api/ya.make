PY3TEST()

PEERDIR(
    contrib/python/numpy/py3/tests
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/array_api/tests/__init__.py
    numpy/array_api/tests/test_array_object.py
    numpy/array_api/tests/test_creation_functions.py
    numpy/array_api/tests/test_data_type_functions.py
    numpy/array_api/tests/test_elementwise_functions.py
    numpy/array_api/tests/test_indexing_functions.py
    numpy/array_api/tests/test_manipulation_functions.py
    numpy/array_api/tests/test_set_functions.py
    numpy/array_api/tests/test_sorting_functions.py
    numpy/array_api/tests/test_validation.py
)

END()
