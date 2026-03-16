PY3TEST()
NO_LINT()

PEERDIR(
    contrib/python/dirty-equals
    contrib/python/tzdata
    contrib/python/pydantic/pydantic-2
)

TEST_SRCS(
    mypy_checks.py
    test_base.py
    test_boolean.py
    # test_datetime.py
    test_dict.py
    # test_docs.py  # needs pytest_examples
    # test_inspection.py
    test_list_tuple.py
    test_numeric.py
    test_other.py
    test_strings.py
)

END()
