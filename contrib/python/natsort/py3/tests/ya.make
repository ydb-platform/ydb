PY3TEST()

PEERDIR(
    contrib/python/natsort
    contrib/python/hypothesis
    contrib/python/pytest-mock
)

TEST_SRCS(
    conftest.py
    test_fake_fastnumbers.py
    test_final_data_transform_factory.py
    test_input_string_transform_factory.py
    test_main.py
    test_natsort_key.py
    test_natsort_keygen.py
    test_natsorted.py
    test_natsorted_convenience.py
    test_ns_enum.py
    test_os_sorted.py
    test_parse_bytes_function.py
    test_parse_number_function.py
    test_parse_string_function.py
    test_regex.py
    test_string_component_transform_factory.py
    test_unicode_numbers.py
    test_utils.py
)

NO_LINT()

END()
