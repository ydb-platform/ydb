PY3TEST()

PEERDIR(
    contrib/python/python-rapidjson
    contrib/python/pytz
)

TEST_SRCS(
    conftest.py
    test_base_types.py
    test_circular.py
    test_dict_subclass.py
    test_enum.py
    test_float.py
    test_memory_leaks.py
    test_params.py
    test_pass1.py
    test_pass2.py
    test_pass3.py
    test_rawjson.py
    test_refs_count.py
    test_streams.py
    test_unicode.py
    test_validator.py
)

NO_LINT()

END()
