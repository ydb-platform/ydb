PY3TEST()

PEERDIR(
    contrib/python/assertpy
)

NO_LINT()

TEST_SRCS(
    test_bool.py
    test_class.py
    test_collection.py
    test_core.py
    test_custom_dict.py
    test_custom_list.py
    test_datetime.py
    test_description.py
    test_dict_compare.py
    test_dict.py
    test_dyn.py
    test_equals.py
    test_expected_exception.py
    test_extensions.py
    test_extracting.py
    test_fail.py
    test_file.py
    test_in.py
    test_list.py
    test_none.py
    test_numbers.py
    test_readme.py
    test_same_as.py
    test_snapshots.py
    test_soft_fail.py
    test_soft.py
    test_string.py
    test_traceback.py
    test_type.py
    test_warn.py
)


END()