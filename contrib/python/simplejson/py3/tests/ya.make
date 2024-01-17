PY3TEST()

PEERDIR(
    contrib/python/simplejson
)

SRCDIR(
    contrib/python/simplejson/py3/simplejson/tests
)

TEST_SRCS(
    __init__.py
    test_bigint_as_string.py
    test_bitsize_int_as_string.py
    test_check_circular.py
    test_decimal.py
    test_decode.py
    test_default.py
    test_dump.py
    test_encode_basestring_ascii.py
    test_encode_for_html.py
    test_errors.py
    test_fail.py
    test_float.py
    test_for_json.py
    test_indent.py
    test_item_sort_key.py
    test_iterable.py
    test_namedtuple.py
    test_pass1.py
    test_pass2.py
    test_pass3.py
    test_raw_json.py
    test_recursion.py
    test_scanstring.py
    test_separators.py
    test_speedups.py
    test_str_subclass.py
    test_subclass.py
    # test_tool.py
    test_tuple.py
    test_unicode.py
)

NO_LINT()

END()
