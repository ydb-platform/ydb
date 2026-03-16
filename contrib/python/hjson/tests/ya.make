PY3TEST()

PEERDIR(
    contrib/python/hjson
)

SRCDIR(
    contrib/python/hjson
)

TEST_SRCS(
    hjson/tests/__init__.py
    hjson/tests/test_bigint_as_string.py
    hjson/tests/test_bitsize_int_as_string.py
    hjson/tests/test_check_circular.py
    hjson/tests/test_decimal.py
    hjson/tests/test_decode.py
    hjson/tests/test_default.py
    hjson/tests/test_dump.py
    hjson/tests/test_encode_basestring_ascii.py
    hjson/tests/test_errors.py
    hjson/tests/test_fail.py
    hjson/tests/test_float.py
    hjson/tests/test_for_json.py
    # hjson/tests/test_hjson.py
    hjson/tests/test_indent.py
    hjson/tests/test_item_sort_key.py
    hjson/tests/test_namedtuple.py
    hjson/tests/test_pass1.py
    hjson/tests/test_pass2.py
    hjson/tests/test_pass3.py
    hjson/tests/test_recursion.py
    hjson/tests/test_scanstring.py
    hjson/tests/test_separators.py
    # hjson/tests/test_tool.py
    hjson/tests/test_tuple.py
    hjson/tests/test_unicode.py
)

NO_LINT()

END()
