PY2TEST()

PEERDIR(
    contrib/deprecated/python/ipaddress
    contrib/python/WTForms-JSON
)

NO_LINT()

TEST_SRCS(
    __init__.py
    test_boolean_field.py
    test_errors.py
    test_field_type_coercion.py
    test_json_decoder.py
    test_object_defaults.py
    test_patch_data.py
    test_select_multiple_field.py
)

END()
