PY3TEST()

PEERDIR(
    contrib/python/fastjsonschema
    contrib/python/pytest-benchmark
)

NO_LINT()

TEST_SRCS(
    benchmarks/test_benchmark.py
    conftest.py
    json_schema/__init__.py
    json_schema/test_draft04.py
    json_schema/test_draft06.py
    json_schema/test_draft07.py
    json_schema/utils.py
    test_array.py
    test_boolean.py
    test_boolean_schema.py
    test_common.py
    test_compile_to_code.py
    test_composition.py
    test_const.py
    test_default.py
    test_examples.py
    test_exceptions.py
    test_format.py
    test_integration.py
    test_null.py
    test_number.py
    test_object.py
    test_pattern_properties.py
    test_pattern_serialization.py
    test_security.py
    test_string.py
)

END()
