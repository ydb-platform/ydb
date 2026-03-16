PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/dirty-equals
    contrib/python/hypothesis
    contrib/python/pydantic-core
    contrib/python/pytz
    contrib/python/typing-extensions
    contrib/python/typing-inspection
)

TEST_SRCS(
    conftest.py
    test_build.py
    test_config.py
    test_docstrings.py
    # test_errors.py
    test_garbage_collection.py
    test_hypothesis.py
    test_isinstance.py
    test_json.py
    test_misc.py
    test_schema_functions.py
    test_strict.py
    test_typing.py
    test_tzinfo.py
    test_validate_strings.py
    test_validation_context.py
)

END()
