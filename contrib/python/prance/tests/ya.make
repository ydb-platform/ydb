PY3TEST()

PEERDIR(
    contrib/python/prance
    contrib/python/pytest-localserver
    contrib/python/click
    contrib/python/openapi-spec-validator
    contrib/python/swagger-spec-validator
    contrib/python/flex
)

DATA(
    arcadia/contrib/python/prance/tests/OpenAPI-Specification
    arcadia/contrib/python/prance/tests/specs
)

TEST_SRCS(
    __init__.py
    mock_response.py
    sandbox.py
    test_backends.py
    test_base_parser.py
    test_cli.py
    test_convert.py
    test_resolving_parser.py
    #test_translating_parser.py
    test_util.py
    test_util_exceptions.py
    test_util_formats.py
    test_util_fs.py
    test_util_iterators.py
    test_util_path.py
    test_util_resolver.py
    test_util_url.py
    test_zzz_specs.py
)

PY_SRCS(
    TOP_LEVEL
    specs/__init__.py
)

RESOURCE_FILES(
    PREFIX contrib/python/prance/tests/
    specs/petstore.yaml
)

NO_LINT()

END()
