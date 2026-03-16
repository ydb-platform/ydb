PY3TEST()

NO_LINT()

FORK_TESTS()

PEERDIR(
    contrib/python/datamodel-code-generator
    contrib/python/freezegun
    contrib/python/httpx
    contrib/python/pytest
    contrib/python/pytest-mock
    contrib/python/prance
    contrib/python/openapi-spec-validator
    contrib/python/graphql-core
)

TEST_SRCS(
    __init__.py
    conftest.py
    main/graphql/__init__.py
    main/graphql/test_main_graphql.py
    main/__init__.py
    main/jsonschema/__init__.py
    main/jsonschema/test_main_jsonschema.py
    main/openapi/__init__.py
    main/openapi/test_main_openapi.py
    main/test_main_csv.py
    main/test_main_general.py
    main/test_main_json.py
    main/test_main_yaml.py
    model/__init__.py
    model/pydantic/__init__.py
    model/pydantic/test_base_model.py
    model/pydantic/test_constraint.py
    model/pydantic/test_custom_root_type.py
    model/pydantic/test_data_class.py
    model/pydantic/test_types.py
    model/pydantic_v2/__init__.py
    model/pydantic_v2/test_root_model.py
    model/test_base.py
    parser/__init__.py
    parser/test_base.py
    parser/test_graphql.py
    parser/test_jsonschema.py
    parser/test_openapi.py
    test_format.py
    test_imports.py
    test_infer_input_type.py
    test_main_kr.py
    test_reference.py
    test_resolver.py
    test_types.py
)

END()
