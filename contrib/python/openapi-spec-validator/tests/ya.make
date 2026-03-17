PY3TEST()

PEERDIR(
    contrib/python/pytest
    contrib/python/mock
    contrib/python/pytest-localserver
    contrib/python/openapi-spec-validator
)

TEST_SRCS(
    conftest.py
    test_main.py
    test_shortcuts.py
    test_validate.py
    test_validators.py
)

PY_SRCS(
    NAMESPACE tests
    util.py
)

RESOURCE(
    data/v2.0/api-with-examples.yaml                        data/v2.0/api-with-examples.yaml
    data/v2.0/petstore.yaml                                 data/v2.0/petstore.yaml
    data/v2.0/petstore-expanded.yaml                        data/v2.0/petstore-expanded.yaml
    data/v3.0/api-with-examples.yaml                        data/v3.0/api-with-examples.yaml
    data/v3.0/api-with-examples-expanded.yaml               data/v3.0/api-with-examples-expanded.yaml
    data/v3.0/empty.yaml                                    data/v3.0/empty.yaml
    data/v3.0/parent-reference/common.yaml                  data/v3.0/parent-reference/common.yaml
    data/v3.0/parent-reference/openapi.yaml                 data/v3.0/parent-reference/openapi.yaml
    data/v3.0/parent-reference/spec/components.yaml         data/v3.0/parent-reference/spec/components.yaml
    data/v3.0/petstore-separate/common/schemas/Error.yaml   data/v3.0/petstore-separate/common/schemas/Error.yaml
    data/v3.0/petstore-separate/spec/openapi.yaml           data/v3.0/petstore-separate/spec/openapi.yaml
    data/v3.0/petstore-separate/spec/schemas/Pet.yaml       data/v3.0/petstore-separate/spec/schemas/Pet.yaml
    data/v3.0/petstore-separate/spec/schemas/Pets.yaml      data/v3.0/petstore-separate/spec/schemas/Pets.yaml
    data/v3.0/petstore.yaml                                 data/v3.0/petstore.yaml
)

NO_LINT()

END()
