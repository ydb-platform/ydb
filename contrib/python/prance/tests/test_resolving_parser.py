"""Test suite for prance.ResolvingParser ."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()

import yatest.common as yc

import pytest
from unittest.mock import patch

from prance import ResolvingParser
from prance import ValidationError

from . import none_of


def mock_get_petstore(*args, **kwargs):
    from .mock_response import MockResponse, PETSTORE_YAML

    return MockResponse(text=PETSTORE_YAML)


@pytest.fixture
def petstore_parser():
    return ResolvingParser(yc.test_source_path("specs/petstore.yaml"))


@pytest.fixture
@patch("requests.get")
def with_externals_parser(mock_get):
    mock_get.side_effect = mock_get_petstore
    return ResolvingParser(yc.test_source_path("specs/with_externals.yaml"))


@pytest.fixture
def petstore_parser_from_string():
    yaml = None
    with open(yc.test_source_path("specs/petstore.yaml"), "rb") as f:
        x = f.read()
        yaml = x.decode("utf8")
    return ResolvingParser(spec_string=yaml)


@pytest.fixture
def issue_1_parser():
    return ResolvingParser(yc.test_source_path("specs/issue_1.json"))


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Missing backends",
)
def test_basics(petstore_parser):
    assert petstore_parser.specification, "No specs loaded!"


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Missing backends",
)
def test_petstore_resolve(petstore_parser):
    assert petstore_parser.specification, "No specs loaded!"

    # The petstore references /definitions/Pet in /definitions/Pets, and uses
    # /definitions/Pets in the 200 response to the /pets path. So let's check
    # whether we can find something of /definitions/Pet there...
    res = petstore_parser.specification["paths"]["/pets"]["get"]["responses"]
    assert res["200"]["schema"]["type"] == "array", "Did not resolve right!"


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Missing backends",
)
def test_with_externals_resolve(with_externals_parser):
    assert with_externals_parser.specification, "No specs loaded!"

    # The specs are a simplified version of the petstore example, with some
    # external references.
    # - Test that the list pets call returns the right thing from the external
    #   definitions.yaml
    res = with_externals_parser.specification["paths"]["/pets"]["get"]
    res = res["responses"]
    assert res["200"]["schema"]["type"] == "array"

    # - Test that the get single pet call returns the right thing from the
    #   remote petstore definition
    res = with_externals_parser.specification["paths"]["/pets/{petId}"]["get"]
    res = res["responses"]
    assert "id" in res["200"]["schema"]["required"]

    # - Test that error responses contain a message from error.json
    res = with_externals_parser.specification["paths"]["/pets"]["get"]
    res = res["responses"]
    assert "message" in res["default"]["schema"]["required"]


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Missing backends",
)
def test_relative_urls_from_string(petstore_parser_from_string):
    # This must succeed
    assert (
        petstore_parser_from_string.yaml()
    ), "Did not get YAML representation of specs!"


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Missing backends",
)
def test_issue_1_relative_path_references(issue_1_parser):
    # Must resolve references correctly
    params = issue_1_parser.specification["paths"]["/test"]["parameters"]
    assert "id" in params[0]["schema"]["required"]


@pytest.mark.skipif(none_of("openapi-spec-validator"), reason="Missing backends")
def test_issue_39_sequence_indices():
    # Must not fail to parse
    parser = ResolvingParser(
        yc.test_source_path("specs/issue_39.yaml"), backend="openapi-spec-validator"
    )

    # The /useCase path should have two values in its response example.
    example = parser.specification["paths"]["/useCase"]["get"]["responses"]["200"][
        "content"
    ]["application/json"]["examples"]["response"]
    assert "value" in example
    assert len(example["value"]) == 2

    # However, the /test path should have only one of the strings.
    example = parser.specification["paths"]["/test"]["get"]["responses"]["200"][
        "content"
    ]["application/json"]["example"]
    assert example == "some really long or specific string"


@pytest.mark.skipif(none_of("openapi-spec-validator"), reason="Missing backends")
def test_issue_51_encoding_error():
    # Parsing used to throw - but shouldn't after heuristic change.
    parser = ResolvingParser(
        yc.test_source_path("specs/issue_51/openapi-main.yaml"),
        lazy=True,
        backend="openapi-spec-validator",
        strict=True,
    )

    parser.parse()

    # Parsing with setting an explicit and wrong file encoding should raise
    # an error, effectively reverting to the old behaviour
    parser = ResolvingParser(
        yc.test_source_path("specs/issue_51/openapi-main.yaml"),
        lazy=True,
        backend="openapi-spec-validator",
        strict=True,
        encoding="iso-8859-2",
    )

    from ruamel.yaml.reader import ReaderError

    with pytest.raises(ReaderError):
        parser.parse()


@pytest.mark.skipif(none_of("openapi-spec-validator"), reason="Missing backends")
def test_issue_65_partial_resolution_files():
    specs = """openapi: "3.0.0"
info:
  title: ''
  version: '1.0.0'
paths: {}
components:
    schemas:
        SampleArray:
            type: array
            items:
              $ref: '#/components/schemas/ItemType'

        ItemType:
          type: integer
"""

    from prance.util import resolver

    parser = ResolvingParser(spec_string=specs, backend="openapi-spec-validator", resolve_types=resolver.RESOLVE_FILES)

    from prance.util.path import path_get

    val = path_get(
        parser.specification, ("components", "schemas", "SampleArray", "items")
    )
    assert "$ref" in val


@pytest.mark.skipif(none_of("openapi-spec-validator"), reason="Missing backends")
def test_issue_83_skip_propagation():
    # Throw with strict parsing
    parser = ResolvingParser(
        yc.test_source_path("specs/issue_83/bad_spec.yml"),
        lazy=True,
        backend="openapi-spec-validator",
        strict=True,
    )

    with pytest.raises(ValidationError):
        parser.parse()

    # Do not throw with non-strict parsing
    parser = ResolvingParser(
        yc.test_source_path("specs/issue_83/bad_spec.yml"),
        lazy=True,
        backend="openapi-spec-validator",
        strict=False,
    )

    parser.parse()


@pytest.mark.skipif(none_of("openapi-spec-validator"), reason="Missing backends")
def test_value_not_converted_to_boolean():
    specs = """openapi: "3.0.0"
info:
  title: ''
  version: '1.0.0'
paths: {}
components:
    schemas:
        SampleEnum:
            type: string
            enum:
              - NO
              - OFF
"""

    from prance.util import resolver

    parser = ResolvingParser(spec_string=specs, backend="openapi-spec-validator", resolve_types=resolver.RESOLVE_FILES)
    specs = parser.specification
    assert specs["components"]["schemas"]["SampleEnum"]["enum"] == ["NO", "OFF"]
