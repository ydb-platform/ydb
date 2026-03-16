"""Test suite for prance.BaseParser ."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()

import pytest

from prance import BaseParser
from prance import ValidationError
from prance.util.fs import FileNotFoundError

from . import none_of

import yatest.common as yc


@pytest.fixture
def petstore_parser():
    return BaseParser(yc.test_source_path("specs/petstore.yaml"))


@pytest.fixture
def petstore_parser_from_string():
    yaml = None
    with open(yc.test_source_path("specs/petstore.yaml"), "rb") as f:
        x = f.read()
        yaml = x.decode("utf8")
    return BaseParser(spec_string=yaml)


def test_load_fail():
    from prance.util.url import ResolutionError

    with pytest.raises(ResolutionError):
        BaseParser(yc.test_source_path("specs/missing.yaml"))


def test_parse_fail():
    with pytest.raises(ValidationError):
        BaseParser(
            spec_string="""---
invalid 'a'sda YAML"""
        )


def test_version_fail():
    with pytest.raises(ValidationError):
        BaseParser(
            spec_string="""---
openapi: 4.0.0"""
        )


def test_filename_or_spec():
    with pytest.raises(AssertionError):
        BaseParser("", "")


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Backends missing.",
)
def test_load_and_parse_valid(petstore_parser):
    assert petstore_parser.specification, "No specs loaded!"


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Backends missing.",
)
def test_load_and_parse_lazy():
    parser = BaseParser(yc.test_source_path("specs/petstore.yaml"), lazy=True)
    assert parser.specification is None, "Should not have specs yet!"

    parser.parse()
    assert parser.specification, "No specs loaded!"


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Backends missing.",
)
def test_yaml_valid(petstore_parser):
    assert petstore_parser.yaml(), "Did not get YAML representation of specs!"


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Backends missing.",
)
def test_json_valid(petstore_parser):
    assert petstore_parser.json(), "Did not get JSON representation of specs!"


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Backends missing.",
)
def test_cache_specs_mixin(petstore_parser):
    # In order to test the caching, we need to first use either the YAML or the
    # JSON mixin. Let's use YAML, because it's more swagger-ish
    yaml = petstore_parser.yaml()
    assert yaml, "Did not get YAML representation of specs!"

    # Caching should mean that if the specifications do not change, then neither
    # does the YAML representation.
    assert yaml == petstore_parser.yaml(), "YAML representation changed!"

    # In fact, the objects shouldn't even change.
    assert id(yaml) == id(petstore_parser.yaml()), (
        "YAML did not change but " "got regenerated!"
    )

    # However, when the specs change, then so must the YAML representation.
    petstore_parser.specification["foo"] = "bar"
    assert yaml != petstore_parser.yaml(), "YAML representation did not change!"


@pytest.mark.skipif(
    none_of("openapi-spec-validator", "swagger-spec-validator", "flex"),
    reason="Backends missing.",
)
def test_relative_urls_from_string(petstore_parser_from_string):
    # This must succeed
    assert (
        petstore_parser_from_string.yaml()
    ), "Did not get YAML representation of specs!"
