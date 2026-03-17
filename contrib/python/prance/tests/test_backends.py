"""Test suite focusing on validation backend features."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2018 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()

import yatest.common as yc

import pytest

from prance import BaseParser
from prance import ValidationError
from prance.util import validation_backends

from . import none_of


def test_bad_backend():
    with pytest.raises(ValueError):
        BaseParser(yc.test_source_path("specs/petstore.yaml"), backend="does_not_exist")


@pytest.mark.skipif(none_of("flex"), reason="Missing dependencies: flex")
def test_flex_issue_5_integer_keys():
    # Must succeed with default (flex) parser; note the parser does not stringify the response code
    parser = BaseParser(yc.test_source_path("specs/issue_5.yaml"), backend="flex")
    assert 200 in parser.specification["paths"]["/test"]["post"]["responses"]


@pytest.mark.skipif(none_of("flex"), reason="Missing dependencies: flex")
def test_flex_validate_success():
    parser = BaseParser(yc.test_source_path("specs/petstore.yaml"), backend="flex")


@pytest.mark.skipif(none_of("flex"), reason="Missing dependencies: flex")
def test_flex_validate_failure():
    with pytest.raises(ValidationError):
        parser = BaseParser(yc.test_source_path("specs/missing_reference.yaml"), backend="flex")


@pytest.mark.skipif(
    none_of("swagger-spec-validator"),
    reason="Missing dependencies: swagger-spec-validator",
)
def test_swagger_spec_validator_issue_5_integer_keys():
    # Must fail in implicit strict mode.
    with pytest.raises(ValidationError):
        BaseParser(yc.test_source_path("specs/issue_5.yaml"), backend="swagger-spec-validator")

    # Must fail in explicit strict mode.
    with pytest.raises(ValidationError):
        BaseParser(
            yc.test_source_path("specs/issue_5.yaml"), backend="swagger-spec-validator", strict=True
        )

    # Must succeed in non-strict/lenient mode
    parser = BaseParser(
        yc.test_source_path("specs/issue_5.yaml"), backend="swagger-spec-validator", strict=False
    )
    assert "200" in parser.specification["paths"]["/test"]["post"]["responses"]


@pytest.mark.skipif(
    none_of("swagger-spec-validator"),
    reason="Missing dependencies: swagger-spec-validator",
)
def test_swagger_spec_validator_validate_success():
    parser = BaseParser(yc.test_source_path("specs/petstore.yaml"), backend="swagger-spec-validator")


@pytest.mark.skipif(
    none_of("swagger-spec-validator"),
    reason="Missing dependencies: swagger-spec-validator",
)
def test_swagger_spec_validator_validate_failure():
    with pytest.raises(ValidationError):
        parser = BaseParser(
            yc.test_source_path("specs/missing_reference.yaml"), backend="swagger-spec-validator"
        )


@pytest.mark.skipif(
    none_of("openapi-spec-validator"),
    reason="Missing dependencies: openapi-spec-validator",
)
def test_openapi_spec_validator_issue_5_integer_keys():
    # Must fail in implicit strict mode.
    with pytest.raises(ValidationError):
        BaseParser(yc.test_source_path("specs/issue_5.yaml"), backend="openapi-spec-validator")

    # Must fail in explicit strict mode.
    with pytest.raises(ValidationError):
        BaseParser(
            yc.test_source_path("specs/issue_5.yaml"), backend="openapi-spec-validator", strict=True
        )

    # Must succeed in non-strict/lenient mode
    parser = BaseParser(
        yc.test_source_path("specs/issue_5.yaml"), backend="openapi-spec-validator", strict=False
    )
    assert "200" in parser.specification["paths"]["/test"]["post"]["responses"]


@pytest.mark.skipif(
    none_of("openapi-spec-validator"),
    reason="Missing dependencies: openapi-spec-validator",
)
def test_openapi_spec_validator_issue_36_error_reporting():
    with pytest.raises(ValidationError, match=r"Strict mode enabled"):
        BaseParser(yc.test_source_path("specs/issue_36.yaml"), backend = "openapi-spec-validator")


@pytest.mark.skipif(
    none_of("openapi-spec-validator"),
    reason="Missing dependencies: openapi-spec-validator",
)
def test_openapi_spec_validator_validate_success():
    parser = BaseParser(yc.test_source_path("specs/petstore.yaml"), backend="openapi-spec-validator")


@pytest.mark.skipif(
    none_of("openapi-spec-validator"),
    reason="Missing dependencies: openapi-spec-validator",
)
def test_openapi_spec_validator_validate_failure():
    with pytest.raises(ValidationError):
        parser = BaseParser(
            yc.test_source_path("specs/missing_reference.yaml"), backend="openapi-spec-validator"
        )


@pytest.mark.skipif(
    none_of("openapi-spec-validator"),
    reason="Missing dependencies: openapi-spec-validator",
)
def test_openapi_spec_validator_issue_20_spec_version_handling():
    # The spec is OpenAPI 3, but broken. Need to set 'strict' to False to stringify keys
    with pytest.raises(ValidationError):
        parser = BaseParser(
            yc.test_source_path("specs/issue_20.yaml"), backend="openapi-spec-validator", strict=False
        )

    # Lazy parsing should let us validate what's happening
    parser = BaseParser(
        yc.test_source_path("specs/issue_20.yaml"),
        backend="openapi-spec-validator",
        strict=False,
        lazy=True,
    )
    assert not parser.valid
    assert parser.version_parsed == ()

    with pytest.raises(ValidationError):
        parser.parse()

    # After parsing, the specs are not valid, but the correct version is
    # detected.
    assert not parser.valid
    assert parser.version_parsed == (3, 0, 0)
