import pytest

from apispec import utils
from apispec.exceptions import APISpecError


class TestOpenAPIVersion:
    @pytest.mark.parametrize("version", ("1.0", "4.0"))
    def test_openapi_version_invalid_version(self, version):
        message = "Not a valid OpenAPI version number:"
        with pytest.raises(APISpecError, match=message):
            utils.OpenAPIVersion(version)

    @pytest.mark.parametrize("version", ("3.0.1", utils.OpenAPIVersion("3.0.1")))
    def test_openapi_version_string_or_openapi_version_param(self, version):
        assert utils.OpenAPIVersion(version) == utils.OpenAPIVersion("3.0.1")

    def test_openapi_version_digits(self):
        ver = utils.OpenAPIVersion("3.0.1")
        assert ver.major == 3
        assert ver.minor == 0
        assert ver.patch == 1
        assert ver.vstring == "3.0.1"
        assert str(ver) == "3.0.1"


def test_build_reference():
    assert utils.build_reference("schema", 2, "Test") == {"$ref": "#/definitions/Test"}
    assert utils.build_reference("parameter", 2, "Test") == {
        "$ref": "#/parameters/Test"
    }
    assert utils.build_reference("response", 2, "Test") == {"$ref": "#/responses/Test"}
    assert utils.build_reference("security_scheme", 2, "Test") == {
        "$ref": "#/securityDefinitions/Test"
    }
    assert utils.build_reference("schema", 3, "Test") == {
        "$ref": "#/components/schemas/Test"
    }
    assert utils.build_reference("parameter", 3, "Test") == {
        "$ref": "#/components/parameters/Test"
    }
    assert utils.build_reference("response", 3, "Test") == {
        "$ref": "#/components/responses/Test"
    }
    assert utils.build_reference("security_scheme", 3, "Test") == {
        "$ref": "#/components/securitySchemes/Test"
    }
