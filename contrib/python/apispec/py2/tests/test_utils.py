# -*- coding: utf-8 -*-
import pytest

from apispec import utils, APISpec
from apispec.exceptions import APISpecError

def test_load_yaml_from_docstring():
    def f():
        """
        Foo
            bar
            baz quux

        ---
        herp: 1
        derp: 2
        """
    result = utils.load_yaml_from_docstring(f.__doc__)
    assert result == {'herp': 1, 'derp': 2}

def _test_validate_swagger_is_deprecated():
    spec = APISpec(
        title='Pets',
        version='0.1',
        openapi_version='3.0.0',
    )
    with pytest.warns(DeprecationWarning, match='validate_spec'):
        utils.validate_swagger(spec)


class TestOpenAPIVersion:

    @pytest.mark.parametrize('version', ('1.0', '4.0'))
    def test_openapi_version_invalid_version(self, version):
        with pytest.raises(APISpecError) as excinfo:
            utils.OpenAPIVersion(version)
        assert 'Not a valid OpenAPI version number:' in str(excinfo)

    @pytest.mark.parametrize('version', ('3.0.1', utils.OpenAPIVersion('3.0.1')))
    def test_openapi_version_string_or_openapi_version_param(self, version):
        assert utils.OpenAPIVersion(version) == utils.OpenAPIVersion('3.0.1')

    def test_openapi_version_digits(self):
        ver = utils.OpenAPIVersion('3.0.1')
        assert ver.major == 3
        assert ver.minor == 0
        assert ver.patch == 1
        assert ver.vstring == '3.0.1'
        assert str(ver) == '3.0.1'
