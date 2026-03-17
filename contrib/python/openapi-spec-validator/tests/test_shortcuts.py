import pytest

from openapi_spec_validator import validate_spec, validate_spec_url
from openapi_spec_validator import validate_v2_spec, validate_v2_spec_url
from openapi_spec_validator.exceptions import OpenAPIValidationError
from tests.util import ArcadiaFilesResourcesAdaptor


class BaseTestValidValidteV2Spec(ArcadiaFilesResourcesAdaptor):

    def test_valid(self, spec):
        validate_v2_spec(spec)


class BaseTestFaliedValidateV2Spec(ArcadiaFilesResourcesAdaptor):

    def test_failed(self, spec):
        with pytest.raises(OpenAPIValidationError):
            validate_v2_spec(spec)


class BaseTestValidValidteSpec(ArcadiaFilesResourcesAdaptor):

    def test_valid(self, spec):
        validate_spec(spec)


class BaseTestFaliedValidateSpec(ArcadiaFilesResourcesAdaptor):

    def test_failed(self, spec):
        with pytest.raises(OpenAPIValidationError):
            validate_spec(spec)


class BaseTestValidValidteV2SpecUrl(ArcadiaFilesResourcesAdaptor):

    def test_valid(self, spec_url):
        validate_v2_spec_url(spec_url)


class BaseTestFaliedValidateV2SpecUrl(ArcadiaFilesResourcesAdaptor):

    def test_failed(self, spec_url):
        with pytest.raises(OpenAPIValidationError):
            validate_v2_spec_url(spec_url)


class BaseTestValidValidteSpecUrl(ArcadiaFilesResourcesAdaptor):

    def test_valid(self, spec_url):
        validate_spec_url(spec_url)


class BaseTestFaliedValidateSpecUrl(ArcadiaFilesResourcesAdaptor):

    def test_failed(self, spec_url):
        with pytest.raises(OpenAPIValidationError):
            validate_spec_url(spec_url)


class TestLocalEmptyExample(BaseTestFaliedValidateSpec):
    RESOURCE = "data/v3.0/empty.yaml"

    @pytest.fixture
    def spec(self, factory, resource_file):
        return factory.spec_from_file(resource_file)


class TestLocalPetstoreV2Example(BaseTestValidValidteV2Spec):
    RESOURCE = "data/v2.0/petstore.yaml"

    @pytest.fixture
    def spec(self, factory, resource_file):
        return factory.spec_from_file(resource_file)


class TestLocalPetstoreExample(BaseTestValidValidteSpec):
    RESOURCE = "data/v3.0/petstore.yaml"

    @pytest.fixture
    def spec(self, factory, resource_file):
        return factory.spec_from_file(resource_file)


class TestPetstoreV2Example(BaseTestValidValidteV2SpecUrl):
    RESOURCE = "data/v2.0/petstore.yaml"

    @pytest.fixture
    def spec_url(self, resource_url):
        yield resource_url


class TestApiV2WithExample(BaseTestValidValidteV2SpecUrl):
    RESOURCE = 'data/v2.0/api-with-examples.yaml'

    @pytest.fixture
    def spec_url(self, resource_url):
        yield resource_url


class TestPetstoreV2ExpandedExample(BaseTestValidValidteV2SpecUrl):
    RESOURCE = 'data/v2.0/petstore-expanded.yaml'

    @pytest.fixture
    def spec_url(self, resource_url):
        yield resource_url


class TestPetstoreExample(BaseTestValidValidteSpecUrl):
    RESOURCE = 'data/v3.0/petstore.yaml'

    @pytest.fixture
    def spec_url(self, resource_url):
        yield resource_url


class TestApiWithExampe(BaseTestValidValidteSpecUrl):
    RESOURCE = 'data/v3.0/api-with-examples.yaml'

    @pytest.fixture
    def spec_url(self, resource_url):
        yield resource_url


class TestPetstoreExpandedExample(BaseTestValidValidteSpecUrl):
    RESOURCE = 'data/v3.0/api-with-examples-expanded.yaml'

    @pytest.fixture
    def spec_url(self, resource_url):
        yield resource_url
