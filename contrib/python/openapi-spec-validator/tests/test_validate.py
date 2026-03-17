import os
import pytest
from openapi_spec_validator.exceptions import OpenAPIValidationError
from tests.util import ArcadiaFilesResourcesAdaptor


class BaseTestValidOpeAPIv3Validator(ArcadiaFilesResourcesAdaptor):

    @pytest.fixture
    def spec_url(self):
        yield ''

    def test_valid(self, validator, spec, spec_url):
        return validator.validate(spec, spec_url=spec_url)


class BaseTestFailedOpeAPIv3Validator(ArcadiaFilesResourcesAdaptor):

    @pytest.fixture
    def spec_url(self):
        yield ''

    def test_failed(self, validator, spec, spec_url):
        with pytest.raises(OpenAPIValidationError):
            validator.validate(spec, spec_url=spec_url)


class TestLocalEmptyExample(BaseTestFailedOpeAPIv3Validator):
    RESOURCE = "data/v3.0/empty.yaml"

    @pytest.fixture
    def spec(self, factory, resource_file):
        yield factory.spec_from_file(resource_file)


class TestLocalPetstoreExample(BaseTestValidOpeAPIv3Validator):
    RESOURCE = "data/v3.0/petstore.yaml"

    @pytest.fixture
    def spec(self, factory, resource_file):
        yield factory.spec_from_file(resource_file)


class TestLocalPetstoreSeparateExample(BaseTestValidOpeAPIv3Validator):
    RESOURCES = [
        ('data/v3.0/petstore-separate/spec/openapi.yaml', 'spec/openapi.yaml'),
        ('data/v3.0/petstore-separate/spec/schemas/Pet.yaml', 'spec/schemas/Pet.yaml'),
        ('data/v3.0/petstore-separate/spec/schemas/Pets.yaml', 'spec/schemas/Pets.yaml'),
        ('data/v3.0/petstore-separate/common/schemas/Error.yaml', 'common/schemas/Error.yaml'),
    ]

    @pytest.fixture
    def spec_url(self, factory, resources_dir):
        path = os.path.join(resources_dir, 'spec/openapi.yaml')
        yield factory.spec_url(path)

    @pytest.fixture
    def spec(self, factory, resources_dir):
        path = os.path.join(resources_dir, 'spec/openapi.yaml')
        yield factory.spec_from_file(path)


class TestLocalParentReferenceExample(BaseTestValidOpeAPIv3Validator):
    RESOURCES = [
        ('data/v3.0/parent-reference/common.yaml', 'common.yaml'),
        ('data/v3.0/parent-reference/openapi.yaml', 'openapi.yaml'),
        ('data/v3.0/parent-reference/spec/components.yaml', 'spec/components.yaml'),
    ]

    @pytest.fixture
    def spec_url(self, factory, resources_dir):
        path = os.path.join(resources_dir, 'openapi.yaml')
        yield factory.spec_url(path)

    @pytest.fixture
    def spec(self, factory, resources_dir):
        path = os.path.join(resources_dir, 'openapi.yaml')
        yield factory.spec_from_file(path)


class TestPetstoreExample(BaseTestValidOpeAPIv3Validator):
    RESOURCE = 'data/v3.0/petstore.yaml'

    @pytest.fixture
    def spec(self, factory, resource_file):
        yield factory.spec_from_file(resource_file)


class TestApiWithExampe(BaseTestValidOpeAPIv3Validator):
    RESOURCE = 'data/v3.0/api-with-examples.yaml'

    @pytest.fixture
    def spec(self, factory, resource_url):
        yield factory.spec_from_url(resource_url)


class TestPetstoreExpandedExample(BaseTestValidOpeAPIv3Validator):
    RESOURCE = 'data/v3.0/api-with-examples-expanded.yaml'

    @pytest.fixture
    def spec(self, factory, resource_url):
        yield factory.spec_from_url(resource_url)
