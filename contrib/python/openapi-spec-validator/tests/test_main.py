import mock

import pytest
from six import StringIO

from openapi_spec_validator.__main__ import main
from tests.util import ArcadiaFilesResourcesAdaptor


class TestSchemaDefault(ArcadiaFilesResourcesAdaptor):
    RESOURCE = 'data/v3.0/petstore.yaml'

    def test_schema_default(self, resource_file):
        """Test default schema is 3.0.0"""
        testargs = [resource_file]
        main(testargs)


class TestSchemaV3(ArcadiaFilesResourcesAdaptor):
    RESOURCE = 'data/v3.0/petstore.yaml'

    def test_schema_v3(self, resource_file):
        """No errors when calling proper v3 file."""
        testargs = ['--schema', '3.0.0', resource_file]
        main(testargs)


class TestSchemaV2(ArcadiaFilesResourcesAdaptor):
    RESOURCE = 'data/v2.0/petstore.yaml'

    def test_schema_v2(self, resource_file):
        """No errors when calling with proper v2 file."""
        testargs = ['--schema', '2.0', resource_file]
        main(testargs)


class TestSchemaUnknown(ArcadiaFilesResourcesAdaptor):
    RESOURCE = 'data/v2.0/petstore.yaml'

    def test_schema_unknown(self, resource_file):
        """Errors on running with unknown schema."""
        testargs = ['--schema', 'x.x', resource_file]
        with pytest.raises(SystemExit):
            main(testargs)


class TestValidationError(ArcadiaFilesResourcesAdaptor):
    RESOURCE = 'data/v2.0/petstore.yaml'

    def test_validation_error(self, resource_file):
        """SystemExit on running with ValidationError."""
        testargs = ['--schema', '3.0.0', resource_file]
        with pytest.raises(SystemExit):
            main(testargs)


class TestUnknownError(ArcadiaFilesResourcesAdaptor):
    RESOURCE = 'data/v2.0/petstore.yaml'

    @mock.patch(
        'openapi_spec_validator.__main__.openapi_v3_spec_validator.validate',
        side_effect=Exception,
    )
    def test_unknown_error(self, m_validate, resource_file):
        """SystemExit on running with unknown error."""
        testargs = ['--schema', '3.0.0',
                    resource_file]
        with pytest.raises(SystemExit):
            main(testargs)


class TestNonexistingFile(ArcadiaFilesResourcesAdaptor):
    def test_nonexisting_file(self):
        """Calling with non-existing file should sys.exit."""
        testargs = ['i_dont_exist.yaml']
        with pytest.raises(SystemExit):
            main(testargs)


class TestSchemaStdin(ArcadiaFilesResourcesAdaptor):
    RESOURCE = 'data/v3.0/petstore.yaml'

    def test_schema_stdin(self, resource_file):
        """Test schema from STDIN"""
        with open(resource_file, 'r') as spec_file:
            spec_lines = spec_file.readlines()
        spec_io = StringIO("".join(spec_lines))

        testargs = ['-']
        with mock.patch('openapi_spec_validator.__main__.sys.stdin', spec_io):
            main(testargs)
