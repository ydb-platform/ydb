import os
import pytest
import tempfile
import shutil
from library.python import resource


# XXX: PATCH adapted tests for Arcadia
class ArcadiaFilesResourcesAdaptor(object):
    RESOURCE = ''
    RESOURCES = []  # should be list of pairs

    @pytest.fixture
    def resource_content(self):
        if not self.RESOURCE:
            raise pytest.UsageError(
                'RESOURCE attr should be specified for this fixture usage'
            )
        content = resource.find(self.RESOURCE)
        return content

    @pytest.fixture
    def resource_file(self, resource_content):
        with tempfile.NamedTemporaryFile() as f:
            f.write(resource_content)
            f.flush()

            yield f.name

    @pytest.fixture
    def resource_url(self, resource_content, httpserver):
        httpserver.serve_content(resource_content, code=200)
        url = httpserver.url + '/' + self.RESOURCE
        yield url

    @pytest.fixture
    def resources_dir(self):
        tempdir = tempfile.mkdtemp()
        for resource_key, filename in self.RESOURCES:
            content = resource.find(resource_key)
            path = os.path.join(tempdir, filename)
            directory = os.path.dirname(path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            with open(path, 'w+b') as f:
                f.write(content)

        yield tempdir
        shutil.rmtree(tempdir)
