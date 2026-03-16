import pytest
import tarfile
import yatest.common as yac

from mkdocs.config import config_options


@pytest.fixture(scope='session', autouse=True)
def fake_docs_dir():

    tarfile.open(yac.work_path('docs/resource.tar.gz')).extractall('docs')
