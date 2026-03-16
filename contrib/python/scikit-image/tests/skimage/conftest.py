import pytest
from pathlib import Path


@pytest.fixture
def test_root_dir():
    import yatest.common as yc
    return Path(yc.source_path("contrib/python/scikit-image/tests/skimage/conftest.py")).absolute().parent
