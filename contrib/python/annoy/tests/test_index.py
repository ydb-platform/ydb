from annoy import AnnoyIndex
import pytest
import numpy as np
import tempfile


@pytest.fixture
def index():
    index = AnnoyIndex(3)
    index.add_item(0, [0, 1.1, 0])
    index.add_item(1, [0, 1.2, 0])
    index.add_item(2, [0, 0, 1.3])
    index.build(-1)
    return index


def test_online(index):
    assert np.allclose(index.get_item_vector(0), [0, 1.1, 0])
    assert np.allclose(index.get_item_vector(1), [0, 1.2, 0])
    assert np.allclose(index.get_item_vector(2), [0, 0, 1.3])


def test_offline(index):
    with tempfile.NamedTemporaryFile() as tmp:
        index.save(tmp.name)
        assert np.allclose(index.get_item_vector(0), [0, 1.1, 0])
        assert np.allclose(index.get_item_vector(1), [0, 1.2, 0])
        assert np.allclose(index.get_item_vector(2), [0, 0, 1.3])
