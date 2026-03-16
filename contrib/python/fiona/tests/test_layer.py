"""Layer tests."""

import pytest

import fiona
from .test_collection import TestReading


def test_index_selection(path_coutwildrnp_shp):
    with fiona.open(path_coutwildrnp_shp, 'r', layer=0) as c:
        assert len(c) == 67


class TestFileReading(TestReading):
    @pytest.fixture(autouse=True)
    def shapefile(self, path_coutwildrnp_shp):
        self.c = fiona.open(path_coutwildrnp_shp, 'r', layer='coutwildrnp')
        yield
        self.c.close()

    def test_open_repr(self, path_coutwildrnp_shp):
        assert repr(self.c) == (
            f"<open Collection '{path_coutwildrnp_shp}:coutwildrnp', "
            f"mode 'r' at {hex(id(self.c))}>"
        )

    def test_closed_repr(self, path_coutwildrnp_shp):
        self.c.close()
        assert repr(self.c) == (
            f"<closed Collection '{path_coutwildrnp_shp}:coutwildrnp', "
            f"mode 'r' at {hex(id(self.c))}>"
        )

    def test_name(self):
        assert self.c.name == 'coutwildrnp'


class TestDirReading(TestReading):
    @pytest.fixture(autouse=True)
    def shapefile(self, data_dir):
        self.c = fiona.open(data_dir, "r", layer="coutwildrnp")
        yield
        self.c.close()

    def test_open_repr(self, data_dir):
        assert repr(self.c) == (
            f"<open Collection '{data_dir}:coutwildrnp', "
            f"mode 'r' at {hex(id(self.c))}>"
        )

    def test_closed_repr(self, data_dir):
        self.c.close()
        assert repr(self.c) == (
            f"<closed Collection '{data_dir}:coutwildrnp', "
            f"mode 'r' at {hex(id(self.c))}>"
        )

    def test_name(self):
        assert self.c.name == 'coutwildrnp'

    def test_path(self, data_dir):
        assert self.c.path == data_dir


def test_invalid_layer(path_coutwildrnp_shp):
    with pytest.raises(ValueError):
        fiona.open(path_coutwildrnp_shp, layer="foo")


def test_write_invalid_numeric_layer(path_coutwildrnp_shp, tmpdir):
    with pytest.raises(ValueError):
        fiona.open(str(tmpdir.join("test-no-iter.shp")), mode='w', layer=0)
