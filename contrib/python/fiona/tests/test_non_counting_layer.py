import unittest

import pytest

import fiona
from fiona.errors import FionaDeprecationWarning


@pytest.mark.usefixtures('uttc_path_gpx')
class TestNonCountingLayer(unittest.TestCase):
    def setUp(self):
        self.c = fiona.open(self.path_gpx, "r", layer="track_points")

    def tearDown(self):
        self.c.close()

    def test_len_fail(self):
        with pytest.raises(TypeError):
            len(self.c)

    def test_list(self):
        features = list(self.c)
        assert len(features) == 19

    def test_getitem(self):
        self.c[2]

    def test_fail_getitem_negative_index(self):
        with pytest.raises(IndexError):
            self.c[-1]

    def test_slice(self):
        with pytest.warns(FionaDeprecationWarning):
            features = self.c[2:5]
            assert len(features) == 3

    def test_warn_slice_negative_index(self):
        with pytest.warns((FionaDeprecationWarning, RuntimeWarning)):
            self.c[2:-4]
