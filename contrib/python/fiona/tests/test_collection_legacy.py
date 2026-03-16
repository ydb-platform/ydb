# Testing collections and workspaces

import unittest
import re

import pytest

import fiona

from .conftest import WGS84PATTERN


@pytest.mark.usefixtures("unittest_path_coutwildrnp_shp")
class ReadingTest(unittest.TestCase):

    def setUp(self):
        self.c = fiona.open(self.path_coutwildrnp_shp, "r")

    def tearDown(self):
        self.c.close()

    def test_open_repr(self):
        assert repr(self.c) == (
            f"<open Collection '{self.path_coutwildrnp_shp}:coutwildrnp', "
            f"mode 'r' at {hex(id(self.c))}>"
        )

    def test_closed_repr(self):
        self.c.close()
        assert repr(self.c) == (
            f"<closed Collection '{self.path_coutwildrnp_shp}:coutwildrnp', "
            f"mode 'r' at {hex(id(self.c))}>"
        )

    def test_path(self):
        assert self.c.path == self.path_coutwildrnp_shp

    def test_name(self):
        assert self.c.name == 'coutwildrnp'

    def test_mode(self):
        assert self.c.mode == 'r'

    def test_encoding(self):
        assert self.c.encoding is None

    def test_iter(self):
        assert iter(self.c)

    def test_closed_no_iter(self):
        self.c.close()
        with pytest.raises(ValueError):
            iter(self.c)

    def test_len(self):
        assert len(self.c) == 67

    def test_closed_len(self):
        # Len is lazy, it's never computed in this case. TODO?
        self.c.close()
        assert len(self.c) == 0

    def test_len_closed_len(self):
        # Lazy len is computed in this case and sticks.
        len(self.c)
        self.c.close()
        assert len(self.c) == 67

    def test_driver(self):
        assert self.c.driver == "ESRI Shapefile"

    def test_closed_driver(self):
        self.c.close()
        assert self.c.driver is None

    def test_driver_closed_driver(self):
        self.c.driver
        self.c.close()
        assert self.c.driver == "ESRI Shapefile"

    def test_schema(self):
        s = self.c.schema['properties']
        assert s['PERIMETER'] == "float:24.15"
        assert s['NAME'] == "str:80"
        assert s['URL'] == "str:101"
        assert s['STATE_FIPS'] == "str:80"
        assert s['WILDRNP020'] == "int:10"

    def test_closed_schema(self):
        # Schema is lazy too, never computed in this case. TODO?
        self.c.close()
        assert self.c.schema is None

    def test_schema_closed_schema(self):
        self.c.schema
        self.c.close()
        assert sorted(self.c.schema.keys()) == ['geometry', 'properties']

    def test_crs(self):
        crs = self.c.crs
        assert crs['init'] == 'epsg:4326'

    def test_crs_wkt(self):
        crs = self.c.crs_wkt
        assert re.match(WGS84PATTERN, crs)

    def test_closed_crs(self):
        # Crs is lazy too, never computed in this case. TODO?
        self.c.close()
        assert self.c.crs is None

    def test_crs_closed_crs(self):
        self.c.crs
        self.c.close()
        assert sorted(self.c.crs.keys()) == ['init']

    def test_meta(self):
        assert (sorted(self.c.meta.keys()) ==
                ['crs', 'crs_wkt', 'driver', 'schema'])

    def test_profile(self):
        assert (sorted(self.c.profile.keys()) ==
                ['crs', 'crs_wkt', 'driver', 'schema'])

    def test_bounds(self):
        assert self.c.bounds[0] == pytest.approx(-113.564247)
        assert self.c.bounds[1] == pytest.approx(37.068981)
        assert self.c.bounds[2] == pytest.approx(-104.970871)
        assert self.c.bounds[3] == pytest.approx(41.996277)

    def test_context(self):
        with fiona.open(self.path_coutwildrnp_shp, "r") as c:
            assert c.name == 'coutwildrnp'
            assert len(c) == 67
        assert c.closed

    def test_iter_one(self):
        itr = iter(self.c)
        f = next(itr)
        assert f['id'] == "0"
        assert f['properties']['STATE'] == 'UT'

    def test_iter_list(self):
        f = list(self.c)[0]
        assert f['id'] == "0"
        assert f['properties']['STATE'] == 'UT'

    def test_re_iter_list(self):
        f = list(self.c)[0]  # Run through iterator
        f = list(self.c)[0]  # Run through a new, reset iterator
        assert f['id'] == "0"
        assert f['properties']['STATE'] == 'UT'

    def test_getitem_one(self):
        f = self.c[0]
        assert f['id'] == "0"
        assert f['properties']['STATE'] == 'UT'

    def test_getitem_iter_combo(self):
        i = iter(self.c)
        f = next(i)
        f = next(i)
        assert f['id'] == "1"
        f = self.c[0]
        assert f['id'] == "0"
        f = next(i)
        assert f['id'] == "2"

    def test_no_write(self):
        with pytest.raises(OSError):
            self.c.write({})

    def test_iter_items_list(self):
        i, f = list(self.c.items())[0]
        assert i == 0
        assert f['id'] == "0"
        assert f['properties']['STATE'] == 'UT'

    def test_iter_keys_list(self):
        i = list(self.c.keys())[0]
        assert i == 0

    def test_in_keys(self):
        assert 0 in self.c.keys()
        assert 0 in self.c
