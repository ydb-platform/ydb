"""Tests for ``fiona.BytesCollection()``."""


import pytest

import fiona
from fiona.model import Geometry


class TestReading:
    @pytest.fixture(autouse=True)
    def bytes_collection_object(self, path_coutwildrnp_json):
        with open(path_coutwildrnp_json) as src:
            bytesbuf = src.read().encode("utf-8")
        self.c = fiona.BytesCollection(bytesbuf, encoding="utf-8")
        yield
        self.c.close()

    def test_construct_with_str(self, path_coutwildrnp_json):
        with open(path_coutwildrnp_json) as src:
            strbuf = src.read()
        with pytest.raises(ValueError):
            fiona.BytesCollection(strbuf)

    def test_open_repr(self):
        # I'm skipping checking the name of the virtual file as it produced by uuid.
        print(repr(self.c))
        assert repr(self.c).startswith("<open BytesCollection '/vsimem/")

    def test_closed_repr(self):
        # I'm skipping checking the name of the virtual file as it produced by uuid.
        self.c.close()
        print(repr(self.c))
        assert repr(self.c).startswith("<closed BytesCollection '/vsimem/")

    def test_path(self):
        assert self.c.path == self.c.virtual_file

    def test_closed_virtual_file(self):
        self.c.close()
        assert self.c.virtual_file is None

    def test_closed_buf(self):
        self.c.close()
        assert self.c.bytesbuf is None

    def test_name(self):
        assert len(self.c.name) > 0

    def test_mode(self):
        assert self.c.mode == "r"

    def test_collection(self):
        assert self.c.encoding == "utf-8"

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
        assert self.c.driver == "GeoJSON"

    def test_closed_driver(self):
        self.c.close()
        assert self.c.driver is None

    def test_driver_closed_driver(self):
        self.c.driver
        self.c.close()
        assert self.c.driver == "GeoJSON"

    def test_schema(self):
        s = self.c.schema["properties"]
        assert s["PERIMETER"] == "float"
        assert s["NAME"] == "str"
        assert s["URL"] == "str"
        assert s["STATE_FIPS"] == "str"
        assert s["WILDRNP020"] == "int32"

    def test_closed_schema(self):
        # Schema is lazy too, never computed in this case. TODO?
        self.c.close()
        assert self.c.schema is None

    def test_schema_closed_schema(self):
        self.c.schema
        self.c.close()
        assert sorted(self.c.schema.keys()) == ["geometry", "properties"]

    def test_crs(self):
        assert self.c.crs["init"] == "epsg:4326"

    def test_crs_wkt(self):
        assert self.c.crs_wkt.startswith('GEOGCS["WGS 84"')

    def test_closed_crs(self):
        # Crs is lazy too, never computed in this case. TODO?
        self.c.close()
        assert self.c.crs is None

    def test_crs_closed_crs(self):
        self.c.crs
        self.c.close()
        assert sorted(self.c.crs.keys()) == ["init"]

    def test_meta(self):
        assert sorted(self.c.meta.keys()) == ["crs", "crs_wkt", "driver", "schema"]

    def test_bounds(self):
        assert self.c.bounds[0] == pytest.approx(-113.564247)
        assert self.c.bounds[1] == pytest.approx(37.068981)
        assert self.c.bounds[2] == pytest.approx(-104.970871)
        assert self.c.bounds[3] == pytest.approx(41.996277)

    def test_iter_one(self):
        itr = iter(self.c)
        f = next(itr)
        assert f["id"] == "0"
        assert f["properties"]["STATE"] == "UT"

    def test_iter_list(self):
        f = list(self.c)[0]
        assert f["id"] == "0"
        assert f["properties"]["STATE"] == "UT"

    def test_re_iter_list(self):
        f = list(self.c)[0]  # Run through iterator
        f = list(self.c)[0]  # Run through a new, reset iterator
        assert f["id"] == "0"
        assert f["properties"]["STATE"] == "UT"

    def test_getitem_one(self):
        f = self.c[0]
        assert f["id"] == "0"
        assert f["properties"]["STATE"] == "UT"

    def test_no_write(self):
        with pytest.raises(OSError):
            self.c.write({})

    def test_iter_items_list(self):
        i, f = list(self.c.items())[0]
        assert i == 0
        assert f["id"] == "0"
        assert f["properties"]["STATE"] == "UT"

    def test_iter_keys_list(self):
        i = list(self.c.keys())[0]
        assert i == 0

    def test_in_keys(self):
        assert 0 in self.c.keys()
        assert 0 in self.c


class TestFilterReading:
    @pytest.fixture(autouse=True)
    def bytes_collection_object(self, path_coutwildrnp_json):
        with open(path_coutwildrnp_json) as src:
            bytesbuf = src.read().encode("utf-8")
        self.c = fiona.BytesCollection(bytesbuf)
        yield
        self.c.close()

    def test_filter_1(self):
        results = list(self.c.filter(bbox=(-120.0, 30.0, -100.0, 50.0)))
        assert len(results) == 67
        f = results[0]
        assert f["id"] == "0"
        assert f["properties"]["STATE"] == "UT"

    def test_filter_reset(self):
        results = list(self.c.filter(bbox=(-112.0, 38.0, -106.0, 40.0)))
        assert len(results) == 26
        results = list(self.c.filter())
        assert len(results) == 67

    def test_filter_mask(self):
        mask = Geometry.from_dict(
            **{
                "type": "Polygon",
                "coordinates": (
                    ((-112, 38), (-112, 40), (-106, 40), (-106, 38), (-112, 38)),
                ),
            }
        )
        results = list(self.c.filter(mask=mask))
        assert len(results) == 26


def test_zipped_bytes_collection(bytes_coutwildrnp_zip):
    """Open a zipped stream of bytes as a collection"""
    with fiona.BytesCollection(bytes_coutwildrnp_zip) as col:
        assert col.name == "coutwildrnp"
        assert len(col) == 67


@pytest.mark.skipif(
    fiona.gdal_version >= (2, 3, 0),
    reason="Changed behavior with gdal 2.3, possibly related to RFC 70:"
    "Guessing output format from output file name extension for utilities",
)
def test_grenada_bytes_geojson(bytes_grenada_geojson):
    """Read grenada.geojson as BytesCollection.

    grenada.geojson is an example of geojson that GDAL's GeoJSON
    driver will fail to read successfully unless the file's extension
    reflects its json'ness.
    """
    # We expect an exception if the GeoJSON driver isn't specified.
    with pytest.raises(fiona.errors.FionaValueError):
        with fiona.BytesCollection(bytes_grenada_geojson) as col:
            pass

    # If told what driver to use, we should be good.
    with fiona.BytesCollection(bytes_grenada_geojson, driver="GeoJSON") as col:
        assert len(col) == 1
