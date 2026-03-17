"""Tests of path and field encoding."""

import os
import shutil
import sys
import tempfile
import unittest

import pytest

import fiona
from fiona.errors import SchemaError
from fiona.model import Feature


class TestUnicodePath(unittest.TestCase):
    def setUp(self):
        tempdir = tempfile.mkdtemp()
        self.dir = os.path.join(tempdir, "français")
        shutil.copytree(os.path.join(os.path.dirname(__file__), "data"), self.dir)

    def tearDown(self):
        shutil.rmtree(os.path.dirname(self.dir))

    def test_unicode_path(self):
        path = self.dir + "/coutwildrnp.shp"
        with fiona.open(path) as c:
            assert len(c) == 67

    def test_unicode_path_layer(self):
        path = self.dir
        layer = "coutwildrnp"
        with fiona.open(path, layer=layer) as c:
            assert len(c) == 67

    def test_utf8_path(self):
        path = self.dir + "/coutwildrnp.shp"
        if sys.version_info < (3,):
            with fiona.open(path) as c:
                assert len(c) == 67


class TestUnicodeStringField(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    @pytest.mark.xfail(reason="OGR silently fails to convert strings")
    def test_write_mismatch(self):
        """TOFIX: OGR silently fails to convert strings"""
        # Details:
        #
        # If we tell OGR that we want a latin-1 encoded output file and
        # give it a feature with a unicode property that can't be converted
        # to latin-1, no error is raised and OGR just writes the utf-8
        # encoded bytes to the output file.
        #
        # This might be shapefile specific.
        #
        # Consequences: no error on write, but there will be an error
        # on reading the data and expecting latin-1.
        schema = {"geometry": "Point", "properties": {"label": "str", "num": "int"}}

        with fiona.open(
            os.path.join(self.tempdir, "test-write-fail.shp"),
            "w",
            driver="ESRI Shapefile",
            schema=schema,
            encoding="latin1",
        ) as c:
            c.writerecords(
                [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0, 0]},
                        "properties": {"label": "徐汇区", "num": 0},
                    }
                ]
            )

        with fiona.open(os.path.join(self.tempdir), encoding="latin1") as c:
            f = next(iter(c))
            # Next assert fails.
            assert f.properties["label"] == "徐汇区"

    def test_write_utf8(self):
        schema = {
            "geometry": "Point",
            "properties": {"label": "str", "verit\xe9": "int"},
        }
        with fiona.open(
            os.path.join(self.tempdir, "test-write.shp"),
            "w",
            "ESRI Shapefile",
            schema=schema,
            encoding="utf-8",
        ) as c:
            c.writerecords(
                [
                    Feature.from_dict(
                        **{
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [0, 0]},
                            "properties": {"label": "Ba\u2019kelalan", "verit\xe9": 0},
                        }
                    )
                ]
            )

        with fiona.open(os.path.join(self.tempdir), encoding="utf-8") as c:
            f = next(iter(c))
            assert f.properties["label"] == "Ba\u2019kelalan"
            assert f.properties["verit\xe9"] == 0

    @pytest.mark.iconv
    def test_write_gb18030(self):
        """Can write a simplified Chinese shapefile"""
        schema = {"geometry": "Point", "properties": {"label": "str", "num": "int"}}
        with fiona.open(
            os.path.join(self.tempdir, "test-write-gb18030.shp"),
            "w",
            driver="ESRI Shapefile",
            schema=schema,
            encoding="gb18030",
        ) as c:
            c.writerecords(
                [
                    Feature.from_dict(
                        **{
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [0, 0]},
                            "properties": {"label": "徐汇区", "num": 0},
                        }
                    )
                ]
            )

        with fiona.open(os.path.join(self.tempdir), encoding="gb18030") as c:
            f = next(iter(c))
            assert f.properties["label"] == "徐汇区"
            assert f.properties["num"] == 0

    @pytest.mark.iconv
    def test_gb2312_field_wrong_encoding(self):
        """Attempt to create field with a name not supported by the encoding

        ESRI Shapefile driver defaults to ISO-8859-1 encoding if none is
        specified. This doesn't support the field name used. Previously this
        went undetected and would raise a KeyError later when the user tried
        to write a feature to the layer. Instead we raise a more useful error.

        See GH#595.
        """
        field_name = "区县名称"
        meta = {
            "schema": {
                "properties": {field_name: "int"},
                "geometry": "Point",
            },
            "driver": "ESRI Shapefile",
        }
        feature = Feature.from_dict(
            **{
                "properties": {field_name: 123},
                "geometry": {"type": "Point", "coordinates": [1, 2]},
            }
        )
        # when encoding is specified, write is successful
        with fiona.open(
            os.path.join(self.tempdir, "test1.shp"), "w", encoding="GB2312", **meta
        ) as collection:
            collection.write(feature)
        # no encoding
        with pytest.raises(SchemaError):
            fiona.open(os.path.join(self.tempdir, "test2.shp"), "w", **meta)
