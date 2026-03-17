import logging
import sys
import os

import pytest
import boto3

import fiona
from fiona.errors import FionaDeprecationWarning
from fiona.vfs import vsi_path, parse_paths

from .test_collection import TestReading
from .test_collection_legacy import ReadingTest


# Custom markers (from rasterio)
mingdalversion = pytest.mark.skipif(
    fiona.gdal_version < (2, 1, 0), reason="S3 raster access requires GDAL 2.1"
)

credentials = pytest.mark.skipif(
    not (boto3.Session()._session.get_credentials()),
    reason="S3 raster access requires credentials",
)


# TODO: remove this once we've successfully moved the tar tests over
# to TestVsiReading.


class VsiReadingTest(ReadingTest):
    # There's a bug in GDAL 1.9.2 http://trac.osgeo.org/gdal/ticket/5093
    # in which the VSI driver reports the wrong number of features.
    # I'm overriding ReadingTest's test_filter_1 with a function that
    # passes and creating a new method in this class that we can exclude
    # from the test runner at run time.

    @pytest.mark.xfail(
        reason="The number of features present in the archive "
        "differs based on the GDAL version."
    )
    def test_filter_vsi(self):
        results = list(self.c.filter(bbox=(-114.0, 35.0, -104, 45.0)))
        assert len(results) == 67
        f = results[0]
        assert f["id"] == "0"
        assert f["properties"]["STATE"] == "UT"


class TestVsiReading(TestReading):
    # There's a bug in GDAL 1.9.2 http://trac.osgeo.org/gdal/ticket/5093
    # in which the VSI driver reports the wrong number of features.
    # I'm overriding TestReading's test_filter_1 with a function that
    # passes and creating a new method in this class that we can exclude
    # from the test runner at run time.

    @pytest.mark.xfail(
        reason="The number of features present in the archive "
        "differs based on the GDAL version."
    )
    def test_filter_vsi(self):
        results = list(self.c.filter(bbox=(-114.0, 35.0, -104, 45.0)))
        assert len(results) == 67
        f = results[0]
        assert f["id"] == "0"
        assert f["properties"]["STATE"] == "UT"


class TestZipReading(TestVsiReading):
    @pytest.fixture(autouse=True)
    def zipfile(self, data_dir, path_coutwildrnp_zip):
        self.c = fiona.open(f"zip://{path_coutwildrnp_zip}", "r")
        self.path = os.path.join(data_dir, "coutwildrnp.zip")
        yield
        self.c.close()

    def test_open_repr(self):
        path = f"/vsizip/{self.path}:coutwildrnp"
        assert repr(self.c) == (
            f"<open Collection '{path}', mode 'r' at {hex(id(self.c))}>"
        )

    def test_closed_repr(self):
        self.c.close()
        path = f"/vsizip/{self.path}:coutwildrnp"
        assert repr(self.c) == (
            f"<closed Collection '{path}', mode 'r' at {hex(id(self.c))}>"
        )

    def test_path(self):
        assert self.c.path == f"/vsizip/{self.path}"


class TestZipArchiveReading(TestVsiReading):
    @pytest.fixture(autouse=True)
    def zipfile(self, data_dir, path_coutwildrnp_zip):
        vfs = f"zip://{path_coutwildrnp_zip}"
        self.c = fiona.open(vfs + "!coutwildrnp.shp", "r")
        self.path = os.path.join(data_dir, "coutwildrnp.zip")
        yield
        self.c.close()

    def test_open_repr(self):
        path = f"/vsizip/{self.path}/coutwildrnp.shp:coutwildrnp"
        assert repr(self.c) == (
            f"<open Collection '{path}', mode 'r' at {hex(id(self.c))}>"
        )

    def test_closed_repr(self):
        self.c.close()
        path = f"/vsizip/{self.path}/coutwildrnp.shp:coutwildrnp"
        assert repr(self.c) == (
            f"<closed Collection '{path}', mode 'r' at {hex(id(self.c))}>"
        )

    def test_path(self):
        assert self.c.path == f"/vsizip/{self.path}/coutwildrnp.shp"


class TestZipArchiveReadingAbsPath(TestZipArchiveReading):
    @pytest.fixture(autouse=True)
    def zipfile(self, path_coutwildrnp_zip):
        vfs = f"zip://{os.path.abspath(path_coutwildrnp_zip)}"
        self.c = fiona.open(vfs + "!coutwildrnp.shp", "r")
        yield
        self.c.close()

    def test_open_repr(self):
        assert repr(self.c).startswith("<open Collection '/vsizip/")

    def test_closed_repr(self):
        self.c.close()
        assert repr(self.c).startswith("<closed Collection '/vsizip/")

    def test_path(self):
        assert self.c.path.startswith("/vsizip/")


@pytest.mark.usefixtures("uttc_path_coutwildrnp_tar", "uttc_data_dir")
class TarArchiveReadingTest(VsiReadingTest):
    def setUp(self):
        vfs = f"tar://{self.path_coutwildrnp_tar}"
        self.c = fiona.open(vfs + "!testing/coutwildrnp.shp", "r")
        self.path = os.path.join(self.data_dir, "coutwildrnp.tar")

    def tearDown(self):
        self.c.close()

    def test_open_repr(self):
        path = f"/vsitar/{self.path}/testing/coutwildrnp.shp:coutwildrnp"
        assert repr(self.c) == (
            f"<open Collection '{path}', mode 'r' at {hex(id(self.c))}>"
        )

    def test_closed_repr(self):
        self.c.close()
        path = f"/vsitar/{self.path}/testing/coutwildrnp.shp:coutwildrnp"
        assert repr(self.c) == (
            f"<closed Collection '{path}', mode 'r' at {hex(id(self.c))}>"
        )

    def test_path(self):
        assert self.c.path == f"/vsitar/{self.path}/testing/coutwildrnp.shp"


@pytest.mark.network
def test_open_http():
    ds = fiona.open(
        "https://raw.githubusercontent.com/OSGeo/gdal/master/autotest/ogr/data/poly.shp"
    )
    assert len(ds) == 10


@credentials
@mingdalversion
@pytest.mark.network
def test_open_s3():
    ds = fiona.open("zip+s3://fiona-testing/coutwildrnp.zip")
    assert len(ds) == 67


@credentials
@pytest.mark.network
def test_open_zip_https():
    ds = fiona.open("zip+https://s3.amazonaws.com/fiona-testing/coutwildrnp.zip")
    assert len(ds) == 67


def test_parse_path():
    assert parse_paths("zip://foo.zip") == ("foo.zip", "zip", None)


def test_parse_path2():
    assert parse_paths("foo") == ("foo", None, None)


def test_parse_vfs():
    assert parse_paths("/", "zip://foo.zip") == ("/", "zip", "foo.zip")
