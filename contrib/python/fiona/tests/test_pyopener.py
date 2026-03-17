"""Tests of the Python opener VSI plugin."""

import io
import os

import fsspec
import pytest

import fiona
from fiona.model import Feature


def test_opener_io_open(path_grenada_geojson):
    """Use io.open as opener."""
    with fiona.open(path_grenada_geojson, opener=io.open) as colxn:
        profile = colxn.profile
        assert profile["driver"] == "GeoJSON"
        assert len(colxn) == 1


def test_opener_fsspec_zip_fs():
    """Use fsspec zip filesystem as opener."""
    fs = fsspec.filesystem("zip", fo="tests/data/coutwildrnp.zip")
    with fiona.open("coutwildrnp.shp", opener=fs) as colxn:
        profile = colxn.profile
        assert profile["driver"] == "ESRI Shapefile"
        assert len(colxn) == 67
        assert colxn.schema["geometry"] == "Polygon"
        assert "AGBUR" in colxn.schema["properties"]


def test_opener_fsspec_zip_http_fs():
    """Use fsspec zip+http filesystem as opener."""
    fs = fsspec.filesystem(
        "zip",
        target_protocol="http",
        fo="https://github.com/Toblerity/Fiona/files/11151652/coutwildrnp.zip",
    )
    with fiona.open("coutwildrnp.shp", opener=fs) as colxn:
        profile = colxn.profile
        assert profile["driver"] == "ESRI Shapefile"
        assert len(colxn) == 67
        assert colxn.schema["geometry"] == "Polygon"
        assert "AGBUR" in colxn.schema["properties"]


def test_opener_tiledb_file():
    """Use tiledb vfs as opener."""
    tiledb = pytest.importorskip("tiledb")
    fs = tiledb.VFS()
    with fiona.open("tests/data/coutwildrnp.shp", opener=fs) as colxn:
        profile = colxn.profile
        assert profile["driver"] == "ESRI Shapefile"
        assert len(colxn) == 67
        assert colxn.schema["geometry"] == "Polygon"
        assert "AGBUR" in colxn.schema["properties"]


def test_opener_fsspec_fs_write(tmp_path):
    """Write a feature via an fsspec fs opener."""
    schema = {"geometry": "Point", "properties": {"zero": "int"}}
    feature = Feature.from_dict(
        **{
            "geometry": {"type": "Point", "coordinates": (0, 0)},
            "properties": {"zero": "0"},
        }
    )
    fs = fsspec.filesystem("file")
    outputfile = tmp_path.joinpath("test.shp")

    with fiona.open(
        str(outputfile),
        "w",
        driver="ESRI Shapefile",
        schema=schema,
        crs="OGC:CRS84",
        opener=fs,
    ) as collection:
        collection.write(feature)
        assert len(collection) == 1
        assert collection.crs == "OGC:CRS84"


def test_threads_context():
    import io
    from threading import Thread


    def target():
        with fiona.open("tests/data/coutwildrnp.shp", opener=io.open) as colxn:
            print(colxn.profile)
            assert len(colxn) == 67


    thread = Thread(target=target)
    thread.start()
    thread.join()


def test_overwrite(data):
    """Opener can overwrite data."""
    schema = {"geometry": "Point", "properties": {"zero": "int"}}
    feature = Feature.from_dict(
        **{
            "geometry": {"type": "Point", "coordinates": (0, 0)},
            "properties": {"zero": "0"},
        }
    )
    fs = fsspec.filesystem("file")
    outputfile = os.path.join(str(data), "coutwildrnp.shp")

    with fiona.open(
        str(outputfile),
        "w",
        driver="ESRI Shapefile",
        schema=schema,
        crs="OGC:CRS84",
        opener=fs,
    ) as collection:
        collection.write(feature)
        assert len(collection) == 1
        assert collection.crs == "OGC:CRS84"


def test_opener_fsspec_zip_fs_listlayers():
    """Use fsspec zip filesystem as opener for listlayers()."""
    fs = fsspec.filesystem("zip", fo="tests/data/coutwildrnp.zip")
    assert fiona.listlayers("coutwildrnp.shp", opener=fs) == ["coutwildrnp"]


def test_opener_fsspec_zip_fs_listdir():
    """Use fsspec zip filesystem as opener for listdir()."""
    fs = fsspec.filesystem("zip", fo="tests/data/coutwildrnp.zip")
    listing = fiona.listdir("/", opener=fs)
    assert len(listing) == 4
    assert set(
        ["coutwildrnp.shp", "coutwildrnp.dbf", "coutwildrnp.shx", "coutwildrnp.prj"]
    ) & set(listing)


def test_opener_fsspec_file_fs_listdir():
    """Use fsspec file filesystem as opener for listdir()."""
    fs = fsspec.filesystem("file")
    listing = fiona.listdir("tests/data", opener=fs)
    assert len(listing) >= 33
    assert set(
        ["coutwildrnp.shp", "coutwildrnp.dbf", "coutwildrnp.shx", "coutwildrnp.prj"]
    ) & set(listing)


def test_opener_fsspec_file_remove(data):
    """Opener can remove data."""
    fs = fsspec.filesystem("file")
    listing = fiona.listdir(str(data), opener=fs)
    assert len(listing) == 4
    outputfile = os.path.join(str(data), "coutwildrnp.shp")
    fiona.remove(outputfile)
    listing = fiona.listdir(str(data), opener=fs)
    assert len(listing) == 0
    assert not set(
        ["coutwildrnp.shp", "coutwildrnp.dbf", "coutwildrnp.shx", "coutwildrnp.prj"]
    ) & set(listing)


def test_opener_tiledb_vfs_listdir():
    """Use tiledb VFS as opener for listdir()."""
    tiledb = pytest.importorskip("tiledb")
    fs = tiledb.VFS()
    listing = fiona.listdir("tests/data", opener=fs)
    assert len(listing) >= 33
    assert set(
        ["coutwildrnp.shp", "coutwildrnp.dbf", "coutwildrnp.shx", "coutwildrnp.prj"]
    ) & set(listing)


def test_opener_interface():
    """Demonstrate implementation of a custom opener."""
    import pathlib
    from fiona.abc import FileContainer

    class CustomContainer:
        """GDAL's VSI ReadDir() uses 5 of FileContainer's methods."""
        def isdir(self, path):
            return pathlib.Path(path).is_dir()

        def isfile(self, path):
            return pathlib.Path(path).is_file()

        def ls(self, path):
            return list(pathlib.Path(path).iterdir())

        def mtime(self, path):
            return pathlib.Path(path).stat().st_mtime

        def size(self, path):
            return pathlib.Path(path).stat().st_size

    FileContainer.register(CustomContainer)

    listing = fiona.listdir("tests/data", opener=CustomContainer())
    assert len(listing) >= 33
    assert set(
        ["coutwildrnp.shp", "coutwildrnp.dbf", "coutwildrnp.shx", "coutwildrnp.prj"]
    ) & set(listing)
