"""Tests of driver support"""

import logging

import pytest

from .conftest import requires_gdal24, get_temp_filename

from fiona.drvsupport import supported_drivers, driver_mode_mingdal
import fiona.drvsupport
from fiona.env import GDALVersion
from fiona._env import calc_gdal_version_num, get_gdal_version_num
from fiona.errors import DriverError

log = logging.getLogger()


@requires_gdal24
@pytest.mark.gdal
@pytest.mark.parametrize("format", ["GeoJSON", "ESRIJSON", "TopoJSON", "GeoJSONSeq"])
def test_geojsonseq(format):
    """Format is available"""
    assert format in fiona.drvsupport.supported_drivers.keys()


@pytest.mark.parametrize(
    "driver", [driver for driver, raw in supported_drivers.items() if "w" in raw]
)
@pytest.mark.gdal
def test_write_or_driver_error(tmpdir, driver, testdata_generator):
    """Test if write mode works."""
    if driver == "BNA" and GDALVersion.runtime() < GDALVersion(2, 0):
        pytest.skip("BNA driver segfaults with gdal 1.11")

    schema, crs, records1, _, _ = testdata_generator(driver, range(0, 10), [])
    path = str(tmpdir.join(get_temp_filename(driver)))

    if driver in driver_mode_mingdal[
        "w"
    ] and get_gdal_version_num() < calc_gdal_version_num(
        *driver_mode_mingdal["w"][driver]
    ):

        # Test if DriverError is raised for gdal < driver_mode_mingdal
        with pytest.raises(DriverError):
            with fiona.open(path, "w", driver=driver, crs=crs, schema=schema) as c:
                c.writerecords(records1)

    else:
        # Test if we can write
        with fiona.open(path, "w", driver=driver, crs=crs, schema=schema) as c:

            c.writerecords(records1)

        if driver in {"FileGDB", "OpenFileGDB"}:
            open_driver = driver
        else:
            open_driver = None

        with fiona.open(path, driver=open_driver) as collection:
            assert collection.driver == driver
            assert len(list(collection)) == len(records1)


# If this test fails, it should be considered to update
# driver_mode_mingdal in drvsupport.py.
@pytest.mark.parametrize(
    "driver", [driver for driver in driver_mode_mingdal["w"].keys()]
)
@pytest.mark.gdal
def test_write_does_not_work_when_gdal_smaller_mingdal(
    tmpdir, driver, testdata_generator, monkeypatch
):
    """Test if driver really can't write for gdal < driver_mode_mingdal."""
    if driver == "BNA" and GDALVersion.runtime() < GDALVersion(2, 0):
        pytest.skip("BNA driver segfaults with gdal 1.11")
    if driver == "FlatGeobuf" and calc_gdal_version_num(
        3, 1, 0
    ) <= get_gdal_version_num() < calc_gdal_version_num(3, 1, 3):
        pytest.skip("See https://github.com/Toblerity/Fiona/pull/924")

    schema, crs, records1, _, _ = testdata_generator(driver, range(0, 10), [])
    path = str(tmpdir.join(get_temp_filename(driver)))

    if driver in driver_mode_mingdal[
        "w"
    ] and get_gdal_version_num() < calc_gdal_version_num(
        *driver_mode_mingdal["w"][driver]
    ):
        monkeypatch.delitem(fiona.drvsupport.driver_mode_mingdal["w"], driver)

        with pytest.raises(Exception):
            with fiona.open(path, "w", driver=driver, crs=crs, schema=schema) as c:
                c.writerecords(records1)


# Some driver only allow a specific schema. These drivers can be
# excluded by adding them to blacklist_append_drivers.
@pytest.mark.parametrize(
    "driver", [driver for driver, raw in supported_drivers.items() if "a" in raw]
)
@pytest.mark.gdal
def test_append_or_driver_error(tmpdir, testdata_generator, driver):
    """Test if driver supports append mode."""
    if driver == "DGN":
        pytest.xfail("DGN schema has changed")

    if driver == "BNA" and GDALVersion.runtime() < GDALVersion(2, 0):
        pytest.skip("BNA driver segfaults with gdal 1.11")

    path = str(tmpdir.join(get_temp_filename(driver)))
    schema, crs, records1, records2, _ = testdata_generator(
        driver, range(0, 5), range(5, 10)
    )

    # If driver is not able to write, we cannot test append
    if driver in driver_mode_mingdal[
        "w"
    ] and get_gdal_version_num() < calc_gdal_version_num(
        *driver_mode_mingdal["w"][driver]
    ):
        return

    # Create test file to append to
    with fiona.open(path, "w", driver=driver, crs=crs, schema=schema) as c:

        c.writerecords(records1)

    if driver in driver_mode_mingdal[
        "a"
    ] and get_gdal_version_num() < calc_gdal_version_num(
        *driver_mode_mingdal["a"][driver]
    ):

        # Test if DriverError is raised for gdal < driver_mode_mingdal
        with pytest.raises(DriverError):
            with fiona.open(path, "a", driver=driver) as c:
                c.writerecords(records2)

    else:
        # Test if we can append
        with fiona.open(path, "a", driver=driver) as c:
            c.writerecords(records2)

        if driver in {"FileGDB", "OpenFileGDB"}:
            open_driver = driver
        else:
            open_driver = None

        with fiona.open(path, driver=open_driver) as collection:
            assert collection.driver == driver
            assert len(list(collection)) == len(records1) + len(records2)


# If this test fails, it should be considered to update
# driver_mode_mingdal in drvsupport.py.
@pytest.mark.parametrize(
    "driver",
    [
        driver
        for driver in driver_mode_mingdal["a"].keys()
        if driver in supported_drivers
    ],
)
@pytest.mark.gdal
def test_append_does_not_work_when_gdal_smaller_mingdal(
    tmpdir, driver, testdata_generator, monkeypatch
):
    """Test if driver supports append mode."""
    if driver == "BNA" and GDALVersion.runtime() < GDALVersion(2, 0):
        pytest.skip("BNA driver segfaults with gdal 1.11")

    if driver == "FlatGeobuf" and GDALVersion.runtime() < GDALVersion(3, 5):
        pytest.skip("FlatGeobuf segfaults with GDAL < 3.5.1")


    path = str(tmpdir.join(get_temp_filename(driver)))
    schema, crs, records1, records2, _ = testdata_generator(
        driver, range(0, 5), range(5, 10)
    )

    # If driver is not able to write, we cannot test append
    if driver in driver_mode_mingdal[
        "w"
    ] and get_gdal_version_num() < calc_gdal_version_num(
        *driver_mode_mingdal["w"][driver]
    ):
        return

    # Create test file to append to
    with fiona.open(path, "w", driver=driver, crs=crs, schema=schema) as c:

        c.writerecords(records1)

    if driver in driver_mode_mingdal[
        "a"
    ] and get_gdal_version_num() < calc_gdal_version_num(
        *driver_mode_mingdal["a"][driver]
    ):
        # Test if driver really can't append for gdal < driver_mode_mingdal
        monkeypatch.delitem(fiona.drvsupport.driver_mode_mingdal["a"], driver)

        with pytest.raises(Exception):
            with fiona.open(path, "a", driver=driver) as c:
                c.writerecords(records2)

            if driver in {"FileGDB", "OpenFileGDB"}:
                open_driver = driver
            else:
                open_driver = None

            with fiona.open(path, driver=open_driver) as collection:
                assert collection.driver == driver
                assert len(list(collection)) == len(records1) + len(records2)


# If this test fails, it should be considered to enable write support
# for the respective driver in drvsupport.py.
@pytest.mark.parametrize(
    "driver", [driver for driver, raw in supported_drivers.items() if raw == "r"]
)
@pytest.mark.gdal
def test_no_write_driver_cannot_write(tmpdir, driver, testdata_generator, monkeypatch):
    """Test if read only driver cannot write."""
    monkeypatch.setitem(fiona.drvsupport.supported_drivers, driver, "rw")
    schema, crs, records1, _, _ = testdata_generator(driver, range(0, 5), [])

    if driver == "BNA" and GDALVersion.runtime() < GDALVersion(2, 0):
        pytest.skip("BNA driver segfaults with gdal 1.11")

    if driver == "FlatGeobuf":
        pytest.xfail("FlatGeobuf doesn't raise an error but doesn't have write support")

    path = str(tmpdir.join(get_temp_filename(driver)))

    with pytest.raises(Exception):
        with fiona.open(path, "w", driver=driver, crs=crs, schema=schema) as c:
            c.writerecords(records1)


# If this test fails, it should be considered to enable append support
# for the respective driver in drvsupport.py.
@pytest.mark.parametrize(
    "driver",
    [
        driver
        for driver, raw in supported_drivers.items()
        if "w" in raw and "a" not in raw
    ],
)
@pytest.mark.gdal
def test_no_append_driver_cannot_append(
    tmpdir, driver, testdata_generator, monkeypatch
):
    """Test if a driver that supports write and not append cannot also append."""
    monkeypatch.setitem(fiona.drvsupport.supported_drivers, driver, "raw")

    if driver == "FlatGeobuf" and get_gdal_version_num() == calc_gdal_version_num(3, 5, 0):
        pytest.skip("FlatGeobuf driver segfaults with gdal 3.5.0")

    path = str(tmpdir.join(get_temp_filename(driver)))
    schema, crs, records1, records2, _ = testdata_generator(
        driver, range(0, 5), range(5, 10)
    )

    # If driver is not able to write, we cannot test append
    if driver in driver_mode_mingdal[
        "w"
    ] and get_gdal_version_num() < calc_gdal_version_num(
        *driver_mode_mingdal["w"][driver]
    ):
        return

    # Create test file to append to
    with fiona.open(path, "w", driver=driver, crs=crs, schema=schema) as c:
        c.writerecords(records1)

    try:
        with fiona.open(path, "a", driver=driver) as c:
            c.writerecords(records2)
    except Exception:
        log.exception("Caught exception in trying to append.")
        return

    if driver in {"FileGDB", "OpenFileGDB"}:
        open_driver = driver
    else:
        open_driver = None

    with fiona.open(path, driver=open_driver) as collection:
        assert collection.driver == driver
        assert len(list(collection)) == len(records1)


def test_mingdal_drivers_are_supported():
    """Test if mode and driver is enabled in supported_drivers"""
    for mode in driver_mode_mingdal:
        for driver in driver_mode_mingdal[mode]:
            # we cannot test drivers that are not present in the gdal installation
            if driver in supported_drivers:
                assert mode in supported_drivers[driver]


def test_allow_unsupported_drivers(monkeypatch, tmpdir):
    """Test if allow unsupported drivers works as expected"""

    # We delete a known working driver from fiona.drvsupport so that we can use it
    monkeypatch.delitem(fiona.drvsupport.supported_drivers, "GPKG")

    schema = {"geometry": "Polygon", "properties": {}}

    # Test that indeed we can't create a file without allow_unsupported_drivers
    path1 = str(tmpdir.join("test1.gpkg"))
    with pytest.raises(DriverError):
        with fiona.open(path1, mode="w", driver="GPKG", schema=schema):
            pass

    # Test that we can create file with allow_unsupported_drivers=True
    path2 = str(tmpdir.join("test2.gpkg"))
    try:
        with fiona.open(
            path2,
            mode="w",
            driver="GPKG",
            schema=schema,
            allow_unsupported_drivers=True,
        ):
            pass
    except Exception as e:
        assert (
            False
        ), f"Using allow_unsupported_drivers=True should not raise an exception: {e}"
