import fiona
import pytest
from fiona.drvsupport import driver_mode_mingdal, _driver_supports_mode
from fiona.errors import DriverError
from tests.conftest import get_temp_filename


@pytest.mark.parametrize(
    "driver",
    [
        driver
        for driver in driver_mode_mingdal["w"].keys()
        if _driver_supports_mode(driver, "w")
    ],
)
def test_write_getextent(tmpdir, driver, testdata_generator):
    """Test if a call to OGR_L_GetExtent has side effects for writing."""
    schema, crs, records1, records2, _ = testdata_generator(
        driver, range(0, 10), range(10, 20)
    )
    path = str(tmpdir.join(get_temp_filename(driver)))
    positions = {int(r['properties']['position']) for r in records1 + records2}

    with fiona.open(
        path,
        "w",
        driver=driver,
        crs=crs,
        schema=schema,
    ) as c:
        c.writerecords(records1)

        # Call to OGR_L_GetExtent
        try:
            c.bounds
        except DriverError:
            pass

        c.writerecords(records2)

    with fiona.open(path) as c:
        data = {int(f['properties']['position']) for f in c}
        assert len(positions) == len(data)
        for p in positions:
            assert p in data


@pytest.mark.parametrize(
    "driver",
    [
        driver
        for driver in driver_mode_mingdal["w"].keys()
        if _driver_supports_mode(driver, "w")
    ],
)
def test_read_getextent(tmpdir, driver, testdata_generator):
    """Test if a call to OGR_L_GetExtent has side effects for reading."""
    schema, crs, records1, records2, _ = testdata_generator(
        driver, range(0, 10), range(10, 20)
    )
    path = str(tmpdir.join(get_temp_filename(driver)))
    positions = {int(r['properties']['position']) for r in records1 + records2}

    with fiona.open(
        path,
        "w",
        driver=driver,
        crs=crs,
        schema=schema,
    ) as c:
        c.writerecords(records1)
        c.writerecords(records2)

    with fiona.open(path) as c:
        data = set()
        for _ in range(len(records1)):
            f = next(c)
            data.add(int(f['properties']['position']))

        # Call to OGR_L_GetExtent
        try:
            c.bounds
        except DriverError:
            pass

        for _ in range(len(records1)):
            f = next(c)
            data.add(int(f['properties']['position']))
        assert len(positions) == len(data)
        for p in positions:
            assert p in data


@pytest.mark.parametrize(
    "driver",
    [
        driver
        for driver in driver_mode_mingdal["w"].keys()
        if _driver_supports_mode(driver, "w")
    ],
)
def test_write_getfeaturecount(tmpdir, driver, testdata_generator):
    """Test if a call to OGR_L_GetFeatureCount has side effects for writing."""
    schema, crs, records1, records2, _ = testdata_generator(
        driver, range(0, 10), range(10, 20)
    )
    path = str(tmpdir.join(get_temp_filename(driver)))
    positions = {int(r['properties']['position']) for r in records1 + records2}

    with fiona.open(
        path,
        "w",
        driver=driver,
        crs=crs,
        schema=schema,
    ) as c:
        c.writerecords(records1)

        # Call to OGR_L_GetFeatureCount
        try:
            assert len(c) == len(records1)
        except TypeError:
            pass
        c.writerecords(records2)

    with fiona.open(path) as c:
        data = {int(f['properties']['position']) for f in c}
        assert len(positions) == len(data)
        for p in positions:
            assert p in data


@pytest.mark.parametrize(
    "driver",
    [
        driver
        for driver in driver_mode_mingdal["w"].keys()
        if _driver_supports_mode(driver, "w")
    ],
)
def test_read_getfeaturecount(tmpdir, driver, testdata_generator):
    """Test if a call to OGR_L_GetFeatureCount has side effects for reading."""
    schema, crs, records1, records2, _ = testdata_generator(
        driver, range(0, 10), range(10, 20)
    )
    path = str(tmpdir.join(get_temp_filename(driver)))
    positions = {int(r['properties']['position']) for r in records1 + records2}

    with fiona.open(
        path,
        "w",
        driver=driver,
        crs=crs,
        schema=schema,
    ) as c:
        c.writerecords(records1)
        c.writerecords(records2)

    with fiona.open(path) as c:
        data = set()
        for _ in range(len(records1)):
            f = next(c)
            data.add(int(f['properties']['position']))

        # Call to OGR_L_GetFeatureCount
        try:
            assert len(data) == len(records1)
        except TypeError:
            pass

        for _ in range(len(records1)):
            f = next(c)
            data.add(int(f['properties']['position']))

        try:
            assert len(data) == len(records1 + records2)
        except TypeError:
            pass

        assert len(positions) == len(data)
        for p in positions:
            assert p in data
