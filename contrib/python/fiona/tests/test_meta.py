import pytest
import fiona
import fiona.drvsupport
import fiona.meta
from fiona.drvsupport import supported_drivers
from fiona.errors import FionaValueError
from .conftest import requires_gdal2, requires_gdal23, requires_gdal31


@requires_gdal31
@pytest.mark.parametrize("driver", supported_drivers)
def test_print_driver_options(driver):
    """ Test fiona.meta.print_driver_options(driver) """
    # do not fail
    fiona.meta.print_driver_options(driver)


@requires_gdal2
def test_metadata_wrong_driver():
    """ Test that FionaValueError is raised for non existing driver"""
    with pytest.raises(FionaValueError):
        fiona.meta.print_driver_options("Not existing driver")


@requires_gdal2
@pytest.mark.parametrize("driver", supported_drivers)
def test_extension(driver):
    """ Test fiona.meta.extension(driver)  """
    # do not fail
    extension = fiona.meta.extension(driver)
    assert extension is None or isinstance(extension, str)


@requires_gdal2
@pytest.mark.parametrize("driver", supported_drivers)
def test_extensions(driver):
    """ Test fiona.meta.extensions(driver) """
    # do not fail
    extensions = fiona.meta.extensions(driver)
    assert extensions is None or isinstance(extensions, list)


@requires_gdal2
@pytest.mark.parametrize("driver", supported_drivers)
def test_supports_vsi(driver):
    """ Test fiona.meta.supports_vsi(driver) """
    # do not fail
    assert fiona.meta.supports_vsi(driver) in (True, False)


@requires_gdal2
@pytest.mark.parametrize("driver", supported_drivers)
def test_supported_field_types(driver):
    """ Test fiona.meta.supported_field_types(driver) """
    # do not fail
    field_types = fiona.meta.supported_field_types(driver)
    assert field_types is None or isinstance(field_types, list)


@requires_gdal23
@pytest.mark.parametrize("driver", supported_drivers)
def test_supported_sub_field_types(driver):
    """ Test fiona.meta.supported_sub_field_types(driver) """
    # do not fail
    sub_field_types = fiona.meta.supported_sub_field_types(driver)
    assert sub_field_types is None or isinstance(sub_field_types, list)
