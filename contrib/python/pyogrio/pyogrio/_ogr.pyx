import os
import sys
import warnings

from pyogrio._err cimport check_pointer
from pyogrio._err import CPLE_BaseError, NullPointerError
from pyogrio.errors import DataSourceError

MULTI_EXTENSIONS = (".gpkg.zip", ".shp.zip")


cdef get_string(const char *c_str, str encoding="UTF-8"):
    """Get Python string from a char *.

    IMPORTANT: the char * must still be freed by the caller.

    Parameters
    ----------
    c_str : char *
    encoding : str, optional (default: UTF-8)

    Returns
    -------
    Python string
    """
    cdef bytes py_str

    py_str = c_str
    return py_str.decode(encoding)


def get_gdal_version():
    """Convert GDAL version number into tuple of (major, minor, revision)"""
    version = int(GDALVersionInfo("VERSION_NUM"))
    major = version // 1000000
    minor = (version - (major * 1000000)) // 10000
    revision = (version - (major * 1000000) - (minor * 10000)) // 100
    return (major, minor, revision)


def get_gdal_version_string():
    cdef const char* version = GDALVersionInfo("RELEASE_NAME")
    return get_string(version)


cdef extern from "ogr_api.h":
    bint OGRGetGEOSVersion(int *pnMajor, int *pnMinor, int *pnPatch)


def get_gdal_geos_version():
    cdef int major, minor, revision

    if not OGRGetGEOSVersion(&major, &minor, &revision):
        return None
    return (major, minor, revision)


def set_gdal_config_options(dict options):
    for name, value in options.items():
        name_b = name.encode("utf-8")
        name_c = name_b

        # None is a special case; this is used to clear the previous value
        if value is None:
            CPLSetConfigOption(<const char*>name_c, NULL)
            continue

        # normalize bool to ON/OFF
        if isinstance(value, bool):
            value_b = b"ON" if value else b"OFF"
        else:
            value_b = str(value).encode("utf-8")

        value_c = value_b
        CPLSetConfigOption(<const char*>name_c, <const char*>value_c)


def get_gdal_config_option(str name):
    name_b = name.encode("utf-8")
    name_c = name_b
    value = CPLGetConfigOption(<const char*>name_c, NULL)

    if not value:
        return None

    if value.isdigit():
        return int(value)

    if value == b"ON":
        return True
    if value == b"OFF":
        return False

    str_value = get_string(value)

    return str_value


def ogr_driver_supports_write(driver):
    # check metadata for driver to see if it supports write
    if _get_driver_metadata_item(driver, "DCAP_CREATE") == "YES":
        return True

    return False


def ogr_driver_supports_vsi(driver):
    # check metadata for driver to see if it supports write
    if _get_driver_metadata_item(driver, "DCAP_VIRTUALIO") == "YES":
        return True

    return False


def ogr_list_drivers():
    cdef OGRSFDriverH driver = NULL
    cdef int i
    cdef char *name_c

    drivers = dict()
    for i in range(OGRGetDriverCount()):
        driver = OGRGetDriver(i)
        name_c = <char *>OGR_Dr_GetName(driver)

        name = get_string(name_c)

        if ogr_driver_supports_write(name):
            drivers[name] = "rw"

        else:
            drivers[name] = "r"

    return drivers


cdef void set_proj_search_path(str path):
    """Set PROJ library data file search path for use in GDAL."""
    cdef char **paths = NULL
    cdef const char *path_c = NULL
    path_b = path.encode("utf-8")
    path_c = path_b
    paths = CSLAddString(paths, path_c)
    OSRSetPROJSearchPaths(<const char *const *>paths)


def has_gdal_data():
    """Verify that GDAL library data files are correctly found.

    Adapted from Fiona (_env.pyx).
    """

    if CPLFindFile("gdal", "header.dxf") != NULL:
        return True

    return False


def get_gdal_data_path():
    """
    Get the path to the directory GDAL uses to read data files.
    """
    cdef const char *path_c = CPLFindFile("gdal", "header.dxf")
    if path_c != NULL:
        return get_string(path_c).replace("header.dxf", "")
    return None


def has_proj_data():
    """Verify that PROJ library data files are correctly found.

    Returns
    -------
    bool
        True if a test spatial reference object could be created, which verifies
        that data files are correctly loaded.

    Adapted from Fiona (_env.pyx).
    """
    cdef OGRSpatialReferenceH srs = OSRNewSpatialReference(NULL)

    retval = OSRImportFromEPSG(srs, 4326)
    if srs != NULL:
        OSRRelease(srs)

    if retval == OGRERR_NONE:
        # Succesfull return, so PROJ data files are correctly found
        return True
    else:
        return False


def init_gdal_data():
    """Set GDAL data search directories.

    They are set in the following precedence:
    - wheel copy of gdal_data
    - default detection by GDAL, including GDAL_DATA (detected automatically by GDAL)
    - other well-known paths under sys.prefix

    Adapted from Fiona (env.py, _env.pyx).
    """

    # wheels are packaged to include GDAL data files at pyogrio/gdal_data
    wheel_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "gdal_data"))
    if os.path.exists(wheel_path):
        set_gdal_config_options({"GDAL_DATA": wheel_path})
        if not has_gdal_data():
            raise ValueError(
                "Could not correctly detect GDAL data files installed by pyogrio wheel"
            )
        return

    # GDAL correctly found data files from GDAL_DATA or compiled-in paths
    if has_gdal_data():
        return

    wk_path = os.path.join(sys.prefix, "share", "gdal")
    if os.path.exists(wk_path):
        set_gdal_config_options({"GDAL_DATA": wk_path})
        if not has_gdal_data():
            raise ValueError(
                f"Found GDAL data directory at {wk_path} but it does not appear to "
                "correctly contain GDAL data files"
            )
        return

    warnings.warn(
        "Could not detect GDAL data files. Set GDAL_DATA environment variable to the "
        "correct path.",
        RuntimeWarning
    )


def init_proj_data():
    """Set Proj search directories in the following precedence:
    - wheel copy of proj_data
    - default detection by PROJ, including PROJ_LIB (detected automatically by PROJ)
    - search other well-known paths under sys.prefix

    Adapted from Fiona (env.py, _env.pyx).
    """

    # wheels are packaged to include PROJ data files at pyogrio/proj_data
    wheel_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "proj_data"))
    if os.path.exists(wheel_path):
        set_proj_search_path(wheel_path)
        # verify that this now resolves
        if not has_proj_data():
            raise ValueError(
                "Could not correctly detect PROJ data files installed by pyogrio wheel"
            )
        return

    # PROJ correctly found data files from PROJ_LIB or compiled-in paths
    if has_proj_data():
        return

    wk_path = os.path.join(sys.prefix, "share", "proj")
    if os.path.exists(wk_path):
        set_proj_search_path(wk_path)
        # verify that this now resolves
        if not has_proj_data():
            raise ValueError(
                f"Found PROJ data directory at {wk_path} but it does not appear to "
                "correctly contain PROJ data files"
            )
        return

    warnings.warn(
            "Could not detect PROJ data files. Set PROJ_LIB environment variable to "
            "the correct path.", RuntimeWarning)


def _register_drivers():
    # Register all drivers
    GDALAllRegister()


def _get_driver_metadata_item(driver, metadata_item):
    """
    Query driver metadata items.

    Parameters
    ----------
    driver : str
        Driver to query
    metadata_item : str
        Metadata item to query

    Returns
    -------
    str or None
        Metadata item
    """
    cdef const char* metadata_c = NULL
    cdef void *cogr_driver = NULL

    try:
        cogr_driver = check_pointer(GDALGetDriverByName(driver.encode("UTF-8")))
    except NullPointerError:
        raise DataSourceError(
            f"Could not obtain driver: {driver} (check that it was installed "
            "correctly into GDAL)"
        )
    except CPLE_BaseError as exc:
        raise DataSourceError(str(exc))

    metadata_c = GDALGetMetadataItem(cogr_driver, metadata_item.encode("UTF-8"), NULL)

    metadata = None
    if metadata_c != NULL:
        metadata = metadata_c
        metadata = metadata.decode("UTF-8")
        if len(metadata) == 0:
            metadata = None

    return metadata


def _get_drivers_for_path(path):
    cdef OGRSFDriverH driver = NULL
    cdef int i
    cdef char *name_c

    path = str(path).lower()

    parts = os.path.splitext(path)
    if len(parts) == 2 and len(parts[1]) > 1:
        ext = parts[1][1:]
    else:
        ext = None

    # allow specific drivers to have a .zip extension to match GDAL behavior
    if ext == "zip":
        for multi_ext in MULTI_EXTENSIONS:
            if path.endswith(multi_ext):
                ext = multi_ext[1:]  # strip leading dot
                break

    drivers = []
    for i in range(OGRGetDriverCount()):
        driver = OGRGetDriver(i)
        name_c = <char *>OGR_Dr_GetName(driver)
        name = get_string(name_c)

        if not ogr_driver_supports_write(name):
            continue

        # extensions is a space-delimited list of supported extensions
        # for driver
        extensions = _get_driver_metadata_item(name, "DMD_EXTENSIONS")
        if (
            ext is not None
            and extensions is not None
            and ext in extensions.lower().split(" ")
        ):
            drivers.append(name)
        else:
            prefix = _get_driver_metadata_item(name, "DMD_CONNECTION_PREFIX")
            if prefix is not None and path.startswith(prefix.lower()):
                drivers.append(name)

    return drivers
