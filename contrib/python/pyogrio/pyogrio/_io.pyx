#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

"""IO support for OGR vector data sources
"""

import contextlib
import datetime
import locale
import logging
import math
import os
import sys
import warnings
from pathlib import Path

from libc.stdint cimport uint8_t, uintptr_t
from libc.stdlib cimport malloc, free
from libc.string cimport strlen
from libc.math cimport isnan
from cpython.pycapsule cimport PyCapsule_GetPointer

cimport cython
from cpython.pycapsule cimport PyCapsule_New, PyCapsule_GetPointer

import numpy as np

from pyogrio._err cimport (
    check_last_error, check_int, check_pointer, ErrorHandler
)
from pyogrio._err import (
    CPLE_AppDefinedError,
    CPLE_BaseError,
    CPLE_NotSupportedError,
    CPLE_OpenFailedError,
    NullPointerError,
    capture_errors,
)
from pyogrio._geometry cimport get_geometry_type, get_geometry_type_code
from pyogrio._ogr cimport *
from pyogrio._ogr import MULTI_EXTENSIONS
from pyogrio._vsi cimport *
from pyogrio.errors import (
    CRSError, DataSourceError, DataLayerError, GeometryError, FieldError, FeatureError
)

log = logging.getLogger(__name__)


# Mapping of OGR integer field types to Python field type names
# (index in array is the integer field type)
FIELD_TYPES = [
    "int32",           # OFTInteger, Simple 32bit integer
    "list(int32)",     # OFTIntegerList, List of 32bit integers
    "float64",         # OFTReal, Double Precision floating point
    "list(float64)",   # OFTRealList, List of doubles
    "object",          # OFTString, String of UTF-8 chars
    "list(str)",       # OFTStringList, Array of strings
    None,              # OFTWideString, deprecated, not supported
    None,              # OFTWideStringList, deprecated, not supported
    "object",          # OFTBinary, Raw Binary data
    "datetime64[D]",   # OFTDate, Date
    None,              # OFTTime, Time, NOTE: not directly supported in numpy
    "datetime64[ms]",  # OFTDateTime, Date and Time
    "int64",           # OFTInteger64, Single 64bit integer
    "list(int64)"      # OFTInteger64List, List of 64bit integers, not supported
]

# Mapping of OGR integer field types to OGR type names
# (index in array is the integer field type)
FIELD_TYPE_NAMES = {
    OFTInteger: "OFTInteger",                # Simple 32bit integer
    OFTIntegerList: "OFTIntegerList",        # List of 32bit integers, not supported
    OFTReal: "OFTReal",                      # Double Precision floating point
    OFTRealList: "OFTRealList",              # List of doubles, not supported
    OFTString: "OFTString",                  # String of UTF-8 chars
    OFTStringList: "OFTStringList",          # Array of strings, not supported
    OFTWideString: "OFTWideString",          # deprecated, not supported
    OFTWideStringList: "OFTWideStringList",  # deprecated, not supported
    OFTBinary: "OFTBinary",                  # Raw Binary data
    OFTDate: "OFTDate",                      # Date
    OFTTime: "OFTTime",                      # Time: not directly supported in numpy
    OFTDateTime: "OFTDateTime",              # Date and Time
    OFTInteger64: "OFTInteger64",            # Single 64bit integer
    OFTInteger64List: "OFTInteger64List",    # List of 64bit integers, not supported
}

FIELD_SUBTYPES = {
    OFSTNone: None,           # No subtype
    OFSTBoolean: "bool",      # Boolean integer
    OFSTInt16: "int16",       # Signed 16-bit integer
    OFSTFloat32: "float32",   # Single precision (32 bit) floating point
}

FIELD_SUBTYPE_NAMES = {
    OFSTNone: "OFSTNone",             # No subtype
    OFSTBoolean: "OFSTBoolean",       # Boolean integer
    OFSTInt16: "OFSTInt16",           # Signed 16-bit integer
    OFSTFloat32: "OFSTFloat32",       # Single precision (32 bit) floating point
    OFSTJSON: "OFSTJSON",
    OFSTUUID: "OFSTUUID",
    OFSTMaxSubType: "OFSTMaxSubType",
}

# Mapping of numpy ndarray dtypes to (field type, subtype)
DTYPE_OGR_FIELD_TYPES = {
    "int8": (OFTInteger, OFSTInt16),
    "int16": (OFTInteger, OFSTInt16),
    "int32": (OFTInteger, OFSTNone),
    "int": (OFTInteger64, OFSTNone),
    "int64": (OFTInteger64, OFSTNone),
    # unsigned ints have to be converted to ints; these are converted
    # to the next largest integer size
    "uint8": (OFTInteger, OFSTInt16),
    "uint16": (OFTInteger, OFSTNone),
    "uint32": (OFTInteger64, OFSTNone),
    # TODO: these might get truncated, check maximum value and raise error
    "uint": (OFTInteger64, OFSTNone),
    "uint64": (OFTInteger64, OFSTNone),

    # bool is handled as integer with boolean subtype
    "bool": (OFTInteger, OFSTBoolean),

    "float32": (OFTReal, OFSTFloat32),
    "float": (OFTReal, OFSTNone),
    "float64": (OFTReal, OFSTNone),

    "datetime64[D]": (OFTDate, OFSTNone),
    "datetime64": (OFTDateTime, OFSTNone),
}


cdef int start_transaction(OGRDataSourceH ogr_dataset, int force) except 1:
    cdef int err = GDALDatasetStartTransaction(ogr_dataset, force)
    if err == OGRERR_FAILURE:
        raise DataSourceError("Failed to start transaction")

    return 0


cdef int commit_transaction(OGRDataSourceH ogr_dataset) except 1:
    cdef int err = GDALDatasetCommitTransaction(ogr_dataset)
    if err == OGRERR_FAILURE:
        raise DataSourceError("Failed to commit transaction")

    return 0


# Not currently used; uncomment when used
# cdef int rollback_transaction(OGRDataSourceH ogr_dataset) except 1:
#     cdef int err = GDALDatasetRollbackTransaction(ogr_dataset)
#     if err == OGRERR_FAILURE:
#         raise DataSourceError("Failed to rollback transaction")

#     return 0


cdef char** dict_to_options(object values):
    """Convert a python dictionary into name / value pairs (stored in a char**)

    Parameters
    ----------
    values: dict
        all keys and values must be strings

    Returns
    -------
    char**
    """
    cdef char **options = NULL

    if values is None:
        return NULL

    for k, v in values.items():
        k = k.encode("UTF-8")
        v = v.encode("UTF-8")
        options = CSLAddNameValue(options, <const char *>k, <const char *>v)

    return options


cdef const char* override_threadlocal_config_option(str key, str value):
    """Set the CPLSetThreadLocalConfigOption for key=value

    Parameters
    ----------
    key : str
    value : str

    Returns
    -------
    const char*
        value previously set for key, so that it can be later restored.  Caller
        is responsible for freeing this via CPLFree() if not NULL.
    """

    key_b = key.encode("UTF-8")
    cdef const char* key_c = key_b

    value_b = value.encode("UTF-8")
    cdef const char* value_c = value_b

    cdef const char *prev_value = CPLGetThreadLocalConfigOption(key_c, NULL)
    if prev_value != NULL:
        # strings returned from config options may be replaced via
        # CPLSetConfigOption() below; GDAL instructs us to save a copy
        # in a new string
        prev_value = CPLStrdup(prev_value)

    CPLSetThreadLocalConfigOption(key_c, value_c)

    return prev_value


cdef void* ogr_open(const char* path_c, int mode, char** options) except NULL:
    """Open an existing OGR data source

    Parameters
    ----------
    path_c : char *
        input path, including an in-memory path (/vsimem/...)
    mode : int
        set to 1 to allow updating data source
    options : char **, optional
        dataset open options
    """
    cdef void *ogr_dataset = NULL
    cdef ErrorHandler errors

    # Force linear approximations in all cases
    OGRSetNonLinearGeometriesEnabledFlag(0)

    flags = GDAL_OF_VECTOR | GDAL_OF_VERBOSE_ERROR
    if mode == 1:
        flags |= GDAL_OF_UPDATE
    else:
        flags |= GDAL_OF_READONLY

    try:
        # WARNING: GDAL logs warnings about invalid open options to stderr
        # instead of raising an error
        with capture_errors() as errors:
            ogr_dataset = GDALOpenEx(
                path_c, flags, NULL, <const char *const *>options, NULL
            )
            return errors.check_pointer(ogr_dataset, True)

    except NullPointerError:
        raise DataSourceError(
            f"Failed to open dataset ({mode=}): {path_c.decode('utf-8')}"
        ) from None

    except CPLE_BaseError as exc:
        if " a supported file format." in str(exc):
            # In gdal 3.9, this error message was slightly changed, so we can only check
            # on this part of the error message.
            raise DataSourceError(
                f"{str(exc)}; It might help to specify the correct driver explicitly "
                "by prefixing the file path with '<DRIVER>:', e.g. 'CSV:path'."
            ) from None

        raise DataSourceError(str(exc)) from None


cdef ogr_close(GDALDatasetH ogr_dataset):
    """Close the dataset and raise exception if that fails.
    NOTE: some drivers only raise errors on write when calling GDALClose()
    """
    if ogr_dataset != NULL:
        IF CTE_GDAL_VERSION >= 30700:
            if GDALClose(ogr_dataset) != CE_None:
                return check_last_error()

            return

        ELSE:
            GDALClose(ogr_dataset)

            # GDAL will set an error if there was an error writing the data source
            # on close
            return check_last_error()


cdef OGRLayerH get_ogr_layer(GDALDatasetH ogr_dataset, layer) except NULL:
    """Open OGR layer by index or name.

    Parameters
    ----------
    ogr_dataset : pointer to open OGR dataset
    layer : str or int
        name or index of layer

    Returns
    -------
    pointer to OGR layer
    """
    cdef OGRLayerH ogr_layer = NULL

    try:
        if isinstance(layer, str):
            name_b = layer.encode("utf-8")
            name_c = name_b
            ogr_layer = check_pointer(GDALDatasetGetLayerByName(ogr_dataset, name_c))

        elif isinstance(layer, int):
            ogr_layer = check_pointer(GDALDatasetGetLayer(ogr_dataset, layer))
        else:
            raise ValueError(
                f"'layer' parameter must be a str or int, got {type(layer)}"
            )

    # GDAL does not always raise exception messages in this case
    except NullPointerError:
        raise DataLayerError(f"Layer '{layer}' could not be opened") from None

    except CPLE_BaseError as exc:
        raise DataLayerError(str(exc))

    # if the driver is OSM, we need to execute SQL to set the layer to read in
    # order to read it properly
    if get_driver(ogr_dataset) == "OSM":
        # Note: this returns NULL and does not need to be freed via
        # GDALDatasetReleaseResultSet()
        layer_name = get_string(OGR_L_GetName(ogr_layer))
        sql_b = f"SET interest_layers = {layer_name}".encode("utf-8")
        sql_c = sql_b

        GDALDatasetExecuteSQL(ogr_dataset, sql_c, NULL, NULL)

    return ogr_layer


cdef OGRLayerH execute_sql(
    GDALDatasetH ogr_dataset, str sql, str sql_dialect=None
) except NULL:
    """Execute an SQL statement on a dataset.

    Parameters
    ----------
    ogr_dataset : pointer to open OGR dataset
    sql : str
        The sql statement to execute
    sql_dialect : str, optional (default: None)
        The sql dialect the sql statement is written in

    Returns
    -------
    pointer to OGR layer
    """

    try:
        sql_b = sql.encode("utf-8")
        sql_c = sql_b
        if sql_dialect is None:
            return check_pointer(GDALDatasetExecuteSQL(ogr_dataset, sql_c, NULL, NULL))

        sql_dialect_b = sql_dialect.encode("utf-8")
        sql_dialect_c = sql_dialect_b
        return check_pointer(GDALDatasetExecuteSQL(
            ogr_dataset, sql_c, NULL, sql_dialect_c)
        )

    # GDAL does not always raise exception messages in this case
    except NullPointerError:
        raise DataLayerError(f"Error executing sql '{sql}'") from None

    except CPLE_BaseError as exc:
        raise DataLayerError(str(exc))


cdef str get_crs(OGRLayerH ogr_layer):
    """Read CRS from layer as EPSG:<code> if available or WKT.

    Parameters
    ----------
    ogr_layer : pointer to open OGR layer

    Returns
    -------
    str or None
        EPSG:<code> or WKT
    """
    cdef void *ogr_crs = NULL
    cdef const char *authority_key = NULL
    cdef const char *authority_val = NULL
    cdef char *ogr_wkt = NULL

    try:
        ogr_crs = check_pointer(OGR_L_GetSpatialRef(ogr_layer))

    except NullPointerError:
        # No coordinate system defined.
        # This is expected and valid for nonspatial tables.
        return None

    except CPLE_BaseError as exc:
        raise CRSError(str(exc))

    # If CRS can be decoded to an EPSG code, use that.
    # The following pointers will be NULL if it cannot be decoded.
    retval = OSRAutoIdentifyEPSG(ogr_crs)
    authority_key = <const char *>OSRGetAuthorityName(ogr_crs, NULL)
    authority_val = <const char *>OSRGetAuthorityCode(ogr_crs, NULL)

    if authority_key != NULL and authority_val != NULL:
        key = get_string(authority_key)
        if key == "EPSG":
            value = get_string(authority_val)
            return f"EPSG:{value}"

    try:
        OSRExportToWkt(ogr_crs, &ogr_wkt)
        if ogr_wkt == NULL:
            raise CRSError("CRS could not be extracted as WKT") from None

        wkt = get_string(ogr_wkt)

    finally:
        CPLFree(ogr_wkt)
        return wkt


cdef get_driver(OGRDataSourceH ogr_dataset):
    """Get the driver for a dataset.

    Parameters
    ----------
    ogr_dataset : pointer to open OGR dataset
    Returns
    -------
    str or None
    """
    cdef void *ogr_driver

    try:
        ogr_driver = check_pointer(GDALGetDatasetDriver(ogr_dataset))

    except NullPointerError:
        raise DataLayerError(f"Could not detect driver of dataset") from None

    except CPLE_BaseError as exc:
        raise DataLayerError(str(exc))

    driver = OGR_Dr_GetName(ogr_driver).decode("UTF-8")
    return driver


cdef get_feature_count(OGRLayerH ogr_layer, int force):
    """Get the feature count of a layer.

    If GDAL returns an unknown count (-1), this iterates over every feature
    to calculate the count.

    Parameters
    ----------
    ogr_layer : pointer to open OGR layer
    force : bool
        True if the feature count should be computed even if it is expensive

    Returns
    -------
    int
        count of features
    """

    cdef OGRFeatureH ogr_feature = NULL
    cdef int feature_count = OGR_L_GetFeatureCount(ogr_layer, force)

    # if GDAL refuses to give us the feature count, we have to loop over all
    # features ourselves and get the count.  This can happen for some drivers
    # (e.g., OSM) or if a where clause is invalid but not rejected as error
    if force and feature_count == -1:
        # make sure layer is read from beginning
        OGR_L_ResetReading(ogr_layer)

        feature_count = 0
        while True:
            try:
                ogr_feature = check_pointer(OGR_L_GetNextFeature(ogr_layer))
                feature_count +=1

            except NullPointerError:
                # No more rows available, so stop reading
                break

            # driver may raise other errors, e.g., for OSM if node ids are not
            # increasing, the default config option OSM_USE_CUSTOM_INDEXING=YES
            # causes errors iterating over features
            except CPLE_BaseError as exc:
                # if an invalid where clause is used for a GPKG file, it is not
                # caught as an error until attempting to iterate over features;
                # catch it here
                if "failed to prepare SQL" in str(exc):
                    raise ValueError(f"Invalid SQL query: {str(exc)}") from None

                raise DataLayerError(
                    f"Could not iterate over features: {str(exc)}"
                ) from None

            finally:
                if ogr_feature != NULL:
                    OGR_F_Destroy(ogr_feature)
                    ogr_feature = NULL

    return feature_count


cdef get_total_bounds(OGRLayerH ogr_layer, int force):
    """Get the total bounds of a layer.

    Parameters
    ----------
    ogr_layer : pointer to open OGR layer
    force : bool
        True if the total bounds should be computed even if it is expensive

    Returns
    -------
    tuple of (xmin, ymin, xmax, ymax) or None
        The total bounds of the layer, or None if they could not be determined.
    """

    cdef OGREnvelope ogr_envelope

    if OGR_L_GetExtent(ogr_layer, &ogr_envelope, force) == OGRERR_NONE:
        bounds = (
           ogr_envelope.MinX, ogr_envelope.MinY, ogr_envelope.MaxX, ogr_envelope.MaxY
        )
    else:
        bounds = None

    return bounds


cdef set_metadata(GDALMajorObjectH obj, object metadata):
    """Set metadata on a dataset or layer

    Parameters
    ----------
    obj : pointer to dataset or layer
    metadata : dict, optional (default None)
        keys and values must be strings
    """

    cdef char **metadata_items = NULL
    cdef int err = 0

    metadata_items = dict_to_options(metadata)
    if metadata_items != NULL:
        # only default namepace is currently supported
        err = GDALSetMetadata(obj, metadata_items, NULL)

        CSLDestroy(metadata_items)
        metadata_items = NULL

    if err:
        raise RuntimeError("Could not set metadata") from None

cdef get_metadata(GDALMajorObjectH obj):
    """Get metadata for a dataset or layer

    Parameters
    ----------
    obj : pointer to dataset or layer

    Returns
    -------
    dict or None
        metadata as key, value pairs
    """
    # only default namespace is currently supported
    cdef char **metadata = GDALGetMetadata(obj, NULL)

    if metadata != NULL:
        return dict(
            metadata[i].decode("UTF-8").split("=", 1)
            for i in range(CSLCount(metadata))
        )

    return None


cdef detect_encoding(OGRDataSourceH ogr_dataset, OGRLayerH ogr_layer):
    """Attempt to detect the encoding to use to read/write string values.

    If the layer/dataset supports reading/writing data in UTF-8, returns UTF-8.
    If UTF-8 is not supported and ESRI Shapefile, returns ISO-8859-1
    Otherwise the system locale preferred encoding is returned.

    Parameters
    ----------
    ogr_dataset : pointer to open OGR dataset
    ogr_layer : pointer to open OGR layer

    Returns
    -------
    str or None
    """

    if OGR_L_TestCapability(ogr_layer, OLCStringsAsUTF8):
        # OGR_L_TestCapability returns True for OLCStringsAsUTF8 if GDAL hides encoding
        # complexities for this layer/driver type. In this case all string attribute
        # values have to be supplied in UTF-8 and values will be returned in UTF-8.
        # The encoding used to read/write under the hood depends on the driver used.
        # For layers/drivers where False is returned, the string values are written and
        # read without recoding. Hence, it is up to you to supply the data in the
        # appropriate encoding. More info:
        # https://gdal.org/development/rfc/rfc23_ogr_unicode.html#oftstring-oftstringlist-fields
        # NOTE: for shapefiles, this always returns False for the layer returned
        # when executing SQL, even when it supports UTF-8 (patched below);
        # this may be fixed by https://github.com/OSGeo/gdal/pull/9649 (GDAL >=3.9.0?)
        return "UTF-8"

    driver = get_driver(ogr_dataset)
    if driver == "ESRI Shapefile":
        # OGR_L_TestCapability returns True for OLCStringsAsUTF8 (above) for
        # shapefiles when a .cpg file is present with a valid encoding, or GDAL
        # auto-detects the encoding from the code page of the .dbf file, or
        # SHAPE_ENCODING config option is set, or ENCODING layer creation option
        # is specified (shapefiles only).  Otherwise, we can only assume that
        # shapefiles are in their default encoding of ISO-8859-1 (which may be
        # incorrect and must be overridden by user-provided encoding)

        # Always use the first layer to test capabilities until detection for
        # SQL results from shapefiles are fixed (above)
        # This block should only be used for unfixed versions of GDAL (<3.9.0?)
        if OGR_L_TestCapability(GDALDatasetGetLayer(ogr_dataset, 0), OLCStringsAsUTF8):
            return "UTF-8"

        return "ISO-8859-1"

    if driver == "OSM":
        # always set OSM data to UTF-8
        # per https://help.openstreetmap.org/questions/2172/what-encoding-does-openstreetmap-use  # noqa: E501
        return "UTF-8"

    if driver in ("XLSX", "ODS"):
        # TestCapability for OLCStringsAsUTF8 for XLSX and ODS was False for new files
        # being created for GDAL < 3.8.5. Once these versions of GDAL are no longer
        # supported, this can be removed.
        return "UTF-8"

    if driver == "GeoJSONSeq":
        # In old gdal versions, OLCStringsAsUTF8 wasn't advertised yet.
        return "UTF-8"

    if driver == "SQLite":
        # TestCapability for OLCStringsAsUTF8 returns False for SQLite in GDAL 3.11.3.
        # Issue opened: https://github.com/OSGeo/gdal/issues/12962
        return "UTF-8"

    return locale.getpreferredencoding()


cdef get_fields(OGRLayerH ogr_layer, str encoding, use_arrow=False):
    """Get field names and types for layer.

    Parameters
    ----------
    ogr_layer : pointer to open OGR layer
    encoding : str
        encoding to use when reading field name
    use_arrow : bool, default False
        If using arrow, all types are supported, and we don't have to
        raise warnings

    Returns
    -------
    ndarray(n, 5)
        array of index, ogr type, name, numpy type, ogr subtype
    """
    cdef int i
    cdef int field_count
    cdef OGRFeatureDefnH ogr_featuredef = NULL
    cdef OGRFieldDefnH ogr_fielddef = NULL
    cdef int field_subtype
    cdef const char *key_c

    try:
        ogr_featuredef = check_pointer(OGR_L_GetLayerDefn(ogr_layer))

    except NullPointerError:
        raise DataLayerError("Could not get layer definition") from None

    except CPLE_BaseError as exc:
        raise DataLayerError(str(exc))

    field_count = OGR_FD_GetFieldCount(ogr_featuredef)

    fields = np.empty(shape=(field_count, 5), dtype=object)
    fields_view = fields[:, :]

    skipped_fields = False

    for i in range(field_count):
        try:
            ogr_fielddef = check_pointer(OGR_FD_GetFieldDefn(ogr_featuredef, i))

        except NullPointerError:
            raise FieldError(
                f"Could not get field definition for field at index {i}"
            ) from None

        except CPLE_BaseError as exc:
            raise FieldError(str(exc))

        field_name = get_string(OGR_Fld_GetNameRef(ogr_fielddef), encoding=encoding)

        field_type = OGR_Fld_GetType(ogr_fielddef)
        np_type = FIELD_TYPES[field_type]
        if not np_type and not use_arrow:
            skipped_fields = True
            log.warning(
                f"Skipping field {field_name}: unsupported OGR type: {field_type}")
            continue

        field_subtype = OGR_Fld_GetSubType(ogr_fielddef)
        subtype = FIELD_SUBTYPES.get(field_subtype)
        if subtype is not None:
            # bool, int16, float32 dtypes
            np_type = subtype

        fields_view[i, 0] = i
        fields_view[i, 1] = field_type
        fields_view[i, 2] = field_name
        fields_view[i, 3] = np_type
        fields_view[i, 4] = field_subtype

    if skipped_fields:
        # filter out skipped fields
        mask = np.array([idx is not None for idx in fields[:, 0]])
        fields = fields[mask]

    return fields


cdef apply_where_filter(OGRLayerH ogr_layer, str where):
    """Applies where filter to layer.

    WARNING: GDAL does not raise an error for GPKG when SQL query is invalid
    but instead only logs to stderr.

    Parameters
    ----------
    ogr_layer : pointer to open OGR layer
    where : str
        See http://ogdi.sourceforge.net/prop/6.2.CapabilitiesMetadata.html
        restricted_where for more information about valid expressions.

    Raises
    ------
    ValueError: if SQL query is not valid
    """

    where_b = where.encode("utf-8")
    where_c = where_b
    err = OGR_L_SetAttributeFilter(ogr_layer, where_c)
    # WARNING: GDAL does not raise this error for GPKG but instead only
    # logs to stderr
    if err != OGRERR_NONE:
        try:
            check_last_error()
        except CPLE_BaseError as exc:
            raise ValueError(str(exc))

        raise ValueError(
            f"Invalid SQL query for layer '{OGR_L_GetName(ogr_layer)}': '{where}'"
        )


cdef apply_bbox_filter(OGRLayerH ogr_layer, bbox):
    """Applies bounding box spatial filter to layer.

    Parameters
    ----------
    ogr_layer : pointer to open OGR layer
    bbox : list or tuple of xmin, ymin, xmax, ymax

    Raises
    ------
    ValueError: if bbox is not a list or tuple or does not have proper number of
        items
    """

    if not (isinstance(bbox, (tuple, list)) and len(bbox) == 4):
        raise ValueError(f"Invalid bbox: {bbox}")

    xmin, ymin, xmax, ymax = bbox
    OGR_L_SetSpatialFilterRect(ogr_layer, xmin, ymin, xmax, ymax)


cdef apply_geometry_filter(OGRLayerH ogr_layer, wkb):
    """Applies geometry spatial filter to layer.

    Parameters
    ----------
    ogr_layer : pointer to open OGR layer
    wkb : WKB encoding of geometry
    """

    cdef OGRGeometryH ogr_geometry = NULL
    cdef unsigned char *wkb_buffer = wkb

    err = OGR_G_CreateFromWkb(wkb_buffer, NULL, &ogr_geometry, len(wkb))
    if err:
        if ogr_geometry != NULL:
            OGR_G_DestroyGeometry(ogr_geometry)
        raise GeometryError("Could not create mask geometry") from None

    OGR_L_SetSpatialFilter(ogr_layer, ogr_geometry)
    OGR_G_DestroyGeometry(ogr_geometry)


cdef apply_skip_features(OGRLayerH ogr_layer, int skip_features):
    """Applies skip_features to layer.

    Parameters
    ----------
    ogr_layer : pointer to open OGR layer
    wskip_features : int
    """
    err = OGR_L_SetNextByIndex(ogr_layer, skip_features)
    # GDAL can raise an error (depending on the format) for out-of-bound index,
    # but `validate_feature_range()` should ensure we only pass a valid number
    if err != OGRERR_NONE:
        try:
            check_last_error()
        except CPLE_BaseError as exc:
            raise ValueError(str(exc))

        raise ValueError(f"Applying {skip_features=} raised an error")


cdef validate_feature_range(
    OGRLayerH ogr_layer, int skip_features=0, int max_features=0
):
    """Limit skip_features and max_features to bounds available for dataset.

    This is typically performed after applying where and spatial filters, which
    reduce the available range of features.

    Parameters
    ----------
    ogr_layer : pointer to open OGR layer
    skip_features : number of features to skip from beginning of available range
    max_features : maximum number of features to read from available range
    """

    feature_count = get_feature_count(ogr_layer, 1)
    num_features = max_features

    if feature_count == 0:
        return 0, 0

    if skip_features >= feature_count:
        return 0, 0

    if max_features == 0:
        num_features = feature_count - skip_features

    elif max_features > feature_count:
        num_features = feature_count

    return skip_features, num_features


@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
cdef process_geometry(OGRFeatureH ogr_feature, int i, geom_view, uint8_t force_2d):

    cdef OGRGeometryH ogr_geometry = NULL
    cdef OGRwkbGeometryType ogr_geometry_type

    cdef unsigned char *wkb = NULL
    cdef int ret_length

    ogr_geometry = OGR_F_GetGeometryRef(ogr_feature)

    if ogr_geometry == NULL:
        geom_view[i] = None
    else:
        try:
            ogr_geometry_type = OGR_G_GetGeometryType(ogr_geometry)

            # if geometry has M values, these need to be removed first
            if (OGR_G_IsMeasured(ogr_geometry)):
                OGR_G_SetMeasured(ogr_geometry, 0)

            if force_2d and OGR_G_Is3D(ogr_geometry):
                OGR_G_Set3D(ogr_geometry, 0)

            # if non-linear (e.g., curve), force to linear type
            if OGR_GT_IsNonLinear(ogr_geometry_type):
                ogr_geometry = OGR_G_GetLinearGeometry(ogr_geometry, 0, NULL)

            ret_length = OGR_G_WkbSize(ogr_geometry)
            wkb = <unsigned char*>malloc(sizeof(unsigned char)*ret_length)
            OGR_G_ExportToWkb(ogr_geometry, 1, wkb)
            geom_view[i] = wkb[:ret_length]

        finally:
            free(wkb)


@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
cdef process_fields(
    OGRFeatureH ogr_feature,
    int i,
    int n_fields,
    object field_data,
    object field_data_view,
    object field_indexes,
    object field_ogr_types,
    encoding,
    bint datetime_as_string
):
    cdef int j
    cdef int success
    cdef int field_index
    cdef int ret_length
    cdef int *ints_c
    cdef GIntBig *int64s_c
    cdef double *doubles_c
    cdef char **strings_c
    cdef GByte *bin_value
    cdef int year = 0
    cdef int month = 0
    cdef int day = 0
    cdef int hour = 0
    cdef int minute = 0
    cdef float fsecond = 0.0
    cdef int timezone = 0

    for j in range(n_fields):
        field_index = field_indexes[j]
        field_type = field_ogr_types[j]
        data = field_data_view[j]

        isnull = OGR_F_IsFieldSetAndNotNull(ogr_feature, field_index) == 0
        if isnull:
            if field_type in (OFTInteger, OFTInteger64, OFTReal):
                # if a boolean or integer type, have to cast to float to hold
                # NaN values
                if data.dtype.kind in ("b", "i", "u"):
                    field_data[j] = field_data[j].astype(np.float64)
                    field_data_view[j] = field_data[j][:]
                    field_data_view[j][i] = np.nan
                else:
                    data[i] = np.nan

            elif field_type in (OFTDate, OFTDateTime) and not datetime_as_string:
                data[i] = np.datetime64("NaT")

            else:
                data[i] = None

            continue

        if field_type == OFTInteger:
            data[i] = OGR_F_GetFieldAsInteger(ogr_feature, field_index)

        elif field_type == OFTInteger64:
            data[i] = OGR_F_GetFieldAsInteger64(ogr_feature, field_index)

        elif field_type == OFTReal:
            data[i] = OGR_F_GetFieldAsDouble(ogr_feature, field_index)

        elif field_type == OFTString:
            value = get_string(OGR_F_GetFieldAsString(
                ogr_feature, field_index), encoding=encoding
            )
            data[i] = value

        elif field_type == OFTBinary:
            bin_value = OGR_F_GetFieldAsBinary(ogr_feature, field_index, &ret_length)
            data[i] = bin_value[:ret_length]

        elif field_type == OFTDateTime or field_type == OFTDate:

            if field_type == OFTDateTime and datetime_as_string:
                # defer datetime parsing to user/ pandas layer
                IF CTE_GDAL_VERSION >= 30700:
                    data[i] = get_string(
                        OGR_F_GetFieldAsISO8601DateTime(ogr_feature, field_index, NULL),
                        encoding=encoding,
                    )
                ELSE:
                    data[i] = get_string(
                        OGR_F_GetFieldAsString(ogr_feature, field_index),
                        encoding=encoding,
                    )
            else:
                success = OGR_F_GetFieldAsDateTimeEx(
                    ogr_feature,
                    field_index,
                    &year,
                    &month,
                    &day,
                    &hour,
                    &minute,
                    &fsecond,
                    &timezone,
                )

                ms, ss = math.modf(fsecond)
                second = int(ss)
                # fsecond has millisecond accuracy
                microsecond = round(ms * 1000) * 1000

                if not success:
                    data[i] = np.datetime64("NaT")

                elif field_type == OFTDate:
                    data[i] = datetime.date(year, month, day).isoformat()

                elif field_type == OFTDateTime:
                    data[i] = datetime.datetime(
                        year, month, day, hour, minute, second, microsecond
                    ).isoformat()

        elif field_type == OFTIntegerList:
            # According to GDAL doc, this can return NULL for an empty list, which is a
            # valid result. So don't use check_pointer as it would throw an exception.
            ints_c = OGR_F_GetFieldAsIntegerList(ogr_feature, field_index, &ret_length)
            int_arr = np.ndarray(shape=(ret_length,), dtype=np.int32)
            for j in range(ret_length):
                int_arr[j] = ints_c[j]
            data[i] = int_arr

        elif field_type == OFTInteger64List:
            # According to GDAL doc, this can return NULL for an empty list, which is a
            # valid result. So don't use check_pointer as it would throw an exception.
            int64s_c = OGR_F_GetFieldAsInteger64List(
                ogr_feature, field_index, &ret_length
            )

            int_arr = np.ndarray(shape=(ret_length,), dtype=np.int64)
            for j in range(ret_length):
                int_arr[j] = int64s_c[j]
            data[i] = int_arr

        elif field_type == OFTRealList:
            # According to GDAL doc, this can return NULL for an empty list, which is a
            # valid result. So don't use check_pointer as it would throw an exception.
            doubles_c = OGR_F_GetFieldAsDoubleList(
                ogr_feature, field_index, &ret_length
            )

            double_arr = np.ndarray(shape=(ret_length,), dtype=np.float64)
            for j in range(ret_length):
                double_arr[j] = doubles_c[j]
            data[i] = double_arr

        elif field_type == OFTStringList:
            # According to GDAL doc, this can return NULL for an empty list, which is a
            # valid result. So don't use check_pointer as it would throw an exception.
            strings_c = OGR_F_GetFieldAsStringList(ogr_feature, field_index)

            string_list_index = 0
            vals = []
            if strings_c != NULL:
                # According to GDAL doc, the list is terminated by a NULL pointer.
                while strings_c[string_list_index] != NULL:
                    val = strings_c[string_list_index]
                    vals.append(get_string(val, encoding=encoding))
                    string_list_index += 1

            data[i] = np.array(vals)


@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
cdef get_features(
    OGRLayerH ogr_layer,
    object[:, :] fields,
    encoding,
    uint8_t read_geometry,
    uint8_t force_2d,
    int skip_features,
    int num_features,
    uint8_t return_fids,
    bint datetime_as_string
):

    cdef OGRFeatureH ogr_feature = NULL
    cdef int n_fields
    cdef int i
    cdef int field_index

    # make sure layer is read from beginning
    OGR_L_ResetReading(ogr_layer)

    if skip_features > 0:
        apply_skip_features(ogr_layer, skip_features)

    if return_fids:
        fid_data = np.empty(shape=(num_features), dtype=np.int64)
        fid_view = fid_data[:]
    else:
        fid_data = None

    if read_geometry:
        geometries = np.empty(shape=(num_features, ), dtype="object")
        geom_view = geometries[:]

    else:
        geometries = None

    n_fields = fields.shape[0]
    field_indexes = fields[:, 0]
    field_ogr_types = fields[:, 1]

    field_data = []
    for field_index in range(n_fields):
        if datetime_as_string and fields[field_index, 3].startswith("datetime"):
            dtype = "object"
        elif fields[field_index, 3].startswith("list"):
            dtype = "object"
        else:
            dtype = fields[field_index, 3]

        field_data.append(np.empty(shape=(num_features, ), dtype=dtype))

    field_data_view = [field_data[field_index][:] for field_index in range(n_fields)]

    if num_features == 0:
        return fid_data, geometries, field_data

    i = 0
    while True:
        try:
            if num_features > 0 and i == num_features:
                break

            try:
                ogr_feature = check_pointer(OGR_L_GetNextFeature(ogr_layer))

            except NullPointerError:
                # No more rows available, so stop reading
                break

            except CPLE_BaseError as exc:
                raise FeatureError(str(exc))

            if i >= num_features:
                raise FeatureError(
                    "GDAL returned more records than expected based on the count of "
                    "records that may meet your combination of filters against this "
                    "dataset.  Please open an issue on Github "
                    "(https://github.com/geopandas/pyogrio/issues) to report "
                    "encountering this error."
                ) from None

            if return_fids:
                fid_view[i] = OGR_F_GetFID(ogr_feature)

            if read_geometry:
                process_geometry(ogr_feature, i, geom_view, force_2d)

            process_fields(
                ogr_feature, i, n_fields, field_data, field_data_view,
                field_indexes, field_ogr_types, encoding, datetime_as_string
            )
            i += 1
        finally:
            if ogr_feature != NULL:
                OGR_F_Destroy(ogr_feature)
                ogr_feature = NULL

    # There may be fewer rows available than expected from OGR_L_GetFeatureCount,
    # such as features with bounding boxes that intersect the bbox
    # but do not themselves intersect the bbox.
    # Empty rows are dropped.
    if i < num_features:
        if return_fids:
            fid_data = fid_data[:i]
        if read_geometry:
            geometries = geometries[:i]
        field_data = [data_field[:i] for data_field in field_data]

    return fid_data, geometries, field_data


@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
cdef get_features_by_fid(
    OGRLayerH ogr_layer,
    int[:] fids,
    object[:, :] fields,
    encoding,
    uint8_t read_geometry,
    uint8_t force_2d,
    bint datetime_as_string
):

    cdef OGRFeatureH ogr_feature = NULL
    cdef int n_fields
    cdef int i
    cdef int fid
    cdef int field_index
    cdef int count = len(fids)

    # make sure layer is read from beginning
    OGR_L_ResetReading(ogr_layer)

    if read_geometry:
        geometries = np.empty(shape=(count, ), dtype="object")
        geom_view = geometries[:]

    else:
        geometries = None

    n_fields = fields.shape[0]
    field_indexes = fields[:, 0]
    field_ogr_types = fields[:, 1]
    field_data = [
        np.empty(
            shape=(count, ),
            dtype=(
                "object"
                if datetime_as_string and fields[field_index, 3].startswith("datetime")
                else fields[field_index, 3]
            )
        ) for field_index in range(n_fields)
    ]

    field_data_view = [field_data[field_index][:] for field_index in range(n_fields)]

    for i in range(count):
        try:
            fid = fids[i]

            try:
                ogr_feature = check_pointer(OGR_L_GetFeature(ogr_layer, fid))

            except NullPointerError:
                raise FeatureError(f"Could not read feature with fid {fid}") from None

            except CPLE_BaseError as exc:
                raise FeatureError(str(exc))

            if read_geometry:
                process_geometry(ogr_feature, i, geom_view, force_2d)

            process_fields(
                ogr_feature, i, n_fields, field_data, field_data_view,
                field_indexes, field_ogr_types, encoding, datetime_as_string
            )
        finally:
            if ogr_feature != NULL:
                OGR_F_Destroy(ogr_feature)
                ogr_feature = NULL

    return (geometries, field_data)


@cython.boundscheck(False)  # Deactivate bounds checking
@cython.wraparound(False)   # Deactivate negative indexing.
cdef get_bounds(OGRLayerH ogr_layer, int skip_features, int num_features):
    cdef OGRFeatureH ogr_feature = NULL
    cdef OGRGeometryH ogr_geometry = NULL
    cdef OGREnvelope ogr_envelope  # = NULL
    cdef int i

    # make sure layer is read from beginning
    OGR_L_ResetReading(ogr_layer)

    if skip_features > 0:
        apply_skip_features(ogr_layer, skip_features)

    fid_data = np.empty(shape=(num_features), dtype=np.int64)
    fid_view = fid_data[:]

    bounds_data = np.empty(shape=(4, num_features), dtype="float64")
    bounds_view = bounds_data[:]

    i = 0
    while True:
        try:
            if num_features > 0 and i == num_features:
                break

            try:
                ogr_feature = check_pointer(OGR_L_GetNextFeature(ogr_layer))

            except NullPointerError:
                # No more rows available, so stop reading
                break

            except CPLE_BaseError as exc:
                raise FeatureError(str(exc))

            if i >= num_features:
                raise FeatureError(
                    "Reading more features than indicated by OGR_L_GetFeatureCount is "
                    "not supported"
                ) from None

            fid_view[i] = OGR_F_GetFID(ogr_feature)

            ogr_geometry = OGR_F_GetGeometryRef(ogr_feature)

            if ogr_geometry == NULL:
                bounds_view[:, i] = np.nan

            else:
                OGR_G_GetEnvelope(ogr_geometry, &ogr_envelope)
                bounds_view[0, i] = ogr_envelope.MinX
                bounds_view[1, i] = ogr_envelope.MinY
                bounds_view[2, i] = ogr_envelope.MaxX
                bounds_view[3, i] = ogr_envelope.MaxY

            i += 1
        finally:
            if ogr_feature != NULL:
                OGR_F_Destroy(ogr_feature)
                ogr_feature = NULL

    # Less rows read than anticipated, so drop empty rows
    if i < num_features:
        fid_data = fid_data[:i]
        bounds_data = bounds_data[:, :i]

    return fid_data, bounds_data


def ogr_read(
    object path_or_buffer,
    object dataset_kwargs,
    object layer=None,
    object encoding=None,
    int read_geometry=True,
    int force_2d=False,
    object columns=None,
    int skip_features=0,
    int max_features=0,
    object where=None,
    tuple bbox=None,
    object mask=None,
    object fids=None,
    str sql=None,
    str sql_dialect=None,
    int return_fids=False,
    bint datetime_as_string=False,
):

    cdef int err = 0
    cdef bint use_tmp_vsimem = isinstance(path_or_buffer, bytes)
    cdef const char *path_c = NULL
    cdef char **dataset_options = NULL
    cdef const char *where_c = NULL
    cdef const char *field_c = NULL
    cdef char **fields_c = NULL
    cdef OGRDataSourceH ogr_dataset = NULL
    cdef OGRLayerH ogr_layer = NULL
    cdef int feature_count = 0
    cdef double xmin, ymin, xmax, ymax
    cdef const char *prev_shape_encoding = NULL
    cdef bint override_shape_encoding = False

    if fids is not None:
        if (
            where is not None
            or bbox is not None
            or mask is not None
            or sql is not None
            or skip_features
            or max_features
        ):
            raise ValueError(
                "cannot set both 'fids' and any of 'where', 'bbox', 'mask', "
                "'sql', 'skip_features' or 'max_features'"
            )
        fids = np.asarray(fids, dtype=np.intc)

    if sql is not None and layer is not None:
        raise ValueError("'sql' parameter cannot be combined with 'layer'")

    if not (read_geometry or return_fids or columns is None or len(columns) > 0):
        raise ValueError(
            "at least one of read_geometry or return_fids must be True or columns must "
            "be None or non-empty"
        )

    if bbox and mask:
        raise ValueError("cannot set both 'bbox' and 'mask'")

    if skip_features < 0:
        raise ValueError("'skip_features' must be >= 0")

    if max_features < 0:
        raise ValueError("'max_features' must be >= 0")

    try:
        path = read_buffer_to_vsimem(
            path_or_buffer
        ) if use_tmp_vsimem else path_or_buffer

        if encoding:
            # for shapefiles, SHAPE_ENCODING must be set before opening the file
            # to prevent automatic decoding to UTF-8 by GDAL, so we save previous
            # SHAPE_ENCODING so that it can be restored later
            # (we do this for all data sources where encoding is set because
            # we don't know the driver until after it is opened, which is too late)
            override_shape_encoding = True
            prev_shape_encoding = override_threadlocal_config_option(
                "SHAPE_ENCODING", encoding
            )

        dataset_options = dict_to_options(dataset_kwargs)
        ogr_dataset = ogr_open(path.encode("UTF-8"), 0, dataset_options)

        if sql is None:
            if layer is None:
                layer = get_default_layer(ogr_dataset)
            ogr_layer = get_ogr_layer(ogr_dataset, layer)
        else:
            ogr_layer = execute_sql(ogr_dataset, sql, sql_dialect)

        crs = get_crs(ogr_layer)

        # Encoding is derived from the user, from the dataset capabilities / type,
        # or from the system locale
        if encoding:
            if get_driver(ogr_dataset) == "ESRI Shapefile":
                # NOTE: SHAPE_ENCODING is a configuration option whereas ENCODING is the
                # dataset open option
                if "ENCODING" in dataset_kwargs:
                    raise ValueError(
                        'cannot provide both encoding parameter and "ENCODING" option; '
                        "use encoding parameter to specify correct encoding for data "
                        "source"
                    )

                # Because SHAPE_ENCODING is set above, GDAL will automatically
                # decode shapefiles to UTF-8; ignore any encoding set by user
                encoding = "UTF-8"

        else:
            encoding = detect_encoding(ogr_dataset, ogr_layer)

        fields = get_fields(ogr_layer, encoding)

        ignored_fields = []
        if columns is not None:
            # identify ignored fields first
            ignored_fields = list(set(fields[:, 2]) - set(columns))

            # Fields are matched exactly by name, duplicates are dropped.
            # Find index of each field into fields
            idx = np.sort(np.intersect1d(fields[:, 2], columns, return_indices=True)[1])
            fields = fields[idx, :]

        if not read_geometry and bbox is None and mask is None:
            ignored_fields.append("OGR_GEOMETRY")

        # Instruct GDAL to ignore reading fields not
        # included in output columns for faster I/O
        if ignored_fields:
            for field in ignored_fields:
                field_b = field.encode("utf-8")
                field_c = field_b
                fields_c = CSLAddString(fields_c, field_c)

            OGR_L_SetIgnoredFields(ogr_layer, <const char**>fields_c)

        geometry_type = get_geometry_type(ogr_layer)

        if fids is not None:
            geometries, field_data = get_features_by_fid(
                ogr_layer,
                fids,
                fields,
                encoding,
                read_geometry=read_geometry and geometry_type is not None,
                force_2d=force_2d,
                datetime_as_string=datetime_as_string,
            )

            # bypass reading fids since these should match fids used for read
            if return_fids:
                fid_data = fids.astype(np.int64)
            else:
                fid_data = None
        else:
            # Apply the attribute filter
            if where is not None and where != "":
                apply_where_filter(ogr_layer, where)

            # Apply the spatial filter
            if bbox is not None:
                apply_bbox_filter(ogr_layer, bbox)

            elif mask is not None:
                apply_geometry_filter(ogr_layer, mask)

            # Limit feature range to available range
            skip_features, num_features = validate_feature_range(
                ogr_layer, skip_features, max_features
            )

            fid_data, geometries, field_data = get_features(
                ogr_layer,
                fields,
                encoding,
                read_geometry=read_geometry and geometry_type is not None,
                force_2d=force_2d,
                skip_features=skip_features,
                num_features=num_features,
                return_fids=return_fids,
                datetime_as_string=datetime_as_string
            )

        ogr_types = [FIELD_TYPE_NAMES.get(field[1], "Unknown") for field in fields]
        ogr_subtypes = [
            FIELD_SUBTYPE_NAMES.get(field[4], "Unknown") for field in fields
        ]

        meta = {
            "crs": crs,
            "encoding": encoding,
            "fields": fields[:, 2],
            "dtypes": fields[:, 3],
            "ogr_types": ogr_types,
            "ogr_subtypes": ogr_subtypes,
            "geometry_type": geometry_type,
        }

    finally:
        if dataset_options != NULL:
            CSLDestroy(dataset_options)
            dataset_options = NULL

        if ogr_dataset != NULL:
            if sql is not None:
                GDALDatasetReleaseResultSet(ogr_dataset, ogr_layer)

            GDALClose(ogr_dataset)
            ogr_dataset = NULL

        # reset SHAPE_ENCODING config parameter if temporarily set above
        if override_shape_encoding:
            CPLSetThreadLocalConfigOption("SHAPE_ENCODING", prev_shape_encoding)

            if prev_shape_encoding != NULL:
                CPLFree(<void*>prev_shape_encoding)
                prev_shape_encoding = NULL

        if use_tmp_vsimem:
            vsimem_rmtree_toplevel(path)

    return (
        meta,
        fid_data,
        geometries,
        field_data
    )


cdef void pycapsule_array_stream_deleter(object stream_capsule) noexcept:
    cdef ArrowArrayStream* stream = <ArrowArrayStream*>PyCapsule_GetPointer(
        stream_capsule, "arrow_array_stream"
    )
    # Do not invoke the deleter on a used/moved capsule
    if stream.release != NULL:
        stream.release(stream)

    free(stream)


cdef object alloc_c_stream(ArrowArrayStream** c_stream):
    c_stream[0] = <ArrowArrayStream*> malloc(sizeof(ArrowArrayStream))
    # Ensure the capsule destructor doesn't call a random release pointer
    c_stream[0].release = NULL
    return PyCapsule_New(
        c_stream[0], "arrow_array_stream", &pycapsule_array_stream_deleter
    )


class _ArrowStream:
    def __init__(self, capsule):
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema=None):
        if requested_schema is not None:
            raise NotImplementedError("requested_schema is not supported")
        return self._capsule


@contextlib.contextmanager
def ogr_open_arrow(
    object path_or_buffer,
    dataset_kwargs,
    object layer=None,
    object encoding=None,
    int read_geometry=True,
    int force_2d=False,
    object columns=None,
    int skip_features=0,
    int max_features=0,
    object where=None,
    tuple bbox=None,
    object mask=None,
    object fids=None,
    str sql=None,
    str sql_dialect=None,
    int return_fids=False,
    int batch_size=0,
    use_pyarrow=False,
    datetime_as_string=False,
):

    cdef int err = 0
    cdef bint use_tmp_vsimem = isinstance(path_or_buffer, bytes)
    cdef const char *path_c = NULL
    cdef char **dataset_options = NULL
    cdef const char *where_c = NULL
    cdef OGRDataSourceH ogr_dataset = NULL
    cdef OGRLayerH ogr_layer = NULL
    cdef void *ogr_driver = NULL
    cdef char **fields_c = NULL
    cdef const char *field_c = NULL
    cdef char **options = NULL
    cdef const char *prev_shape_encoding = NULL
    cdef bint override_shape_encoding = False
    cdef ArrowArrayStream* stream
    cdef ArrowSchema schema

    if force_2d:
        raise ValueError("forcing 2D is not supported for Arrow")

    if fids is not None:
        if (
            where is not None
            or bbox is not None
            or mask is not None
            or sql is not None
            or skip_features
            or max_features
        ):
            raise ValueError(
                "cannot set both 'fids' and any of 'where', 'bbox', 'mask', "
                "'sql', 'skip_features', or 'max_features'"
            )

    IF CTE_GDAL_VERSION < 30800:
        if skip_features:
            raise ValueError(
                "specifying 'skip_features' is not supported for Arrow for GDAL<3.8.0"
            )

    if skip_features < 0:
        raise ValueError("'skip_features' must be >= 0")

    if max_features:
        raise ValueError(
            "specifying 'max_features' is not supported for Arrow"
        )

    if sql is not None and layer is not None:
        raise ValueError("'sql' parameter cannot be combined with 'layer'")

    if not (read_geometry or return_fids or columns is None or len(columns) > 0):
        raise ValueError(
            "at least one of read_geometry or return_fids must be True or columns must "
            "be None or non-empty"
        )

    if bbox and mask:
        raise ValueError("cannot set both 'bbox' and 'mask'")

    reader = None
    try:
        path = read_buffer_to_vsimem(
            path_or_buffer
        ) if use_tmp_vsimem else path_or_buffer

        if encoding:
            override_shape_encoding = True
            prev_shape_encoding = override_threadlocal_config_option(
                "SHAPE_ENCODING", encoding
            )

        dataset_options = dict_to_options(dataset_kwargs)
        ogr_dataset = ogr_open(path.encode("UTF-8"), 0, dataset_options)

        if sql is None:
            if layer is None:
                layer = get_default_layer(ogr_dataset)
            ogr_layer = get_ogr_layer(ogr_dataset, layer)
        else:
            ogr_layer = execute_sql(ogr_dataset, sql, sql_dialect)

        crs = get_crs(ogr_layer)

        # Encoding is derived from the user, from the dataset capabilities / type,
        # or from the system locale
        if encoding:
            if get_driver(ogr_dataset) == "ESRI Shapefile":
                if "ENCODING" in dataset_kwargs:
                    raise ValueError(
                        'cannot provide both encoding parameter and "ENCODING" option; '
                        "use encoding parameter to specify correct encoding for data "
                        "source"
                    )

                encoding = "UTF-8"

            elif encoding.replace("-", "").upper() != "UTF8":
                raise ValueError(
                    "non-UTF-8 encoding is not supported for Arrow; use the non-Arrow "
                    "interface instead"
                )

        else:
            encoding = detect_encoding(ogr_dataset, ogr_layer)

        fields = get_fields(ogr_layer, encoding, use_arrow=True)

        ignored_fields = []
        if columns is not None:
            # Fields are matched exactly by name, duplicates are dropped.
            ignored_fields = list(set(fields[:, 2]) - set(columns))

            # Find index of each field in columns, and only keep those
            idx = np.sort(np.intersect1d(fields[:, 2], columns, return_indices=True)[1])
            fields = fields[idx, :]

        if not read_geometry:
            ignored_fields.append("OGR_GEOMETRY")

        # raise error if schema has bool values for FGB / GPKG and GDAL <3.8.3
        # due to https://github.com/OSGeo/gdal/issues/8998
        IF CTE_GDAL_VERSION < 30803:

            driver = get_driver(ogr_dataset)
            if driver in {"FlatGeobuf", "GPKG"}:
                for field in fields:
                    if field[3] == "bool":  # numpy type is bool
                        raise RuntimeError(
                            "GDAL < 3.8.3 does not correctly read boolean data values "
                            "using the Arrow API. Do not use read_arrow() / "
                            "use_arrow=True for this dataset."
                        )

        geometry_type = get_geometry_type(ogr_layer)

        geometry_name = get_string(OGR_L_GetGeometryColumn(ogr_layer))

        fid_column = get_string(OGR_L_GetFIDColumn(ogr_layer))
        fid_column_where = fid_column
        # OGR_L_GetFIDColumn returns the column name if it is a custom column,
        # or "" if not. For arrow, the default column name used to return the FID data
        # read is "OGC_FID". When accessing the underlying datasource like when using a
        # where clause, the default column name is "FID".
        if fid_column == "":
            fid_column = "OGC_FID"
            fid_column_where = "FID"

        # Use fids list to create a where clause, as arrow doesn't support direct fid
        # filtering.
        if fids is not None:
            IF CTE_GDAL_VERSION < 30800:
                driver = get_driver(ogr_dataset)
                if driver not in {"GPKG", "GeoJSON"}:
                    warnings.warn(
                        "Using 'fids' and 'use_arrow=True' with GDAL < 3.8 can be slow "
                        "for some drivers. Upgrading GDAL or using 'use_arrow=False' "
                        "can avoid this.",
                        stacklevel=2,
                    )

            fids_str = ",".join([str(fid) for fid in fids])
            where = f"{fid_column_where} IN ({fids_str})"

        # Apply the attribute filter
        if where is not None and where != "":
            try:
                apply_where_filter(ogr_layer, where)
            except ValueError as ex:
                if fids is not None and str(ex).startswith("Invalid SQL query"):
                    # If fids is not None, the where being applied is the one formatted
                    # above.
                    raise ValueError(
                        f"error applying filter for {len(fids)} fids; max. number for "
                        f"drivers with default SQL dialect 'OGRSQL' is 4997"
                    ) from ex

                raise

        # Apply the spatial filter
        if bbox is not None:
            apply_bbox_filter(ogr_layer, bbox)

        elif mask is not None:
            apply_geometry_filter(ogr_layer, mask)

        # Limit feature range to available range (cannot use logic of
        # `validate_feature_range` because max_features is not supported)
        if skip_features > 0:
            feature_count = get_feature_count(ogr_layer, 1)
            if skip_features >= feature_count:
                skip_features = feature_count

        # Limit to specified columns
        if ignored_fields:
            for field in ignored_fields:
                field_b = field.encode("utf-8")
                field_c = field_b
                fields_c = CSLAddString(fields_c, field_c)

            OGR_L_SetIgnoredFields(ogr_layer, <const char**>fields_c)

        if not return_fids:
            options = CSLSetNameValue(options, "INCLUDE_FID", "NO")

        if batch_size > 0:
            options = CSLSetNameValue(
                options,
                "MAX_FEATURES_IN_BATCH",
                str(batch_size).encode("UTF-8")
            )

        # Default to geoarrow metadata encoding
        IF CTE_GDAL_VERSION >= 30800:
            options = CSLSetNameValue(
                options,
                "GEOMETRY_METADATA_ENCODING",
                "GEOARROW".encode("UTF-8")
            )

        # Read DateTime fields as strings, as the Arrow DateTime column type is
        # quite limited regarding support for mixed time zones,...
        IF CTE_GDAL_VERSION >= 31100:
            if datetime_as_string:
                options = CSLSetNameValue(options, "DATETIME_AS_STRING", "YES")

        # make sure layer is read from beginning
        OGR_L_ResetReading(ogr_layer)

        # allocate the stream struct and wrap in capsule to ensure clean-up on error
        capsule = alloc_c_stream(&stream)

        if not OGR_L_GetArrowStream(ogr_layer, stream, options):
            raise RuntimeError("Failed to open ArrowArrayStream from Layer")

        if skip_features > 0:
            # only supported for GDAL >= 3.8.0; have to do this after getting
            # the Arrow stream
            # use `OGR_L_SetNextByIndex` directly and not `apply_skip_features`
            # to ignore errors in case skip_features == feature_count
            OGR_L_SetNextByIndex(ogr_layer, skip_features)

        if use_pyarrow:
            import pyarrow as pa

            reader = pa.RecordBatchStreamReader._import_from_c(<uintptr_t> stream)
        else:
            reader = _ArrowStream(capsule)

        ogr_types = [FIELD_TYPE_NAMES.get(field[1], "Unknown") for field in fields]
        ogr_subtypes = [
            FIELD_SUBTYPE_NAMES.get(field[4], "Unknown") for field in fields
        ]

        meta = {
            "crs": crs,
            "encoding": encoding,
            "fields": fields[:, 2],
            "dtypes": fields[:, 3],
            "ogr_types": ogr_types,
            "ogr_subtypes": ogr_subtypes,
            "geometry_type": geometry_type,
            "geometry_name": geometry_name,
            "fid_column": fid_column,
        }

        # stream has to be consumed before the Dataset is closed
        yield meta, reader

    finally:
        if use_pyarrow and reader is not None:
            # Mark reader as closed to prevent reading batches
            reader.close()

        # `stream` will be freed through `capsule` destructor

        CSLDestroy(options)
        if fields_c != NULL:
            CSLDestroy(fields_c)
            fields_c = NULL

        if dataset_options != NULL:
            CSLDestroy(dataset_options)
            dataset_options = NULL

        if ogr_dataset != NULL:
            if sql is not None:
                GDALDatasetReleaseResultSet(ogr_dataset, ogr_layer)

            GDALClose(ogr_dataset)
            ogr_dataset = NULL

        # reset SHAPE_ENCODING config parameter if temporarily set above
        if override_shape_encoding:
            CPLSetThreadLocalConfigOption("SHAPE_ENCODING", prev_shape_encoding)

            if prev_shape_encoding != NULL:
                CPLFree(<void*>prev_shape_encoding)
                prev_shape_encoding = NULL

        if use_tmp_vsimem:
            vsimem_rmtree_toplevel(path)


def ogr_read_bounds(
    object path_or_buffer,
    object layer=None,
    object encoding=None,
    int read_geometry=True,
    int force_2d=False,
    object columns=None,
    int skip_features=0,
    int max_features=0,
    object where=None,
    tuple bbox=None,
    object mask=None,
):

    cdef int err = 0
    cdef bint use_tmp_vsimem = isinstance(path_or_buffer, bytes)
    cdef const char *path_c = NULL
    cdef const char *where_c = NULL
    cdef OGRDataSourceH ogr_dataset = NULL
    cdef OGRLayerH ogr_layer = NULL
    cdef int feature_count = 0
    cdef double xmin, ymin, xmax, ymax

    if bbox and mask:
        raise ValueError("cannot set both 'bbox' and 'mask'")

    if skip_features < 0:
        raise ValueError("'skip_features' must be >= 0")

    if max_features < 0:
        raise ValueError("'max_features' must be >= 0")

    try:
        path = read_buffer_to_vsimem(
            path_or_buffer
        ) if use_tmp_vsimem else path_or_buffer
        ogr_dataset = ogr_open(path.encode("UTF-8"), 0, NULL)

        if layer is None:
            layer = get_default_layer(ogr_dataset)

        ogr_layer = get_ogr_layer(ogr_dataset, layer)

        # Apply the attribute filter
        if where is not None and where != "":
            apply_where_filter(ogr_layer, where)

        # Apply the spatial filter
        if bbox is not None:
            apply_bbox_filter(ogr_layer, bbox)

        elif mask is not None:
            apply_geometry_filter(ogr_layer, mask)

        # Limit feature range to available range
        skip_features, num_features = validate_feature_range(
            ogr_layer, skip_features, max_features
        )

        bounds = get_bounds(ogr_layer, skip_features, num_features)

    finally:
        if ogr_dataset != NULL:
            GDALClose(ogr_dataset)
            ogr_dataset = NULL

        if use_tmp_vsimem:
            vsimem_rmtree_toplevel(path)

    return bounds


def ogr_read_info(
    object path_or_buffer,
    dataset_kwargs,
    object layer=None,
    object encoding=None,
    int force_feature_count=False,
    int force_total_bounds=False
):

    cdef bint use_tmp_vsimem = isinstance(path_or_buffer, bytes)
    cdef const char *path_c = NULL
    cdef char **dataset_options = NULL
    cdef OGRDataSourceH ogr_dataset = NULL
    cdef OGRLayerH ogr_layer = NULL
    cdef const char *prev_shape_encoding = NULL
    cdef bint override_shape_encoding = False

    try:
        path = read_buffer_to_vsimem(
            path_or_buffer
        ) if use_tmp_vsimem else path_or_buffer

        if encoding:
            override_shape_encoding = True
            prev_shape_encoding = override_threadlocal_config_option(
                "SHAPE_ENCODING", encoding
            )

        dataset_options = dict_to_options(dataset_kwargs)
        ogr_dataset = ogr_open(path.encode("UTF-8"), 0, dataset_options)

        if layer is None:
            layer = get_default_layer(ogr_dataset)
        ogr_layer = get_ogr_layer(ogr_dataset, layer)

        if encoding and get_driver(ogr_dataset) == "ESRI Shapefile":
            encoding = "UTF-8"
        else:
            encoding = encoding or detect_encoding(ogr_dataset, ogr_layer)

        fields = get_fields(ogr_layer, encoding)
        ogr_types = [FIELD_TYPE_NAMES.get(field[1], "Unknown") for field in fields]
        ogr_subtypes = [
            FIELD_SUBTYPE_NAMES.get(field[4], "Unknown") for field in fields
        ]

        meta = {
            "layer_name": get_string(OGR_L_GetName(ogr_layer)),
            "crs": get_crs(ogr_layer),
            "encoding": encoding,
            "fields": fields[:, 2],
            "dtypes": fields[:, 3],
            "ogr_types": ogr_types,
            "ogr_subtypes": ogr_subtypes,
            "fid_column": get_string(OGR_L_GetFIDColumn(ogr_layer)),
            "geometry_name": get_string(OGR_L_GetGeometryColumn(ogr_layer)),
            "geometry_type": get_geometry_type(ogr_layer),
            "features": get_feature_count(ogr_layer, force_feature_count),
            "total_bounds": get_total_bounds(ogr_layer, force_total_bounds),
            "driver": get_driver(ogr_dataset),
            "capabilities": {
                "random_read": OGR_L_TestCapability(ogr_layer, OLCRandomRead) == 1,
                "fast_set_next_by_index": OGR_L_TestCapability(
                    ogr_layer, OLCFastSetNextByIndex
                ) == 1,
                "fast_spatial_filter": OGR_L_TestCapability(
                    ogr_layer, OLCFastSpatialFilter
                ) == 1,
                "fast_feature_count": OGR_L_TestCapability(
                    ogr_layer, OLCFastFeatureCount
                ) == 1,
                "fast_total_bounds": OGR_L_TestCapability(
                    ogr_layer, OLCFastGetExtent
                ) == 1,
            },
            "layer_metadata": get_metadata(ogr_layer),
            "dataset_metadata": get_metadata(ogr_dataset),
        }

    finally:
        if dataset_options != NULL:
            CSLDestroy(dataset_options)
            dataset_options = NULL

        if ogr_dataset != NULL:
            GDALClose(ogr_dataset)
            ogr_dataset = NULL

        # reset SHAPE_ENCODING config parameter if temporarily set above
        if override_shape_encoding:
            CPLSetThreadLocalConfigOption("SHAPE_ENCODING", prev_shape_encoding)

            if prev_shape_encoding != NULL:
                CPLFree(<void*>prev_shape_encoding)

        if use_tmp_vsimem:
            vsimem_rmtree_toplevel(path)

    return meta


def ogr_list_layers(object path_or_buffer):
    cdef bint use_tmp_vsimem = isinstance(path_or_buffer, bytes)
    cdef const char *path_c = NULL
    cdef OGRDataSourceH ogr_dataset = NULL

    try:
        path = (
            read_buffer_to_vsimem(path_or_buffer) if use_tmp_vsimem else path_or_buffer
        )
        ogr_dataset = ogr_open(path.encode("UTF-8"), 0, NULL)
        layers = get_layer_names(ogr_dataset)

    finally:
        if ogr_dataset != NULL:
            GDALClose(ogr_dataset)
            ogr_dataset = NULL

        if use_tmp_vsimem:
            vsimem_rmtree_toplevel(path)

    return layers


cdef str get_default_layer(OGRDataSourceH ogr_dataset):
    """ Get the layer in the dataset that is read by default.

    The caller is responsible for closing the dataset.

    Parameters
    ----------
    ogr_dataset : pointer to open OGR dataset

    Returns
    -------
    str
        the name of the default layer to be read.

    """
    layers = get_layer_names(ogr_dataset)
    first_layer_name = layers[0][0]

    if len(layers) > 1:
        dataset_name = os.path.basename(get_string(OGR_DS_GetName(ogr_dataset)))

        other_layer_names = ", ".join([f"'{lyr}'" for lyr in layers[1:, 0]])
        warnings.warn(
            f"More than one layer found in '{dataset_name}': '{first_layer_name}' "
            f"(default), {other_layer_names}. Specify layer parameter to avoid this "
            "warning.",
            stacklevel=2,
        )

    return first_layer_name


cdef get_layer_names(OGRDataSourceH ogr_dataset):
    """ Get the layers in the dataset.

    The caller is responsible for closing the dataset.

    Parameters
    ----------
    ogr_dataset : pointer to open OGR dataset

    Returns
    -------
    ndarray(n)
        array of layer names

    """
    cdef OGRLayerH ogr_layer = NULL

    layer_count = GDALDatasetGetLayerCount(ogr_dataset)

    data = np.empty(shape=(layer_count, 2), dtype=object)
    data_view = data[:]
    for i in range(layer_count):
        ogr_layer = GDALDatasetGetLayer(ogr_dataset, i)

        data_view[i, 0] = get_string(OGR_L_GetName(ogr_layer))
        data_view[i, 1] = get_geometry_type(ogr_layer)

    return data


# NOTE: all modes are write-only
# some data sources have multiple layers
cdef void * ogr_create(
    const char* path_c, const char* driver_c, char** options
) except NULL:
    cdef void *ogr_driver = NULL
    cdef OGRDataSourceH ogr_dataset = NULL

    # Get the driver
    try:
        ogr_driver = check_pointer(GDALGetDriverByName(driver_c))

    except NullPointerError:
        raise DataSourceError(
            f"Could not obtain driver: {driver_c.decode('utf-8')} "
            "(check that it was installed correctly into GDAL)"
        )

    except CPLE_BaseError as exc:
        raise DataSourceError(str(exc))

    # For /vsimem/ files, with GDAL >= 3.8 parent directories are created automatically.
    IF CTE_GDAL_VERSION < 30800:
        path = path_c.decode("UTF-8")
        if "/vsimem/" in path:
            parent = str(Path(path).parent.as_posix())
            if not parent.endswith("/vsimem"):
                retcode = VSIMkdirRecursive(parent.encode("UTF-8"), 0666)
                if retcode != 0:
                    raise OSError(f"Could not create parent directory '{parent}'")

    # Create the dataset
    try:
        ogr_dataset = check_pointer(
            GDALCreate(ogr_driver, path_c, 0, 0, 0, GDT_Unknown, options)
        )

    except NullPointerError:
        raise DataSourceError(
            f"Failed to create dataset with driver: {path_c.decode('utf-8')} "
            f"{driver_c.decode('utf-8')}"
        ) from None

    except CPLE_NotSupportedError as exc:
        raise DataSourceError(
            f"Driver {driver_c.decode('utf-8')} does not support write functionality"
        ) from None

    except CPLE_BaseError as exc:
        raise DataSourceError(str(exc))

    return ogr_dataset


cdef void * create_crs(str crs) except NULL:
    cdef char *crs_c = NULL
    cdef void *ogr_crs = NULL

    crs_b = crs.encode("UTF-8")
    crs_c = crs_b

    try:
        ogr_crs = check_pointer(OSRNewSpatialReference(NULL))
        err = OSRSetFromUserInput(ogr_crs, crs_c)
        if err:
            raise CRSError(
                "Could not set CRS: {}".format(crs_c.decode("UTF-8"))
            ) from None

    except CPLE_BaseError as exc:
        OSRRelease(ogr_crs)
        raise CRSError("Could not set CRS: {}".format(exc))

    return ogr_crs


cdef infer_field_types(list dtypes):
    cdef int field_type = 0
    cdef int field_subtype = 0
    cdef int width = 0
    cdef int precision = 0

    field_types = np.zeros(shape=(len(dtypes), 4), dtype=int)
    field_types_view = field_types[:]

    for i in range(len(dtypes)):
        dtype = dtypes[i]

        if dtype.name in DTYPE_OGR_FIELD_TYPES:
            field_type, field_subtype = DTYPE_OGR_FIELD_TYPES[dtype.name]
            field_types_view[i, 0] = field_type
            field_types_view[i, 1] = field_subtype

        # Determine field type from ndarray values
        elif dtype == np.dtype("O"):
            # Object type is ambiguous: could be a string or binary data
            # TODO: handle binary or other types
            # for now fall back to string (same as Geopandas)
            field_types_view[i, 0] = OFTString
            # Convert to unicode string then take itemsize
            # TODO: better implementation of this
            # width = values.astype(np.str_).dtype.itemsize // 4
            # DO WE NEED WIDTH HERE?

        elif dtype.type is np.str_ or dtype.type is np.bytes_:
            field_types_view[i, 0] = OFTString
            field_types_view[i, 2] = int(dtype.itemsize // 4)

        elif dtype.name.startswith("datetime64"):
            # datetime dtype precision is specified with eg. [ms], but this isn't
            # usefull when writing to gdal.
            field_type, field_subtype = DTYPE_OGR_FIELD_TYPES["datetime64"]
            field_types_view[i, 0] = field_type
            field_types_view[i, 1] = field_subtype

        else:
            raise NotImplementedError(
                f"field type is not supported {dtype.name} (field index: {i})"
            )

    return field_types


cdef create_ogr_dataset_layer(
    str path,
    bint use_tmp_vsimem,
    str layer,
    str driver,
    str crs,
    str geometry_type,
    str encoding,
    object dataset_kwargs,
    object layer_kwargs,
    bint append,
    dataset_metadata,
    layer_metadata,
    OGRDataSourceH* ogr_dataset_out,
    OGRLayerH* ogr_layer_out,
):
    """
    Construct the OGRDataSource and OGRLayer objects based on input
    path and layer.

    If the file already exists, will open the existing dataset and overwrite
    or append the layer (depending on `append`), otherwise will create a new
    dataset.

    Fills in the `ogr_dataset_out` and `ogr_layer_out` pointers passed as
    parameter with initialized objects (or raise error is it fails to do so).
    It is the responsibility of the caller to clean up those objects after use.
    Returns whether a new layer was created or not (when the layer was created,
    the caller still needs to set up the layer definition, i.e. create the
    fields).

    Parameters
    ----------
    encoding : str
        Only used if `driver` is "ESRI Shapefile". If not None, it overrules the default
        shapefile encoding, which is "UTF-8" in pyogrio.
    use_tmp_vsimem : bool
        Whether the file path is meant to save a temporary memory file to.

    Returns
    -------
    bool :
        Whether a new layer was created, or False if we are appending to an
        existing layer.
    """
    cdef const char *path_c = NULL
    cdef const char *layer_c = NULL
    cdef const char *driver_c = NULL
    cdef const char *crs_c = NULL
    cdef const char *encoding_c = NULL
    cdef char **dataset_options = NULL
    cdef char **layer_options = NULL
    cdef const char *ogr_name = NULL
    cdef OGRDataSourceH ogr_dataset = NULL
    cdef OGRLayerH ogr_layer = NULL
    cdef OGRSpatialReferenceH ogr_crs = NULL
    cdef OGRwkbGeometryType geometry_code
    cdef int layer_idx = -1

    path_b = path.encode("UTF-8")
    path_c = path_b

    driver_b = driver.encode("UTF-8")
    driver_c = driver_b

    # temporary in-memory dataset is always created from scratch
    path_exists = os.path.exists(path) if not use_tmp_vsimem else False

    if not layer:
        # For multi extensions (e.g. ".shp.zip"), strip the full extension
        for multi_ext in MULTI_EXTENSIONS:
            if path.endswith(multi_ext):
                layer = os.path.split(path)[1][:-len(multi_ext)]
                break

        # If it wasn't a multi-extension, use the file stem
        if not layer:
            layer = os.path.splitext(os.path.split(path)[1])[0]

    # if shapefile, GeoJSON, or FlatGeobuf, always delete first
    # for other types, check if we can create layers
    # GPKG might be the only multi-layer writeable type.  TODO: check this
    if (
        driver in ("ESRI Shapefile", "GeoJSON", "GeoJSONSeq", "FlatGeobuf")
        and path_exists
    ):
        if not append:
            os.unlink(path)
            path_exists = False

    layer_exists = False
    if path_exists:
        try:
            ogr_dataset = ogr_open(path_c, 1, NULL)

            for i in range(GDALDatasetGetLayerCount(ogr_dataset)):
                name = OGR_L_GetName(GDALDatasetGetLayer(ogr_dataset, i))
                if layer == name.decode("UTF-8"):
                    layer_idx = i
                    break

            if layer_idx >= 0:
                layer_exists = True

                if not append:
                    GDALDatasetDeleteLayer(ogr_dataset, layer_idx)

        except DataSourceError as exc:
            # open failed
            if append:
                raise exc

            # otherwise create from scratch
            os.unlink(path)

            ogr_dataset = NULL

    # either it didn't exist or could not open it in write mode
    if ogr_dataset == NULL:
        dataset_options = dict_to_options(dataset_kwargs)
        ogr_dataset = ogr_create(path_c, driver_c, dataset_options)

    # if we are not appending to an existing layer, we need to create
    # the layer and all associated properties (CRS, field defs, etc)
    create_layer = not (append and layer_exists)

    # Create the layer
    if create_layer:
        # Create the CRS
        if crs is not None:
            try:
                ogr_crs = create_crs(crs)
                # force geographic CRS to use lon, lat order and ignore axis order
                # specified by CRS, in order to correctly write KML and GeoJSON
                # coordinates in correct order
                OSRSetAxisMappingStrategy(ogr_crs, OAMS_TRADITIONAL_GIS_ORDER)

            except Exception as exc:
                if dataset_options != NULL:
                    CSLDestroy(dataset_options)
                    dataset_options = NULL

                GDALClose(ogr_dataset)
                ogr_dataset = NULL

                raise exc

        # Setup other layer creation options
        for k, v in layer_kwargs.items():
            k = k.encode("UTF-8")
            v = v.encode("UTF-8")
            layer_options = CSLAddNameValue(
                layer_options, <const char *>k, <const char *>v
            )

        if driver == "ESRI Shapefile":
            # ENCODING option must be set for shapefiles to properly write *.cpg
            # file containing the encoding; this is not a supported option for
            # other drivers.  This is done after setting general options above
            # to override ENCODING if passed by the user as a layer option.
            if encoding and "ENCODING" in layer_kwargs:
                raise ValueError(
                    'cannot provide both encoding parameter and "ENCODING" layer '
                    "creation option; use the encoding parameter"
                )

            # always write to UTF-8 if encoding is not set
            encoding = encoding or "UTF-8"
            encoding_b = encoding.upper().encode("UTF-8")
            encoding_c = encoding_b
            layer_options = CSLSetNameValue(layer_options, "ENCODING", encoding_c)

        # Get geometry type
        # TODO: this is brittle for 3D / ZM / M types
        # TODO: fail on M / ZM types
        geometry_code = get_geometry_type_code(geometry_type)

    try:
        if create_layer:
            layer_b = layer.encode("UTF-8")
            layer_c = layer_b

            ogr_layer = check_pointer(
                GDALDatasetCreateLayer(
                    ogr_dataset, layer_c, ogr_crs, geometry_code, layer_options
                )
            )

        else:
            ogr_layer = check_pointer(get_ogr_layer(ogr_dataset, layer))

        # Set dataset and layer metadata
        set_metadata(ogr_dataset, dataset_metadata)
        set_metadata(ogr_layer, layer_metadata)

    except Exception as exc:
        GDALClose(ogr_dataset)
        ogr_dataset = NULL
        raise DataLayerError(str(exc))

    finally:
        if ogr_crs != NULL:
            OSRRelease(ogr_crs)
            ogr_crs = NULL

        if dataset_options != NULL:
            CSLDestroy(dataset_options)
            dataset_options = NULL

        if layer_options != NULL:
            CSLDestroy(layer_options)
            layer_options = NULL

    ogr_dataset_out[0] = ogr_dataset
    ogr_layer_out[0] = ogr_layer

    return create_layer


# TODO: set geometry and field data as memory views?
def ogr_write(
    object path_or_fp,
    str layer,
    str driver,
    geometry,
    fields,
    field_data,
    field_mask,
    str crs,
    str geometry_type,
    str encoding,
    object dataset_kwargs,
    object layer_kwargs,
    bint promote_to_multi=False,
    bint nan_as_null=True,
    bint append=False,
    dataset_metadata=None,
    layer_metadata=None,
    gdal_tz_offsets=None
):
    cdef OGRDataSourceH ogr_dataset = NULL
    cdef OGRLayerH ogr_layer = NULL
    cdef OGRFeatureH ogr_feature = NULL
    cdef OGRGeometryH ogr_geometry = NULL
    cdef OGRGeometryH ogr_geometry_multi = NULL
    cdef OGRFeatureDefnH ogr_featuredef = NULL
    cdef OGRFieldDefnH ogr_fielddef = NULL
    cdef const unsigned char *wkb_buffer = NULL
    cdef unsigned int wkbtype = 0
    cdef int supports_transactions = 0
    cdef int err = 0
    cdef int i = 0
    cdef int num_records = -1
    cdef int num_field_data = len(field_data) if field_data is not None else 0
    cdef int num_fields = len(fields) if fields is not None else 0
    cdef bint use_tmp_vsimem = False

    if num_fields != num_field_data:
        raise ValueError("field_data array needs to be same length as fields array")

    if num_fields == 0 and geometry is None:
        raise ValueError("You must provide at least a geometry column or a field")

    if num_fields > 0:
        num_records = len(field_data[0])
        for i in range(1, len(field_data)):
            if len(field_data[i]) != num_records:
                raise ValueError("field_data arrays must be same length")

    if geometry is None:
        # If no geometry data, we ignore the geometry_type and don't create a geometry
        # column
        geometry_type = None
    else:
        if num_fields > 0:
            if len(geometry) != num_records:
                raise ValueError(
                    "field_data arrays must be same length as geometry array"
                )
        else:
            num_records = len(geometry)

    if field_mask is not None:
        if len(field_data) != len(field_mask):
            raise ValueError("field_data and field_mask must be same length")
        for i in range(0, len(field_mask)):
            if field_mask[i] is not None and len(field_mask[i]) != num_records:
                raise ValueError(
                    "field_mask arrays must be same length as geometry array"
                )
    else:
        field_mask = [None] * num_fields

    if gdal_tz_offsets is None:
        gdal_tz_offsets = {}

    try:
        # Setup in-memory handler if needed
        path, use_tmp_vsimem = get_ogr_vsimem_write_path(path_or_fp, driver)

        # Setup dataset and layer
        layer_created = create_ogr_dataset_layer(
            path, use_tmp_vsimem, layer, driver, crs, geometry_type, encoding,
            dataset_kwargs, layer_kwargs, append,
            dataset_metadata, layer_metadata,
            &ogr_dataset, &ogr_layer,
        )

        if driver == "ESRI Shapefile":
            # force encoding for remaining operations to be in UTF-8 (even if
            # user provides an encoding) because GDAL will automatically
            # convert those to the target encoding because ENCODING is set as a
            # layer creation option
            encoding = "UTF-8"

        else:
            # Now the dataset and layer have been created, we can properly
            # determine the encoding. It is derived from the user, from the
            # dataset capabilities / type, or from the system locale
            encoding = encoding or detect_encoding(ogr_dataset, ogr_layer)

        # Create the fields
        field_types = None
        if num_fields > 0:
            field_types = infer_field_types([field.dtype for field in field_data])

        if layer_created:
            for i in range(num_fields):
                field_type, field_subtype, width, precision = field_types[i]

                name_b = fields[i].encode(encoding)
                try:
                    ogr_fielddef = check_pointer(OGR_Fld_Create(name_b, field_type))

                    # subtypes, see: https://gdal.org/development/rfc/rfc50_ogr_field_subtype.html  # noqa: E501
                    if field_subtype != OFSTNone:
                        OGR_Fld_SetSubType(ogr_fielddef, field_subtype)

                    if width:
                        OGR_Fld_SetWidth(ogr_fielddef, width)

                    # TODO: set precision

                    check_int(OGR_L_CreateField(ogr_layer, ogr_fielddef, 1))

                except Exception:
                    raise FieldError(
                        f"Error adding field '{fields[i]}' to layer"
                    ) from None

                finally:
                    if ogr_fielddef != NULL:
                        OGR_Fld_Destroy(ogr_fielddef)
                        ogr_fielddef = NULL

        # Create the features
        ogr_featuredef = OGR_L_GetLayerDefn(ogr_layer)

        supports_transactions = OGR_L_TestCapability(ogr_layer, OLCTransactions)
        if supports_transactions:
            start_transaction(ogr_dataset, 0)

        for i in range(num_records):
            # create the feature
            ogr_feature = OGR_F_Create(ogr_featuredef)
            if ogr_feature == NULL:
                raise FeatureError(f"Could not create feature at index {i}") from None

            # create the geometry based on specific WKB type
            # (there might be mixed types in geometries)
            # TODO: geometry must not be null or errors
            wkb = None if geometry is None else geometry[i]
            if wkb is not None:
                wkb_buffer = wkb
                if wkb_buffer[0] == 1:
                    # Little endian WKB type.
                    wkbtype = (
                        wkb_buffer[1] + (wkb_buffer[2] << 8) + (wkb_buffer[3] << 16) +
                        (<unsigned int>wkb_buffer[4] << 24)
                    )
                else:
                    # Big endian WKB type.
                    wkbtype = (
                        (<unsigned int>(wkb_buffer[1]) << 24) + (wkb_buffer[2] << 16) +
                        (wkb_buffer[3] << 8) + wkb_buffer[4]
                    )
                ogr_geometry = OGR_G_CreateGeometry(<OGRwkbGeometryType>wkbtype)
                if ogr_geometry == NULL:
                    raise GeometryError(
                        f"Could not create geometry at index {i} for WKB type {wkbtype}"
                    ) from None

                # import the WKB
                err = OGR_G_ImportFromWkb(ogr_geometry, wkb_buffer, len(wkb))
                if err:
                    raise GeometryError(
                        f"Could not create geometry from WKB at index {i}"
                    ) from None

                # Convert to multi type
                if promote_to_multi:
                    if wkbtype in (wkbPoint, wkbPoint25D, wkbPointM, wkbPointZM):
                        ogr_geometry = OGR_G_ForceToMultiPoint(ogr_geometry)
                    elif wkbtype in (
                        wkbLineString, wkbLineString25D, wkbLineStringM, wkbLineStringZM
                    ):
                        ogr_geometry = OGR_G_ForceToMultiLineString(ogr_geometry)
                    elif wkbtype in (
                        wkbPolygon, wkbPolygon25D, wkbPolygonM, wkbPolygonZM
                    ):
                        ogr_geometry = OGR_G_ForceToMultiPolygon(ogr_geometry)

                # Set the geometry on the feature
                # this assumes ownership of the geometry and it's cleanup
                err = OGR_F_SetGeometryDirectly(ogr_feature, ogr_geometry)
                ogr_geometry = NULL  # to prevent cleanup after this point
                if err:
                    raise GeometryError(
                        f"Could not set geometry for feature at index {i}"
                    ) from None

            # Set field values
            for field_idx in range(num_fields):
                field_value = field_data[field_idx][i]
                field_type = field_types[field_idx][0]

                mask = field_mask[field_idx]
                if mask is not None and mask[i]:
                    OGR_F_SetFieldNull(ogr_feature, field_idx)

                elif field_type == OFTString:
                    if (
                        field_value is None
                        or (isinstance(field_value, float) and isnan(field_value))
                    ):
                        OGR_F_SetFieldNull(ogr_feature, field_idx)

                    else:
                        if not isinstance(field_value, str):
                            field_value = str(field_value)

                        try:
                            value_b = field_value.encode(encoding)
                            OGR_F_SetFieldString(ogr_feature, field_idx, value_b)

                        except AttributeError:
                            raise ValueError(
                                f"Could not encode value '{field_value}' in field "
                                f"'{fields[field_idx]}' to string"
                            )

                        except Exception:
                            raise

                elif field_type == OFTInteger:
                    OGR_F_SetFieldInteger(ogr_feature, field_idx, field_value)

                elif field_type == OFTInteger64:
                    OGR_F_SetFieldInteger64(ogr_feature, field_idx, field_value)

                elif field_type == OFTReal:
                    if nan_as_null and isnan(field_value):
                        OGR_F_SetFieldNull(ogr_feature, field_idx)
                    else:
                        OGR_F_SetFieldDouble(ogr_feature, field_idx, field_value)

                elif field_type == OFTDate:
                    if np.isnat(field_value):
                        OGR_F_SetFieldNull(ogr_feature, field_idx)
                    else:
                        datetime = field_value.item()
                        OGR_F_SetFieldDateTimeEx(
                            ogr_feature,
                            field_idx,
                            datetime.year,
                            datetime.month,
                            datetime.day,
                            0,
                            0,
                            0.0,
                            0
                        )

                elif field_type == OFTDateTime:
                    if np.isnat(field_value):
                        OGR_F_SetFieldNull(ogr_feature, field_idx)
                    else:
                        datetime = field_value.astype("datetime64[ms]").item()
                        tz_array = gdal_tz_offsets.get(fields[field_idx], None)
                        if tz_array is None:
                            gdal_tz = 0
                        else:
                            gdal_tz = tz_array[i]
                        OGR_F_SetFieldDateTimeEx(
                            ogr_feature,
                            field_idx,
                            datetime.year,
                            datetime.month,
                            datetime.day,
                            datetime.hour,
                            datetime.minute,
                            datetime.second + datetime.microsecond / 10**6,
                            gdal_tz
                        )

                else:
                    raise NotImplementedError(
                        f"OGR field type is not supported for writing: {field_type}"
                    )

            # Add feature to the layer
            try:
                check_int(OGR_L_CreateFeature(ogr_layer, ogr_feature))

            except CPLE_BaseError as exc:
                raise FeatureError(
                    f"Could not add feature to layer at index {i}: {exc}"
                ) from None

            OGR_F_Destroy(ogr_feature)
            ogr_feature = NULL

        if supports_transactions:
            commit_transaction(ogr_dataset)

        log.info(f"Created {num_records:,} records")

        # close dataset to force driver to flush data
        exc = ogr_close(ogr_dataset)
        ogr_dataset = NULL
        if exc:
            raise DataSourceError(f"Failed to write features to dataset {path}; {exc}")

        # copy in-memory file back to path_or_fp object
        if use_tmp_vsimem:
            read_vsimem_to_buffer(path, path_or_fp)

    finally:
        # Final cleanup
        # make sure that all objects allocated above are released if exceptions
        # are raised, and the dataset is closed
        if ogr_fielddef != NULL:
            OGR_Fld_Destroy(ogr_fielddef)
            ogr_fielddef = NULL

        if ogr_feature != NULL:
            OGR_F_Destroy(ogr_feature)
            ogr_feature = NULL

        if ogr_geometry != NULL:
            OGR_G_DestroyGeometry(ogr_geometry)
            ogr_geometry = NULL

        if ogr_dataset != NULL:
            ogr_close(ogr_dataset)

        if use_tmp_vsimem:
            vsimem_rmtree_toplevel(path)


def ogr_write_arrow(
    object path_or_fp,
    str layer,
    str driver,
    object arrow_obj,
    str crs,
    str geometry_type,
    str geometry_name,
    str encoding,
    object dataset_kwargs,
    object layer_kwargs,
    bint append=False,
    dataset_metadata=None,
    layer_metadata=None,
):
    IF CTE_GDAL_VERSION < 30800:
        raise RuntimeError("Need GDAL>=3.8 for Arrow write support")

    cdef OGRDataSourceH ogr_dataset = NULL
    cdef OGRLayerH ogr_layer = NULL
    cdef char **options = NULL
    cdef bint use_tmp_vsimem = False
    cdef ArrowArrayStream* stream = NULL
    cdef ArrowSchema schema
    cdef ArrowArray array

    schema.release = NULL
    array.release = NULL

    try:
        # Setup in-memory handler if needed
        path, use_tmp_vsimem = get_ogr_vsimem_write_path(path_or_fp, driver)

        layer_created = create_ogr_dataset_layer(
            path, use_tmp_vsimem, layer, driver, crs, geometry_type, encoding,
            dataset_kwargs, layer_kwargs, append,
            dataset_metadata, layer_metadata,
            &ogr_dataset, &ogr_layer,
        )

        # only shapefile supports non-UTF encoding because ENCODING option is set
        # during dataset creation and GDAL auto-translates from UTF-8 values to that
        # encoding
        if (
            encoding and encoding.replace("-", "").upper() != "UTF8"
            and driver != "ESRI Shapefile"
        ):
            raise ValueError(
                "non-UTF-8 encoding is not supported for Arrow; use the non-Arrow "
                "interface instead"
            )

        if geometry_name:
            opts = {"GEOMETRY_NAME": geometry_name}
        else:
            opts = {}

        options = dict_to_options(opts)

        stream_capsule = arrow_obj.__arrow_c_stream__()
        stream = <ArrowArrayStream*>PyCapsule_GetPointer(
            stream_capsule, "arrow_array_stream"
        )

        if stream == NULL:
            raise RuntimeError("Could not extract valid Arrow array stream.")

        if stream.release == NULL:
            raise RuntimeError("Arrow array stream was already released.")

        if stream.get_schema(stream, &schema) != 0:
            raise RuntimeError("Could not get Arrow schema from stream.")

        if layer_created:
            create_fields_from_arrow_schema(ogr_layer, &schema, options, geometry_name)

        while True:
            if stream.get_next(stream, &array) != 0:
                raise RuntimeError("Error while accessing batch from stream.")

            # We've reached the end of the stream
            if array.release == NULL:
                break

            if not OGR_L_WriteArrowBatch(ogr_layer, &schema, &array, options):
                exc = check_last_error()
                gdal_msg = f": {str(exc)}" if exc else "."
                raise DataLayerError(
                    f"Error while writing batch to OGR layer{gdal_msg}"
                )

            if array.release != NULL:
                array.release(&array)

        # close dataset to force driver to flush data
        exc = ogr_close(ogr_dataset)
        ogr_dataset = NULL
        if exc:
            raise DataSourceError(f"Failed to write features to dataset {path}; {exc}")

        # copy in-memory file back to path_or_fp object
        if use_tmp_vsimem:
            read_vsimem_to_buffer(path, path_or_fp)

    finally:
        if stream != NULL and stream.release != NULL:
            stream.release(stream)

        if schema.release != NULL:
            schema.release(&schema)

        if array.release != NULL:
            array.release(&array)

        if options != NULL:
            CSLDestroy(options)
            options = NULL

        if ogr_dataset != NULL:
            ogr_close(ogr_dataset)

        if use_tmp_vsimem:
            vsimem_rmtree_toplevel(path)


cdef get_arrow_extension_metadata(const ArrowSchema* schema):
    """
    Parse the metadata of the ArrowSchema and extract extension type
    metadata (extension name and metadata).

    For the exact layout of the bytes, see
    https://arrow.apache.org/docs/dev/format/CDataInterface.html#c.ArrowSchema.metadata
    """
    cdef const char *metadata = schema.metadata

    extension_name = None
    extension_metadata = None

    if metadata == NULL:
        return extension_name, extension_metadata

    # the number of metadata key/value pairs is stored
    # as an int32 value in the first 4 bytes
    n = int.from_bytes(metadata[:4], byteorder=sys.byteorder)
    pos = 4

    for i in range(n):
        # for each metadata key/value pair, the first 4 bytes is the byte length
        # of the key as an int32, then follows the key (not null-terminated),
        # and then the same for the value length and bytes
        key_length = int.from_bytes(
            metadata[pos:pos+4], byteorder=sys.byteorder, signed=True
        )
        pos += 4
        key = metadata[pos:pos+key_length]
        pos += key_length
        value_length = int.from_bytes(
            metadata[pos:pos+4], byteorder=sys.byteorder, signed=True
        )
        pos += 4
        value = metadata[pos:pos+value_length]
        pos += value_length

        if key == b"ARROW:extension:name":
            extension_name = value
        elif key == b"ARROW:extension:metadata":
            extension_metadata = value

        if extension_name is not None and extension_metadata is not None:
            break

    return extension_name, extension_metadata


cdef is_arrow_geometry_field(const ArrowSchema* schema):
    name, _ = get_arrow_extension_metadata(schema)
    if name is not None:
        if name == b"geoarrow.wkb" or name == b"ogc.wkb":
            return True

        # raise an error for other geoarrow types
        if name.startswith(b"geoarrow."):
            raise NotImplementedError(
                f"Writing a geometry column of type {name.decode()} is not yet "
                "supported. Only WKB is currently supported ('geoarrow.wkb' or "
                "'ogc.wkb' types)."
            )

    return False


cdef create_fields_from_arrow_schema(
    OGRLayerH destLayer, const ArrowSchema* schema, char** options, str geometry_name
):
    """Create output fields using CreateFieldFromArrowSchema()"""

    IF CTE_GDAL_VERSION < 30800:
        raise RuntimeError("Need GDAL>=3.8 for Arrow write support")

    # Some formats store the FID explicitly in a real column, e.g. GPKG.
    # For those formats, OGR_L_GetFIDColumn will return the column name used
    # for this and otherwise it returns "". GDAL typically also provides a
    # layer creation option to overrule the column name to be used as FID
    # column. When a column with the appropriate name is present in the data,
    # GDAL will automatically use it for the FID. Reference:
    # https://gdal.org/en/stable/tutorials/vector_api_tut.html#writing-to-ogr-using-the-arrow-c-data-interface
    # Hence, the column should not be created as an ordinary field as well.
    # Doing so triggers a bug in GDAL < 3.10.1:
    # https://github.com/OSGeo/gdal/issues/11527#issuecomment-2556092722
    fid_column = get_string(OGR_L_GetFIDColumn(destLayer))

    # The schema object is a struct type where each child is a column.
    cdef ArrowSchema* child
    for i in range(schema.n_children):
        child = schema.children[i]

        if child == NULL:
            raise RuntimeError("Received invalid Arrow schema (null child)")

        # Don't create property for geometry column
        if get_string(child.name) == geometry_name or is_arrow_geometry_field(child):
            continue

        # Don't create property for column that will already be used as FID
        # Note: it seems that GDAL initially uses a case-sensitive check of the
        # FID column, but then falls back to case-insensitive matching via
        # the "ordinary" field being added. So, the check here needs to be
        # case-sensitive so the column is still added as regular column if the
        # casing isn't matched, otherwise the column is simply "lost".
        # Note2: in the non-arrow path, the FID column is also treated
        # case-insensitive, so this is consistent with that.
        if fid_column != "" and get_string(child.name) == fid_column:
            continue

        if not OGR_L_CreateFieldFromArrowSchema(destLayer, child, options):
            exc = check_last_error()
            gdal_msg = f" ({str(exc)})" if exc else ""
            raise FieldError(
                f"Error while creating field from Arrow for field {i} with name "
                f"'{get_string(child.name)}' and type {get_string(child.format)}"
                f"{gdal_msg}."
            )
