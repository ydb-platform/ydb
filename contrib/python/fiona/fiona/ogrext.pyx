"""Extension classes and functions using the OGR C API."""

include "gdal.pxi"

import datetime
import json
import locale
import logging
import os
import warnings
import math
from collections import namedtuple
from typing import List
from uuid import uuid4

from fiona.crs cimport CRS, osr_set_traditional_axis_mapping_strategy
from fiona._geometry cimport (
    GeomBuilder, OGRGeomBuilder, geometry_type_code,
    normalize_geometry_type_code, base_geometry_type_code)
from fiona._err cimport exc_wrap_int, exc_wrap_pointer, exc_wrap_vsilfile, get_last_error_msg
from fiona._err cimport StackChecker

import fiona
from fiona._env import get_gdal_version_num, calc_gdal_version_num, get_gdal_version_tuple
from fiona._err import (
    cpl_errs, stack_errors, FionaNullPointerError, CPLE_BaseError, CPLE_AppDefinedError,
    CPLE_OpenFailedError)
from fiona._geometry import GEOMETRY_TYPES
from fiona import compat
from fiona.compat import strencode
from fiona.env import Env
from fiona.errors import (
    DriverError, DriverIOError, SchemaError, CRSError, FionaValueError,
    TransactionError, GeometryTypeValidationError, DatasetDeleteError,
    AttributeFilterError, FeatureWarning, FionaDeprecationWarning, UnsupportedGeometryTypeError)
from fiona.model import decode_object, Feature, Geometry, Properties
from fiona._path import _vsi_path
from fiona.rfc3339 import parse_date, parse_datetime, parse_time
from fiona.schema import FIELD_TYPES_MAP2, normalize_field_type, NAMED_FIELD_TYPES

from libc.stdlib cimport malloc, free
from libc.string cimport strcmp
from cpython cimport PyBytes_FromStringAndSize, PyBytes_AsString
from fiona.drvsupport import _driver_supports_timezones


log = logging.getLogger(__name__)

DEFAULT_TRANSACTION_SIZE = 20000

# OGR Driver capability
cdef const char * ODrCCreateDataSource = "CreateDataSource"
cdef const char * ODrCDeleteDataSource = "DeleteDataSource"

# OGR Layer capability
cdef const char * OLC_RANDOMREAD = "RandomRead"
cdef const char * OLC_SEQUENTIALWRITE = "SequentialWrite"
cdef const char * OLC_RANDOMWRITE = "RandomWrite"
cdef const char * OLC_FASTSPATIALFILTER = "FastSpatialFilter"
cdef const char * OLC_FASTFEATURECOUNT = "FastFeatureCount"
cdef const char * OLC_FASTGETEXTENT = "FastGetExtent"
cdef const char * OLC_FASTSETNEXTBYINDEX = "FastSetNextByIndex"
cdef const char * OLC_CREATEFIELD = "CreateField"
cdef const char * OLC_CREATEGEOMFIELD = "CreateGeomField"
cdef const char * OLC_DELETEFIELD = "DeleteField"
cdef const char * OLC_REORDERFIELDS = "ReorderFields"
cdef const char * OLC_ALTERFIELDDEFN = "AlterFieldDefn"
cdef const char * OLC_DELETEFEATURE = "DeleteFeature"
cdef const char * OLC_STRINGSASUTF8 = "StringsAsUTF8"
cdef const char * OLC_TRANSACTIONS = "Transactions"
cdef const char * OLC_IGNOREFIELDS =  "IgnoreFields"

# OGR integer error types.

OGRERR_NONE = 0
OGRERR_NOT_ENOUGH_DATA = 1    # not enough data to deserialize */
OGRERR_NOT_ENOUGH_MEMORY = 2
OGRERR_UNSUPPORTED_GEOMETRY_TYPE = 3
OGRERR_UNSUPPORTED_OPERATION = 4
OGRERR_CORRUPT_DATA = 5
OGRERR_FAILURE = 6
OGRERR_UNSUPPORTED_SRS = 7
OGRERR_INVALID_HANDLE = 8


cdef void gdal_flush_cache(void *cogr_ds):
    with cpl_errs:
        GDALFlushCache(cogr_ds)


cdef void* gdal_open_vector(const char* path_c, int mode, drivers, options) except NULL:
    cdef void* cogr_ds = NULL
    cdef char **drvs = NULL
    cdef void* drv = NULL
    cdef char **open_opts = NULL
    cdef char **registered_prefixes = NULL
    cdef int prefix_index = 0
    cdef VSIFilesystemPluginCallbacksStruct *callbacks_struct = NULL
    cdef StackChecker checker

    flags = GDAL_OF_VECTOR | GDAL_OF_VERBOSE_ERROR
    if mode == 1:
        flags |= GDAL_OF_UPDATE
    else:
        flags |= GDAL_OF_READONLY

    if drivers:
        for name in drivers:
            name_b = name.encode()
            name_c = name_b
            drv = GDALGetDriverByName(name_c)
            if drv != NULL:
                drvs = CSLAddString(drvs, name_c)

    for k, v in options.items():

        if v is not None:
            kb = k.upper().encode('utf-8')

            if isinstance(v, bool):
                vb = ('ON' if v else 'OFF').encode('utf-8')
            else:
                vb = str(v).encode('utf-8')

            open_opts = CSLAddNameValue(open_opts, <const char *>kb, <const char *>vb)

    open_opts = CSLAddNameValue(open_opts, "VALIDATE_OPEN_OPTIONS", "NO")

    try:
        with stack_errors() as checker:
            cogr_ds = GDALOpenEx(
                path_c, flags, <const char *const *>drvs, <const char *const *>open_opts, NULL
            )
            return checker.exc_wrap_pointer(cogr_ds)
    except CPLE_BaseError as exc:
        raise DriverError(f"Failed to open dataset (flags={flags}): {path_c.decode('utf-8')}") from exc
    finally:
        CSLDestroy(drvs)
        CSLDestroy(open_opts)


cdef void* gdal_create(void* cogr_driver, const char *path_c, options) except NULL:
    cdef char **creation_opts = NULL
    cdef void *cogr_ds = NULL

    db = <const char *>GDALGetDriverShortName(cogr_driver)

    # To avoid a circular import.
    from fiona import meta

    option_keys = set(key.upper() for key in options.keys())
    creation_option_keys = option_keys & set(meta.dataset_creation_options(db.decode("utf-8")))

    for k, v in options.items():
        if k.upper() in creation_option_keys:
            kb = k.upper().encode('utf-8')

            if isinstance(v, bool):
                vb = ('ON' if v else 'OFF').encode('utf-8')
            else:
                vb = str(v).encode('utf-8')

            creation_opts = CSLAddNameValue(creation_opts, <const char *>kb, <const char *>vb)

    try:
        return exc_wrap_pointer(GDALCreate(cogr_driver, path_c, 0, 0, 0, GDT_Unknown, creation_opts))
    except FionaNullPointerError:
        raise DriverError(f"Failed to create dataset: {path_c.decode('utf-8')}")
    except CPLE_BaseError as exc:
        raise DriverError(str(exc))
    finally:
        CSLDestroy(creation_opts)


def _explode(coords):
    """Explode a GeoJSON geometry's coordinates object and yield
    coordinate tuples. As long as the input is conforming, the type of
    the geometry doesn't matter."""
    for e in coords:
        if isinstance(e, (float, int)):
            yield coords
            break
        else:
            for f in _explode(e):
                yield f


def _bounds(geometry):
    """Bounding box of a GeoJSON geometry"""
    try:
        xyz = tuple(zip(*list(_explode(geometry['coordinates']))))
        return min(xyz[0]), min(xyz[1]), max(xyz[0]), max(xyz[1])
    except (KeyError, TypeError):
        return None


cdef int GDAL_VERSION_NUM = get_gdal_version_num()


class TZ(datetime.tzinfo):

    def __init__(self, minutes):
        self.minutes = minutes

    def utcoffset(self, dt):
        return datetime.timedelta(minutes=self.minutes)


cdef class AbstractField:

    cdef object driver
    cdef object supports_tz

    def __init__(self, driver=None):
        self.driver = driver

    cdef object name(self, OGRFieldDefnH fdefn):
        """Get the short Fiona field name corresponding to the OGR field definition."""
        raise NotImplementedError

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        """Get the value of a feature's field."""
        raise NotImplementedError

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        """Set the value of a feature's field."""
        raise NotImplementedError


cdef class IntegerField(AbstractField):

    cdef object name(self, OGRFieldDefnH fdefn):
        cdef int width = OGR_Fld_GetWidth(fdefn)
        fmt = ""
        if width:
            fmt = f":{width:d}"
        return f"int32{fmt}"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        return OGR_F_GetFieldAsInteger(feature, i)

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        OGR_F_SetFieldInteger(feature, i, int(value))


cdef class Int16Field(AbstractField):

    cdef object name(self, OGRFieldDefnH fdefn):
        cdef int width = OGR_Fld_GetWidth(fdefn)
        fmt = ""
        if width:
            fmt = f":{width:d}"
        return f"int16{fmt}"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        return OGR_F_GetFieldAsInteger(feature, i)

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        OGR_F_SetFieldInteger(feature, i, int(value))


cdef class BooleanField(AbstractField):

    cdef object name(self, OGRFieldDefnH fdefn):
        return "bool"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        return bool(OGR_F_GetFieldAsInteger(feature, i))

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        OGR_F_SetFieldInteger(feature, i, int(value))


cdef class Integer64Field(AbstractField):

    cdef object name(self, OGRFieldDefnH fdefn):
        cdef int width = OGR_Fld_GetWidth(fdefn)
        fmt = ""
        if width:
            fmt = f":{width:d}"
        return f"int{fmt}"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        return OGR_F_GetFieldAsInteger64(feature, i)

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        OGR_F_SetFieldInteger64(feature, i, int(value))


cdef class RealField(AbstractField):

    cdef object name(self, OGRFieldDefnH fdefn):
        cdef int width = OGR_Fld_GetWidth(fdefn)
        cdef int precision = OGR_Fld_GetPrecision(fdefn)
        fmt = ""
        if width:
            fmt = f":{width:d}"
        if precision:
            fmt += f".{precision:d}"
        return f"float{fmt}"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        return OGR_F_GetFieldAsDouble(feature, i)

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        OGR_F_SetFieldDouble(feature, i, float(value))


cdef class StringField(AbstractField):

    cdef object name(self, OGRFieldDefnH fdefn):
        cdef int width = OGR_Fld_GetWidth(fdefn)
        fmt = ""
        if width:
            fmt = f":{width:d}"
        return f"str{fmt}"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        encoding = kwds["encoding"]
        val = OGR_F_GetFieldAsString(feature, i)
        try:
            val = val.decode(encoding)
        except UnicodeDecodeError:
            log.warning(
                "Failed to decode %s using %s codec", val, encoding)
        else:
            return val

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        encoding = kwds["encoding"]
        cdef object value_b = str(value).encode(encoding)
        OGR_F_SetFieldString(feature, i, <const char *>value_b)


cdef class BinaryField(AbstractField):

    cdef object name(self, OGRFieldDefnH fdefn):
        return "bytes"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        cdef unsigned char *data = NULL
        cdef int l
        data = OGR_F_GetFieldAsBinary(feature, i, &l)
        return data[:l]

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        OGR_F_SetFieldBinary(feature, i, len(value), <const unsigned char *>value)


cdef class StringListField(AbstractField):

    cdef object name(self, OGRFieldDefnH fdefn):
        return "List[str]"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        cdef char **string_list = NULL
        encoding = kwds["encoding"]
        string_list = OGR_F_GetFieldAsStringList(feature, i)
        string_list_index = 0
        vals = []
        if string_list != NULL:
            while string_list[string_list_index] != NULL:
                val = string_list[string_list_index]
                try:
                    val = val.decode(encoding)
                except UnicodeDecodeError:
                    log.warning(
                        "Failed to decode %s using %s codec", val, encoding
                    )
                vals.append(val)
                string_list_index += 1
        return vals

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        cdef char **string_list = NULL
        encoding = kwds["encoding"]
        for item in value:
            item_b = item.encode(encoding)
            string_list = CSLAddString(string_list, <const char *>item_b)
        OGR_F_SetFieldStringList(feature, i, <CSLConstList>string_list)


cdef class JSONField(AbstractField):

    cdef object name(self, OGRFieldDefnH fdefn):
        return "json"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        val = OGR_F_GetFieldAsString(feature, i)
        return json.loads(val)

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        value_b = json.dumps(value).encode("utf-8")
        OGR_F_SetFieldString(feature, i, <const char *>value_b)


cdef class DateField(AbstractField):
    """Dates without time."""

    cdef object name(self, OGRFieldDefnH fdefn):
        return "date"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        cdef int retval
        cdef int y = 0
        cdef int m = 0
        cdef int d = 0
        cdef int hh = 0
        cdef int mm = 0
        cdef float fss = 0.0
        cdef int tz = 0
        retval = OGR_F_GetFieldAsDateTimeEx(feature, i, &y, &m, &d, &hh, &mm, &fss, &tz)
        return datetime.date(y, m, d).isoformat()

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        if isinstance(value, str):
            y, m, d, hh, mm, ss, ms, tz = parse_date(value)
        elif isinstance(value, datetime.date):
            y, m, d = value.year, value.month, value.day
            hh = mm = ss = ms = 0
        else:
            raise ValueError()

        tzinfo = 0
        OGR_F_SetFieldDateTimeEx(feature, i, y, m, d, hh, mm, ss, tzinfo)


cdef class TimeField(AbstractField):
    """Times without dates."""

    def __init__(self, driver=None):
        self.driver = driver
        self.supports_tz = _driver_supports_timezones(self.driver, "time")

    cdef object name(self, OGRFieldDefnH fdefn):
        return "time"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        cdef int retval
        cdef int y = 0
        cdef int m = 0
        cdef int d = 0
        cdef int hh = 0
        cdef int mm = 0
        cdef float fss = 0.0
        cdef int tz = 0
        retval = OGR_F_GetFieldAsDateTimeEx(feature, i, &y, &m, &d, &hh, &mm, &fss, &tz)
        ms, ss = math.modf(fss)
        ss = int(ss)
        ms = int(round(ms * 10**6))
        # OGR_F_GetFieldAsDateTimeEx: (0=unknown, 1=localtime, 100=GMT, see data model for details)
        # CPLParseRFC822DateTime: (0=unknown, 100=GMT, 101=GMT+15minute, 99=GMT-15minute), or NULL
        tzinfo = None
        if tz > 1:
            tz_minutes = (tz - 100) * 15
            tzinfo = TZ(tz_minutes)
        return datetime.time(hh, mm, ss, ms, tzinfo).isoformat()

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        if isinstance(value, str):
            y, m, d, hh, mm, ss, ms, tz = parse_time(value)
        elif isinstance(value, datetime.time):
            y = m = d = 0
            hh, mm, ss, ms = value.hour, value.minute, value.second, value.microsecond
            if value.utcoffset() is None:
                tz = None
            else:
                tz = value.utcoffset().total_seconds() / 60
        else:
            raise ValueError()

        if tz is not None and not self.supports_tz:
            d_tz = datetime.datetime(1900, 1, 1, hh, mm, ss, int(ms), TZ(tz))
            d_utc = d_tz - d_tz.utcoffset()
            y = m = d = 0
            hh, mm, ss, ms = d_utc.hour, d_utc.minute, d_utc.second, d_utc.microsecond
            tz = 0

        # tzinfo: (0=unknown, 100=GMT, 101=GMT+15minute, 99=GMT-15minute), or NULL
        if tz is not None:
            tzinfo = int(tz / 15.0 + 100)
        else:
            tzinfo = 0

        ss += ms / 10**6
        OGR_F_SetFieldDateTimeEx(feature, i, y, m, d, hh, mm, ss, tzinfo)


cdef class DateTimeField(AbstractField):
    """Dates and times."""

    def __init__(self, driver=None):
        self.driver = driver
        self.supports_tz = _driver_supports_timezones(self.driver, "datetime")

    cdef object name(self, OGRFieldDefnH fdefn):
        return "datetime"

    cdef object get(self, OGRFeatureH feature, int i, object kwds):
        cdef int retval
        cdef int y = 0
        cdef int m = 0
        cdef int d = 0
        cdef int hh = 0
        cdef int mm = 0
        cdef float fss = 0.0
        cdef int tz = 0
        retval = OGR_F_GetFieldAsDateTimeEx(feature, i, &y, &m, &d, &hh, &mm, &fss, &tz)
        ms, ss = math.modf(fss)
        ss = int(ss)
        ms = int(round(ms * 10**6))
        # OGR_F_GetFieldAsDateTimeEx: (0=unknown, 1=localtime, 100=GMT, see data model for details)
        # CPLParseRFC822DateTime: (0=unknown, 100=GMT, 101=GMT+15minute, 99=GMT-15minute), or NULL
        tzinfo = None
        if tz > 1:
            tz_minutes = (tz - 100) * 15
            tzinfo = TZ(tz_minutes)
        return datetime.datetime(y, m, d, hh, mm, ss, ms, tzinfo).isoformat()

    cdef set(self, OGRFeatureH feature, int i, object value, object kwds):
        if isinstance(value, str):
            y, m, d, hh, mm, ss, ms, tz = parse_datetime(value)
        elif isinstance(value, datetime.datetime):
            y, m, d = value.year, value.month, value.day
            hh, mm, ss, ms = value.hour, value.minute, value.second, value.microsecond
            if value.utcoffset() is None:
                tz = None
            else:
                tz = value.utcoffset().total_seconds() / 60
        else:
            raise ValueError()

        if tz is not None and not self.supports_tz:
            d_tz = datetime.datetime(y, m, d, hh, mm, ss, int(ms), TZ(tz))
            d_utc = d_tz - d_tz.utcoffset()
            y, m, d = d_utc.year, d_utc.month, d_utc.day
            hh, mm, ss, ms = d_utc.hour, d_utc.minute, d_utc.second, d_utc.microsecond
            tz = 0

        # tzinfo: (0=unknown, 100=GMT, 101=GMT+15minute, 99=GMT-15minute), or NULL
        if tz is not None:
            tzinfo = int(tz / 15.0 + 100)
        else:
            tzinfo = 0

        ss += ms / 10**6
        OGR_F_SetFieldDateTimeEx(feature, i, y, m, d, hh, mm, ss, tzinfo)


cdef bint is_field_null(OGRFeatureH feature, int i):
    return OGR_F_IsFieldNull(feature, i) or not OGR_F_IsFieldSet(feature, i)


cdef class FeatureBuilder:
    """Build Fiona features from OGR feature pointers.

    No OGR objects are allocated by this function and the feature
    argument is not destroyed.
    """

    cdef object driver
    cdef object property_getter_cache

    OGRPropertyGetter = {
        (OFTInteger, OFSTNone): IntegerField,
        (OFTInteger, OFSTBoolean): BooleanField,
        (OFTInteger, OFSTInt16): Int16Field,
        (OFTInteger64, OFSTNone): Integer64Field,
        (OFTReal, OFSTNone): RealField,
        (OFTString, OFSTNone): StringField,
        (OFTDate, OFSTNone): DateField,
        (OFTTime, OFSTNone): TimeField,
        (OFTDateTime, OFSTNone): DateTimeField,
        (OFTBinary, OFSTNone): BinaryField,
        (OFTStringList, OFSTNone): StringListField,
        (OFTString, OFSTJSON): JSONField,
    }

    def __init__(self, driver=None):
        self.driver = driver
        self.property_getter_cache = {}

    cdef build(
        self,
        OGRFeatureH feature,
        encoding='utf-8',
        bbox=False,
        driver=None,
        ignore_fields=None,
        ignore_geometry=False
    ):
        """Build a Fiona feature object from an OGR feature

        Parameters
        ----------
        feature : void *
            The OGR feature # TODO: use a real typedef
        encoding : str
            The encoding of OGR feature attributes
        bbox : bool
            Not used
        driver : str
            OGR format driver name like 'GeoJSON'
        ignore_fields : sequence
            A sequence of field names that will be ignored and omitted
            in the Fiona feature properties
        ignore_geometry : bool
            Flag for whether the OGR geometry field is to be ignored

        Returns
        -------
        dict
        """
        cdef OGRFieldDefnH fdefn
        cdef int i
        cdef int fieldtype
        cdef int fieldsubtype
        cdef const char *key_c
        cdef AbstractField getter

        # Skeleton of the feature to be returned.
        fid = OGR_F_GetFID(feature)
        props = {}

        ignore_fields = set(ignore_fields or [])

        for i in range(OGR_F_GetFieldCount(feature)):
            fdefn = OGR_F_GetFieldDefnRef(feature, i)
            if fdefn == NULL:
                raise ValueError(f"NULL field definition at index {i}")

            key_c = OGR_Fld_GetNameRef(fdefn)
            if key_c == NULL:
                raise ValueError(f"NULL field name reference at index {i}")

            key_b = key_c
            key = key_b.decode(encoding)

            # Some field names are empty strings, apparently.
            # We warn in this case.
            if not key:
                warnings.warn(f"Empty field name at index {i}")

            if key in ignore_fields:
                continue

            fieldtype = OGR_Fld_GetType(fdefn)
            fieldsubtype = OGR_Fld_GetSubType(fdefn)
            fieldkey = (fieldtype, fieldsubtype)

            if is_field_null(feature, i):
                props[key] = None
            else:
                if fieldkey in self.property_getter_cache:
                    getter = self.property_getter_cache[fieldkey]
                else:
                    try:
                        getter = self.OGRPropertyGetter[fieldkey](driver=driver or self.driver)
                        self.property_getter_cache[fieldkey] = getter
                    except KeyError:
                        log.warning(
                            "Skipping field %s: invalid type %s",
                            key,
                            fieldkey
                        )
                        continue

                props[key] = getter.get(feature, i, {"encoding": encoding})

        cdef void *cogr_geometry = NULL
        geom = None

        if not ignore_geometry:
            cogr_geometry = OGR_F_GetGeometryRef(feature)
            geom = GeomBuilder().build_from_feature(feature)

        return Feature(id=str(fid), properties=Properties(**props), geometry=geom)


cdef class OGRFeatureBuilder:

    """Builds an OGR Feature from a Fiona feature mapping.

    Allocates one OGR Feature which should be destroyed by the caller.
    Borrows a layer definition from the collection.

    """

    cdef object driver
    cdef object property_setter_cache

    OGRPropertySetter = {
        (OFTInteger, OFSTNone, "int"): IntegerField,
        (OFTInteger, OFSTNone, "int32"): IntegerField,
        (OFTInteger, OFSTNone, "float"): RealField,
        (OFTInteger, OFSTNone, "str"): StringField,
        (OFTInteger, OFSTBoolean, "bool"): BooleanField,
        (OFTInteger, OFSTBoolean, "int"): BooleanField,
        (OFTInteger, OFSTInt16, "int"): Int16Field,
        (OFTInteger, OFSTInt16, "str"): StringField,
        (OFTInteger64, OFSTNone, "int"): Integer64Field,
        (OFTInteger64, OFSTNone, "int64"): Integer64Field,
        (OFTInteger64, OFSTNone, "float"): RealField,
        (OFTInteger64, OFSTNone, "str"): StringField,
        (OFTReal, OFSTNone, "float"): RealField,
        (OFTReal, OFSTNone, "str"): StringField,
        (OFTReal, OFSTFloat32, "float"): RealField,
        (OFTReal, OFSTFloat32, "float32"): RealField,
        (OFTReal, OFSTFloat32, "str"): StringField,
        (OFTString, OFSTNone, "str"): StringField,
        (OFTString, OFSTNone, "dict"): StringField,
        (OFTDate, OFSTNone, "date"): DateField,
        (OFTDate, OFSTNone, "str"): DateField,
        (OFTTime, OFSTNone, "time"): TimeField,
        (OFTTime, OFSTNone, "str"): TimeField,
        (OFTDateTime, OFSTNone, "datetime"): DateTimeField,
        (OFTDateTime, OFSTNone, "str"): DateTimeField,
        (OFTBinary, OFSTNone, "bytes"): BinaryField,
        (OFTBinary, OFSTNone, "bytearray"): BinaryField,
        (OFTBinary, OFSTNone, "memoryview"): BinaryField,
        (OFTStringList, OFSTNone, "list"): StringListField,
        (OFTString, OFSTJSON, "dict"): JSONField,
        (OFTString, OFSTJSON, "list"): JSONField,
    }

    def __init__(self, driver=None):
        self.driver = driver
        self.property_setter_cache = {}

    cdef OGRFeatureH build(self, feature, collection) except NULL:
        cdef void *cogr_geometry = NULL
        cdef const char *string_c = NULL
        cdef WritingSession session = collection.session
        cdef void *cogr_layer = session.cogr_layer
        cdef void *cogr_featuredefn = OGR_L_GetLayerDefn(cogr_layer)
        cdef void *cogr_feature = OGR_F_Create(cogr_featuredefn)
        cdef AbstractField setter

        if cogr_layer == NULL:
            raise ValueError("Null layer")

        if cogr_featuredefn == NULL:
            raise ValueError("Null feature definition")

        if cogr_feature == NULL:
            raise ValueError("Null feature")

        if feature.geometry is not None:
            cogr_geometry = OGRGeomBuilder().build(feature.geometry)
            exc_wrap_int(OGR_F_SetGeometryDirectly(cogr_feature, cogr_geometry))

        encoding = session._get_internal_encoding()

        for key, value in feature.properties.items():
            i = session._schema_mapping_index[key]

            if i < 0:
                continue

            if value is None:
                OGR_F_SetFieldNull(cogr_feature, i)
            else:
                schema_type = session._schema_normalized_field_types[key]
                val_type = type(value)

                if val_type in self.property_setter_cache:
                    setter = self.property_setter_cache[val_type]
                else:
                    for cls in val_type.mro():
                        fieldkey = (*FIELD_TYPES_MAP2[NAMED_FIELD_TYPES[schema_type]], cls.__name__)
                        try:
                            setter = self.OGRPropertySetter[fieldkey](driver=self.driver)
                        except KeyError:
                            continue
                        else:
                            self.property_setter_cache[val_type] = setter
                            break
                    else:
                        log.warning("Skipping field because of invalid value: key=%r, value=%r", key, value)
                        continue

                # Special case: serialize dicts to assist OGR.
                if isinstance(value, dict):
                    value = json.dumps(value)

                log.debug("Setting feature property: key=%r, value=%r, i=%r, setter=%r", key, value, i, setter)

                setter.set(cogr_feature, i, value, {"encoding": encoding})

        return cogr_feature


cdef _deleteOgrFeature(void *cogr_feature):
    """Delete an OGR feature"""
    if cogr_feature is not NULL:
        OGR_F_Destroy(cogr_feature)
    cogr_feature = NULL


def featureRT(feat, collection):
    # For testing purposes only, leaks the JSON data
    feature = decode_object(feat)
    cdef void *cogr_feature = OGRFeatureBuilder().build(feature, collection)
    cdef void *cogr_geometry = OGR_F_GetGeometryRef(cogr_feature)
    if cogr_geometry == NULL:
        raise ValueError("Null geometry")
    result = FeatureBuilder().build(
        cogr_feature,
        encoding='utf-8',
        bbox=False,
        driver=collection.driver
    )
    _deleteOgrFeature(cogr_feature)
    return result


cdef class Session:

    cdef void *cogr_ds
    cdef void *cogr_layer
    cdef object _fileencoding
    cdef object _encoding
    cdef object collection
    cdef bint cursor_interrupted

    OGRFieldGetter = {
        (OFTInteger, OFSTNone): IntegerField,
        (OFTInteger, OFSTBoolean): BooleanField,
        (OFTInteger, OFSTInt16): Int16Field,
        (OFTInteger64, OFSTNone): Integer64Field,
        (OFTReal, OFSTNone): RealField,
        (OFTString, OFSTNone): StringField,
        (OFTDate, OFSTNone): DateField,
        (OFTTime, OFSTNone): TimeField,
        (OFTDateTime, OFSTNone): DateTimeField,
        (OFTBinary, OFSTNone): BinaryField,
        (OFTStringList, OFSTNone): StringListField,
        (OFTString, OFSTJSON): JSONField,
    }

    def __init__(self):
        self.cogr_ds = NULL
        self.cogr_layer = NULL
        self._fileencoding = None
        self._encoding = None
        self.cursor_interrupted = False

    def __dealloc__(self):
        self.stop()

    def start(self, collection, **kwargs):
        cdef const char *path_c = NULL
        cdef const char *name_c = NULL
        cdef void *drv = NULL
        cdef void *ds = NULL
        cdef char **ignore_fields = NULL

        path_b = collection.path.encode('utf-8')
        path_c = path_b

        self._fileencoding = kwargs.get('encoding') or collection.encoding

        # We have two ways of specifying drivers to try. Resolve the
        # values into a single set of driver short names.
        if collection._driver:
            drivers = set([collection._driver])
        elif collection.enabled_drivers:
            drivers = set(collection.enabled_drivers)
        else:
            drivers = None

        encoding = kwargs.pop('encoding', None)
        if encoding:
            kwargs['encoding'] = encoding.upper()

        self.cogr_ds = gdal_open_vector(path_c, 0, drivers, kwargs)

        layername = collection.name

        if isinstance(layername, str):
            layername_b = layername.encode('utf-8')
            self.cogr_layer = GDALDatasetGetLayerByName(self.cogr_ds, <const char *>layername_b)
        elif isinstance(layername, int):
            self.cogr_layer = GDALDatasetGetLayer(self.cogr_ds, <int>layername)

        if self.cogr_layer == NULL:
            raise ValueError(f"Null layer: {collection} {layername}")
        else:
            name_c = OGR_L_GetName(self.cogr_layer)
            name_b = name_c
            collection.name = name_b.decode('utf-8')

        encoding = self._get_internal_encoding()

        if collection.ignore_fields or collection.include_fields is not None:
            if not OGR_L_TestCapability(self.cogr_layer, OLC_IGNOREFIELDS):
                raise DriverError("Driver does not support ignore_fields")

        self.collection = collection

        if self.collection.include_fields is not None:
            self.collection.ignore_fields = list(
                set(self.get_schema()["properties"]) - set(collection.include_fields)
            )

        if self.collection.ignore_fields:
            try:
                for name in self.collection.ignore_fields:
                    try:
                        name_b = name.encode(encoding)
                    except AttributeError:
                        raise TypeError(
                            f'Ignored field "{name}" has type '
                            f'"{name.__class__.__name__}", expected string'
                        )
                    else:
                        ignore_fields = CSLAddString(ignore_fields, <const char *>name_b)

                OGR_L_SetIgnoredFields(self.cogr_layer, <const char**>ignore_fields)

            finally:
                CSLDestroy(ignore_fields)

    cpdef stop(self):
        self.cogr_layer = NULL
        if self.cogr_ds != NULL:
            try:
                with cpl_errs:
                    GDALClose(self.cogr_ds)
            except CPLE_BaseError as exc:
                raise DriverError(str(exc))
            finally:
                self.cogr_ds = NULL

    def get_fileencoding(self):
        """DEPRECATED"""
        warnings.warn("get_fileencoding is deprecated and will be removed in a future version.", FionaDeprecationWarning)
        return self._fileencoding

    def _get_fallback_encoding(self):
        """Determine a format-specific fallback encoding to use when using OGR_F functions

        Parameters
        ----------
        None

        Returns
        -------
        str

        """
        if "Shapefile" in self.get_driver():
            return 'iso-8859-1'
        else:
            return locale.getpreferredencoding()


    def _get_internal_encoding(self):
        """Determine the encoding to use when use OGR_F functions

        Parameters
        ----------
        None

        Returns
        -------
        str

        Notes
        -----
        If the layer implements RFC 23 support for UTF-8, the return
        value will be 'utf-8' and callers can be certain that this is
        correct.  If the layer does not have the OLC_STRINGSASUTF8
        capability marker, it is not possible to know exactly what the
        internal encoding is and this method returns best guesses. That
        means ISO-8859-1 for shapefiles and the locale's preferred
        encoding for other formats such as CSV files.

        """
        if OGR_L_TestCapability(self.cogr_layer, OLC_STRINGSASUTF8):
            return 'utf-8'
        else:
            return self._fileencoding or self._get_fallback_encoding()

    def get_length(self):
        if self.cogr_layer == NULL:
            raise ValueError("Null layer")
        return self._get_feature_count(0)

    def get_driver(self):
        cdef void *cogr_driver = GDALGetDatasetDriver(self.cogr_ds)
        if cogr_driver == NULL:
            raise ValueError("Null driver")
        cdef const char *name = OGR_Dr_GetName(cogr_driver)
        driver_name = name
        return driver_name.decode()

    def get_schema(self):
        """Get a dictionary representation of a collection's schema.

        The schema dict contains "geometry" and "properties" items.

        Returns
        -------
        dict

        Warnings
        --------
        Fiona 1.9 does not support multiple fields with the name
        name. When encountered, a warning message is logged and the
        field is skipped.

        """
        cdef int i
        cdef int num_fields
        cdef OGRFeatureDefnH featuredefn = NULL
        cdef OGRFieldDefnH fielddefn = NULL
        cdef const char *key_c
        cdef AbstractField getter

        props = {}

        if self.cogr_layer == NULL:
            raise ValueError("Null layer")

        if self.collection.ignore_fields:
            ignore_fields = self.collection.ignore_fields
        else:
            ignore_fields = set()

        featuredefn = OGR_L_GetLayerDefn(self.cogr_layer)

        if featuredefn == NULL:
            raise ValueError("Null feature definition")

        encoding = self._get_internal_encoding()
        num_fields = OGR_FD_GetFieldCount(featuredefn)

        for i from 0 <= i < num_fields:
            fielddefn = OGR_FD_GetFieldDefn(featuredefn, i)

            if fielddefn == NULL:
                raise ValueError(f"NULL field definition at index {i}")

            key_c = OGR_Fld_GetNameRef(fielddefn)

            if key_c == NULL:
                raise ValueError(f"NULL field name reference at index {i}")

            key_b = key_c
            key = key_b.decode(encoding)

            if not key:
                warnings.warn(f"Empty field name at index {i}", FeatureWarning)

            if key in ignore_fields:
                continue

            # See gh-1178 for an example of a pathological collection
            # with multiple identically name fields.
            if key in props:
                log.warning(
                    "Field name collision detected, field is skipped: i=%r, key=%r",
                    i,
                    key
                )
                continue

            fieldtype = OGR_Fld_GetType(fielddefn)
            fieldsubtype = OGR_Fld_GetSubType(fielddefn)
            fieldkey = (fieldtype, fieldsubtype)

            try:
                getter = self.OGRFieldGetter[fieldkey](driver=self.collection.driver)
                props[key] = getter.name(fielddefn)
            except KeyError:
                log.warning(
                    "Skipping field %s: invalid type %s",
                    key,
                    fieldkey
                )
                continue

        ret = {"properties": props}

        if not self.collection.ignore_geometry:
            code = normalize_geometry_type_code(OGR_FD_GetGeomType(featuredefn))
            ret["geometry"] = GEOMETRY_TYPES[code]

        return ret

    def get_crs(self):
        """Get the layer's CRS

        Returns
        -------
        CRS

        """
        wkt = self.get_crs_wkt()
        if not wkt:
            return CRS()
        else:
            return CRS.from_user_input(wkt)

    def get_crs_wkt(self):
        cdef char *proj_c = NULL
        cdef void *cogr_crs = NULL

        if self.cogr_layer == NULL:
            raise ValueError("Null layer")

        try:
            cogr_crs = exc_wrap_pointer(OGR_L_GetSpatialRef(self.cogr_layer))

        # TODO: we don't intend to use try/except for flow control
        # this is a work around for a GDAL issue.
        except FionaNullPointerError:
            log.debug("Layer has no coordinate system")
        except fiona._err.CPLE_OpenFailedError as exc:
            log.debug("A support file wasn't opened. See the preceding ERROR level message.")
            cogr_crs = OGR_L_GetSpatialRef(self.cogr_layer)
            log.debug("Called OGR_L_GetSpatialRef() again without error checking.")
            if cogr_crs == NULL:
                raise exc

        if cogr_crs is not NULL:
            log.debug("Got coordinate system")

            try:
                OSRExportToWkt(cogr_crs, &proj_c)
                if proj_c == NULL:
                    raise ValueError("Null projection")
                proj_b = proj_c
                crs_wkt = proj_b.decode('utf-8')

            finally:
                CPLFree(proj_c)
                return crs_wkt

        else:
            log.debug("Projection not found (cogr_crs was NULL)")
            return ""

    def get_extent(self):
        cdef OGREnvelope extent

        if self.cogr_layer == NULL:
            raise ValueError("Null layer")

        result = OGR_L_GetExtent(self.cogr_layer, &extent, 1)
        self.cursor_interrupted = True
        if result != OGRERR_NONE:
            raise DriverError("Driver was not able to calculate bounds")
        return (extent.MinX, extent.MinY, extent.MaxX, extent.MaxY)

    cdef int _get_feature_count(self, force=0):
        if self.cogr_layer == NULL:
            raise ValueError("Null layer")

        self.cursor_interrupted = True
        return OGR_L_GetFeatureCount(self.cogr_layer, force)

    def has_feature(self, fid):
        """Provides access to feature data by FID.

        Supports Collection.__contains__().
        """
        cdef void * cogr_feature
        fid = int(fid)
        cogr_feature = OGR_L_GetFeature(self.cogr_layer, fid)
        if cogr_feature != NULL:
            _deleteOgrFeature(cogr_feature)
            return True
        else:
            return False

    def get_feature(self, fid):
        """Provides access to feature data by FID.

        Supports Collection.__contains__().
        """
        cdef void * cogr_feature
        fid = int(fid)
        cogr_feature = OGR_L_GetFeature(self.cogr_layer, fid)
        if cogr_feature != NULL:
            feature = FeatureBuilder(driver=self.collection.driver).build(
                cogr_feature,
                encoding=self._get_internal_encoding(),
                bbox=False,
                driver=self.collection.driver,
                ignore_fields=self.collection.ignore_fields,
                ignore_geometry=self.collection.ignore_geometry,
            )
            _deleteOgrFeature(cogr_feature)
            return feature
        else:
            raise KeyError(f"There is no feature with fid {fid!r}")

    get = get_feature

    # TODO: Make this an alias for get_feature in a future version.
    def __getitem__(self, item):
        cdef void * cogr_feature
        if isinstance(item, slice):
            warnings.warn("Collection slicing is deprecated and will be disabled in a future version.", FionaDeprecationWarning)
            itr = Iterator(self.collection, item.start, item.stop, item.step)
            return list(itr)
        elif isinstance(item, int):
            index = item
            # from the back
            if index < 0:
                ftcount = self._get_feature_count(0)
                if ftcount == -1:
                    raise IndexError(
                        "collection's dataset does not support negative indexes")
                index += ftcount
            cogr_feature = OGR_L_GetFeature(self.cogr_layer, index)
            if cogr_feature == NULL:
                return None
            feature = FeatureBuilder(driver=self.collection.driver).build(
                cogr_feature,
                encoding=self._get_internal_encoding(),
                bbox=False,
                driver=self.collection.driver,
                ignore_fields=self.collection.ignore_fields,
                ignore_geometry=self.collection.ignore_geometry,
            )
            _deleteOgrFeature(cogr_feature)
            return feature

    def isactive(self):
        if self.cogr_layer != NULL and self.cogr_ds != NULL:
            return 1
        else:
            return 0


    def tags(self, ns=None):
        """Returns a dict containing copies of the dataset or layers's
        tags. Tags are pairs of key and value strings. Tags belong to
        namespaces.  The standard namespaces are: default (None) and
        'IMAGE_STRUCTURE'.  Applications can create their own additional
        namespaces.

        Parameters
        ----------
        ns: str, optional
            Can be used to select a namespace other than the default.

        Returns
        -------
        dict
        """
        cdef GDALMajorObjectH obj = NULL
        if self.cogr_layer != NULL:
            obj = self.cogr_layer
        else:
            obj = self.cogr_ds

        cdef const char *domain = NULL
        if ns:
            ns = ns.encode('utf-8')
            domain = ns

        cdef char **metadata = NULL
        metadata = GDALGetMetadata(obj, domain)
        num_items = CSLCount(<CSLConstList>metadata)

        return dict(metadata[i].decode('utf-8').split('=', 1) for i in range(num_items))


    def get_tag_item(self, key, ns=None):
        """Returns tag item value

        Parameters
        ----------
        key: str
            The key for the metadata item to fetch.
        ns: str, optional
            Used to select a namespace other than the default.

        Returns
        -------
        str
        """

        key = key.encode('utf-8')
        cdef const char *name = key

        cdef const char *domain = NULL
        if ns:
            ns = ns.encode('utf-8')
            domain = ns

        cdef GDALMajorObjectH obj = NULL
        if self.cogr_layer != NULL:
            obj = self.cogr_layer
        else:
            obj = self.cogr_ds

        cdef const char *value = NULL
        value = GDALGetMetadataItem(obj, name, domain)
        if value == NULL:
            return None
        return value.decode("utf-8")


cdef class WritingSession(Session):

    cdef object _schema_mapping
    cdef object _schema_mapping_index
    cdef object _schema_normalized_field_types

    def start(self, collection, **kwargs):
        cdef OGRSpatialReferenceH cogr_srs = NULL
        cdef char **options = NULL
        cdef char *path_c = NULL
        cdef const char *driver_c = NULL
        cdef const char *name_c = NULL
        cdef const char *proj_c = NULL
        cdef const char *fileencoding_c = NULL
        cdef OGRFieldSubType field_subtype
        cdef int ret
        path = collection.path
        self.collection = collection

        userencoding = kwargs.get('encoding')

        if collection.mode == 'a':

            path_b = strencode(path)
            path_c = path_b
            if not CPLCheckForFile(path_c, NULL):
                raise OSError("No such file or directory %s" % path)

            try:
                self.cogr_ds = gdal_open_vector(path_c, 1, None, kwargs)

                if isinstance(collection.name, str):
                    name_b = collection.name.encode('utf-8')
                    name_c = name_b
                    self.cogr_layer = exc_wrap_pointer(GDALDatasetGetLayerByName(self.cogr_ds, name_c))

                elif isinstance(collection.name, int):
                    self.cogr_layer = exc_wrap_pointer(GDALDatasetGetLayer(self.cogr_ds, collection.name))

            except CPLE_BaseError as exc:
                GDALClose(self.cogr_ds)
                self.cogr_ds = NULL
                self.cogr_layer = NULL
                raise DriverError(str(exc))

            else:
                self._fileencoding = userencoding or self._get_fallback_encoding()

            before_fields = self.get_schema()['properties']

        elif collection.mode == 'w':
            path_b = strencode(path)
            path_c = path_b

            driver_b = collection.driver.encode()
            driver_c = driver_b
            cogr_driver = exc_wrap_pointer(GDALGetDriverByName(driver_c))

            cogr_ds = NULL
            if not CPLCheckForFile(path_c, NULL):
                log.debug("File doesn't exist. Creating a new one...")
                with Env(GDAL_VALIDATE_CREATION_OPTIONS="NO"):
                    cogr_ds = gdal_create(cogr_driver, path_c, kwargs)

            else:
                if collection.driver == "GeoJSON":
                    # We must manually remove geojson files as GDAL doesn't do this for us.
                    log.debug("Removing GeoJSON file")
                    if path.startswith("/vsi"):
                        VSIUnlink(path_c)
                    else:
                        os.unlink(path)
                    with Env(GDAL_VALIDATE_CREATION_OPTIONS="NO"):
                        cogr_ds = gdal_create(cogr_driver, path_c, kwargs)

                else:
                    try:
                        # Attempt to open existing dataset in write mode,
                        # letting GDAL/OGR handle the overwriting.
                        cogr_ds = gdal_open_vector(path_c, 1, None, kwargs)
                    except DriverError:
                        # log.exception("Caught DriverError")
                        # failed, attempt to create it
                        with Env(GDAL_VALIDATE_CREATION_OPTIONS="NO"):
                            cogr_ds = gdal_create(cogr_driver, path_c, kwargs)
                    else:
                        # check capability of creating a new layer in the existing dataset
                        capability = GDALDatasetTestCapability(cogr_ds, ODsCCreateLayer)
                        if not capability or collection.name is None:
                            # unable to use existing dataset, recreate it
                            log.debug("Unable to use existing dataset: capability=%r, name=%r", capability, collection.name)
                            GDALClose(cogr_ds)
                            cogr_ds = NULL
                            with Env(GDAL_VALIDATE_CREATION_OPTIONS="NO"):
                                cogr_ds = gdal_create(cogr_driver, path_c, kwargs)

            self.cogr_ds = cogr_ds

            # Set the spatial reference system from the crs given to the
            # collection constructor. We by-pass the crs_wkt
            # properties because they aren't accessible until the layer
            # is constructed (later).
            try:
                col_crs = collection._crs_wkt
                if col_crs:
                    cogr_srs = exc_wrap_pointer(OSRNewSpatialReference(NULL))
                    proj_b = col_crs.encode('utf-8')
                    proj_c = proj_b
                    OSRSetFromUserInput(cogr_srs, proj_c)
                    osr_set_traditional_axis_mapping_strategy(cogr_srs)
            except CPLE_BaseError as exc:
                GDALClose(self.cogr_ds)
                self.cogr_ds = NULL
                self.cogr_layer = NULL
                raise CRSError(str(exc))

            # Determine which encoding to use. The encoding parameter given to
            # the collection constructor takes highest precedence, then
            # 'iso-8859-1' (for shapefiles), then the system's default encoding
            # as last resort.
            sysencoding = locale.getpreferredencoding()
            self._fileencoding = userencoding or ("Shapefile" in collection.driver and 'iso-8859-1') or sysencoding

            if "Shapefile" in collection.driver:
                if self._fileencoding:
                    fileencoding_b = self._fileencoding.upper().encode('utf-8')
                    fileencoding_c = fileencoding_b
                    options = CSLSetNameValue(options, "ENCODING", fileencoding_c)

            # Does the layer exist already? If so, we delete it.
            layer_count = GDALDatasetGetLayerCount(self.cogr_ds)
            layer_names = []
            for i in range(layer_count):
                cogr_layer = GDALDatasetGetLayer(self.cogr_ds, i)
                name_c = OGR_L_GetName(cogr_layer)
                name_b = name_c
                layer_names.append(name_b.decode('utf-8'))

            idx = -1
            if isinstance(collection.name, str):
                if collection.name in layer_names:
                    idx = layer_names.index(collection.name)
            elif isinstance(collection.name, int):
                if collection.name >= 0 and collection.name < layer_count:
                    idx = collection.name
            if idx >= 0:
                log.debug("Deleted pre-existing layer at %s", collection.name)
                GDALDatasetDeleteLayer(self.cogr_ds, idx)

            # Create the named layer in the datasource.
            layername = collection.name
            layername_b = layername.encode('utf-8')

            # To avoid circular import.
            from fiona import meta

            kwarg_keys = set(key.upper() for key in kwargs.keys())
            lyr_creation_option_keys = kwarg_keys & set(meta.layer_creation_options(collection.driver))

            for k, v in kwargs.items():

                if v is not None and k.upper() in lyr_creation_option_keys:
                    kb = k.upper().encode('utf-8')

                    if isinstance(v, bool):
                        vb = ('ON' if v else 'OFF').encode('utf-8')
                    else:
                        vb = str(v).encode('utf-8')

                    options = CSLAddNameValue(options, <const char *>kb, <const char *>vb)

            geometry_type = collection.schema.get("geometry", "Unknown")

            if not isinstance(geometry_type, str) and geometry_type is not None:
                geometry_types = set(geometry_type)

                if len(geometry_types) > 1:
                    geometry_type = "Unknown"
                else:
                    geometry_type = geometry_types.pop()

            if geometry_type == "Any" or geometry_type is None:
                geometry_type = "Unknown"

            geometry_code = geometry_type_code(geometry_type)

            try:
                # In GDAL versions > 3.6.0 the following directive may
                # suffice and we might be able to eliminate the import
                # of fiona.meta in a future version of Fiona.
                with Env(GDAL_VALIDATE_CREATION_OPTIONS="NO"):
                    self.cogr_layer = exc_wrap_pointer(
                        GDALDatasetCreateLayer(
                            self.cogr_ds, <const char *>layername_b, cogr_srs,
                            <OGRwkbGeometryType>geometry_code, options))

            except Exception as exc:
                GDALClose(self.cogr_ds)
                self.cogr_ds = NULL
                raise DriverIOError(str(exc))

            finally:
                if options != NULL:
                    CSLDestroy(options)

                # Shapefile layers make a copy of the passed srs. GPKG
                # layers, on the other hand, increment its reference
                # count. OSRRelease() is the safe way to release
                # OGRSpatialReferenceH.
                if cogr_srs != NULL:
                    OSRRelease(cogr_srs)

            log.debug("Created layer %s", collection.name)

            # Next, make a layer definition from the given schema properties,
            # which are a dict built-in since Fiona 2.0

            # Test if default fields are included in provided schema
            schema_fields = collection.schema['properties']
            default_fields = self.get_schema()['properties']

            for key, value in default_fields.items():
                if key in schema_fields and not schema_fields[key] == value:
                    raise SchemaError(
                        f"Property '{key}' must have type '{value}' "
                        f"for driver '{self.collection.driver}'"
                    )

            new_fields = {k: v for k, v in schema_fields.items() if k not in default_fields}
            before_fields = default_fields.copy()
            before_fields.update(new_fields)

            encoding = self._get_internal_encoding()

            for key, value in new_fields.items():

                # Is there a field width/precision?
                width = precision = None

                if ':' in value:
                    value, fmt = value.split(':')

                    if '.' in fmt:
                        width, precision = map(int, fmt.split('.'))
                    else:
                        width = int(fmt)

                    # Type inference based on field width is something
                    # we should reconsider down the road.
                    if value == 'int':
                        if width == 0 or width >= 10:
                            value = 'int64'
                        else:
                            value = 'int32'

                value = normalize_field_type(value)
                ftype = NAMED_FIELD_TYPES[value]
                ogrfieldtype, ogrfieldsubtype = FIELD_TYPES_MAP2[ftype]

                try:
                    key_b = key.encode(encoding)
                    cogr_fielddefn = exc_wrap_pointer(
                        OGR_Fld_Create(<char *>key_b, <OGRFieldType>ogrfieldtype)
                    )

                    if width:
                        OGR_Fld_SetWidth(cogr_fielddefn, width)
                    if precision:
                        OGR_Fld_SetPrecision(cogr_fielddefn, precision)
                    if ogrfieldsubtype != OFSTNone:
                        OGR_Fld_SetSubType(cogr_fielddefn, ogrfieldsubtype)

                    exc_wrap_int(OGR_L_CreateField(self.cogr_layer, cogr_fielddefn, 1))

                except (UnicodeEncodeError, CPLE_BaseError) as exc:
                    GDALClose(self.cogr_ds)
                    self.cogr_ds = NULL
                    self.cogr_layer = NULL
                    raise SchemaError(str(exc))

                else:
                    OGR_Fld_Destroy(cogr_fielddefn)

        # Mapping of the Python collection schema to the munged
        # OGR schema.
        after_fields = self.get_schema()['properties']
        self._schema_mapping = dict(zip(before_fields.keys(), after_fields.keys()))

        # Mapping of the Python collection schema to OGR field indices.
        # We assume that get_schema()['properties'].keys() is in the exact OGR field order
        assert len(before_fields) == len(after_fields)
        self._schema_mapping_index = dict(zip(before_fields.keys(), range(len(after_fields.keys()))))

        # Mapping of the Python collection schema to normalized field types
        self._schema_normalized_field_types = {k: normalize_field_type(v) for (k, v) in self.collection.schema['properties'].items()}

        log.debug("Writing started")

    def writerecs(self, records, collection):
        """Writes records to collection storage.

        Parameters
        ----------
        records : Iterable
            A stream of feature records.
        collection : Collection
            The collection in which feature records are stored.

        Returns
        -------
        None

        """
        cdef OGRSFDriverH cogr_driver
        cdef OGRFeatureH cogr_feature
        cdef int features_in_transaction = 0
        cdef OGRLayerH cogr_layer = self.cogr_layer

        if cogr_layer == NULL:
            raise ValueError("Null layer")

        cdef OGRFeatureBuilder feat_builder = OGRFeatureBuilder(driver=collection.driver)
        valid_geom_types = collection._valid_geom_types

        def validate_geometry_type(record):
            if record["geometry"] is None:
                return True
            return record["geometry"]["type"].lstrip("3D ") in valid_geom_types

        transactions_supported = GDALDatasetTestCapability(self.cogr_ds, ODsCTransactions)
        log.debug("Transaction supported: %s", transactions_supported)

        if transactions_supported:
            log.debug("Starting transaction (initial)")
            result = GDALDatasetStartTransaction(self.cogr_ds, 0)
            if result == OGRERR_FAILURE:
                raise TransactionError("Failed to start transaction")

        schema_props_keys = set(collection.schema['properties'].keys())

        for _rec in records:
            record = decode_object(_rec)

            # Validate against collection's schema.
            if set(record.properties.keys()) != schema_props_keys:
                raise ValueError(
                    "Record does not match collection schema: %r != %r" % (
                        list(record.properties.keys()),
                        list(schema_props_keys) ))

            if not validate_geometry_type(record):
                raise GeometryTypeValidationError(
                    "Record's geometry type does not match "
                    "collection schema's geometry type: %r != %r" % (
                        record.geometry.type,
                        collection.schema['geometry'] ))

            cogr_feature = feat_builder.build(record, collection)
            result = OGR_L_CreateFeature(cogr_layer, cogr_feature)

            if result != OGRERR_NONE:
                msg = get_last_error_msg()
                raise RuntimeError(
                    f"GDAL Error: {msg}. Failed to write record: {record}"
                )

            _deleteOgrFeature(cogr_feature)

            if transactions_supported:
                features_in_transaction += 1

                if features_in_transaction == DEFAULT_TRANSACTION_SIZE:
                    log.debug("Committing transaction (intermediate)")
                    result = GDALDatasetCommitTransaction(self.cogr_ds)

                    if result == OGRERR_FAILURE:
                        raise TransactionError("Failed to commit transaction")

                    log.debug("Starting transaction (intermediate)")
                    result = GDALDatasetStartTransaction(self.cogr_ds, 0)

                    if result == OGRERR_FAILURE:
                        raise TransactionError("Failed to start transaction")

                    features_in_transaction = 0

        if transactions_supported:
            log.debug("Committing transaction (final)")
            result = GDALDatasetCommitTransaction(self.cogr_ds)
            if result == OGRERR_FAILURE:
                raise TransactionError("Failed to commit transaction")

    def sync(self, collection):
        """Syncs OGR to disk."""
        cdef void *cogr_ds = self.cogr_ds
        cdef void *cogr_layer = self.cogr_layer
        if cogr_ds == NULL:
            raise ValueError("Null data source")

        gdal_flush_cache(cogr_ds)
        log.debug("Flushed data source cache")

    def update_tags(self, tags, ns=None):
        """Writes a dict containing the dataset or layers's tags.
        Tags are pairs of key and value strings. Tags belong to
        namespaces.  The standard namespaces are: default (None) and
        'IMAGE_STRUCTURE'.  Applications can create their own additional
        namespaces.

        Parameters
        ----------
        tags: dict
            The dict of metadata items to set.
        ns: str, optional
            Used to select a namespace other than the default.

        Returns
        -------
        int
        """
        cdef GDALMajorObjectH obj = NULL
        if self.cogr_layer != NULL:
            obj = self.cogr_layer
        else:
            obj = self.cogr_ds

        cdef const char *domain = NULL
        if ns:
            ns = ns.encode('utf-8')
            domain = ns

        cdef char **metadata = NULL
        try:
            for key, value in tags.items():
                key = key.encode("utf-8")
                value = value.encode("utf-8")
                metadata = CSLAddNameValue(metadata, <const char *>key, <const char *>value)
            return GDALSetMetadata(obj, metadata, domain)
        finally:
            CSLDestroy(metadata)

    def update_tag_item(self, key, tag, ns=None):
        """Updates the tag item value

        Parameters
        ----------
        key: str
            The key for the metadata item to set.
        tag: str
            The value of the metadata item to set.
        ns: str
            Used to select a namespace other than the default.

        Returns
        -------
        int
        """
        key = key.encode('utf-8')
        cdef const char *name = key
        tag = tag.encode("utf-8")
        cdef char *value = tag

        cdef const char *domain = NULL
        if ns:
            ns = ns.encode('utf-8')
            domain = ns

        cdef GDALMajorObjectH obj = NULL
        if self.cogr_layer != NULL:
            obj = self.cogr_layer
        else:
            obj = self.cogr_ds

        return GDALSetMetadataItem(obj, name, value, domain)


cdef class Iterator:

    """Provides iterated access to feature data.
    """

    cdef collection
    cdef encoding
    cdef int next_index
    cdef stop
    cdef start
    cdef step
    cdef fastindex
    cdef fastcount
    cdef ftcount
    cdef stepsign
    cdef FeatureBuilder feat_builder

    def __cinit__(self, collection, start=None, stop=None, step=None,
                  bbox=None, mask=None, where=None):
        if collection.session is None:
            raise ValueError("I/O operation on closed collection")
        self.collection = collection
        cdef Session session
        cdef void *cogr_geometry
        session = self.collection.session
        cdef void *cogr_layer = session.cogr_layer
        if cogr_layer == NULL:
            raise ValueError("Null layer")
        OGR_L_ResetReading(cogr_layer)

        if bbox and mask:
            raise ValueError("mask and bbox can not be set together")

        if bbox:
            OGR_L_SetSpatialFilterRect(
                cogr_layer, bbox[0], bbox[1], bbox[2], bbox[3])
        elif mask:
            mask_geom = decode_object(mask)
            cogr_geometry = OGRGeomBuilder().build(mask_geom)
            OGR_L_SetSpatialFilter(cogr_layer, cogr_geometry)
            OGR_G_DestroyGeometry(cogr_geometry)

        else:
            OGR_L_SetSpatialFilter(cogr_layer, NULL)

        if where:
            where_b = where.encode('utf-8')
            where_c = where_b
            try:
                exc_wrap_int(
                    OGR_L_SetAttributeFilter(cogr_layer, <const char*>where_c))
            except CPLE_AppDefinedError as e:
                raise AttributeFilterError(e) from None

        else:
            OGR_L_SetAttributeFilter(cogr_layer, NULL)

        self.encoding = session._get_internal_encoding()

        self.fastindex = OGR_L_TestCapability(
            session.cogr_layer, OLC_FASTSETNEXTBYINDEX)
        log.debug("OLC_FASTSETNEXTBYINDEX: %s", self.fastindex)

        self.fastcount = OGR_L_TestCapability(
            session.cogr_layer, OLC_FASTFEATURECOUNT)
        log.debug("OLC_FASTFEATURECOUNT: %s", self.fastcount)

        # In some cases we need to force count of all features
        # We need to check if start is not greater ftcount: (start is not None and start > 0)
        # If start is a negative index: (start is not None and start < 0)
        # If stop is a negative index: (stop is not None and stop < 0)
        if ((start is not None and not start == 0) or
                (stop is not None and stop < 0)):
            if not self.fastcount:
                warnings.warn("Layer does not support" \
                        " OLC_FASTFEATURECOUNT, negative slices or start values other than zero" \
                        " may be slow.", RuntimeWarning)
            self.ftcount = session._get_feature_count(1)
        else:
            self.ftcount = session._get_feature_count(0)

        if self.ftcount == -1 and ((start is not None and start < 0) or
                              (stop is not None and stop < 0)):
            raise IndexError(
                "collection's dataset does not support negative slice indexes")

        if stop is not None and stop < 0:
            stop += self.ftcount

        if start is None:
            start = 0
        if start is not None and start < 0:
            start += self.ftcount

        # step size
        if step is None:
            step = 1
        if step == 0:
            raise ValueError("slice step cannot be zero")
        if step < 0 and not self.fastindex:
            warnings.warn("Layer does not support" \
                    " OLCFastSetNextByIndex, negative step size may" \
                    " be slow.", RuntimeWarning)

        # Check if we are outside of the range:
        if not self.ftcount == -1:
            if start > self.ftcount and step > 0:
                start = -1
            if start > self.ftcount and step < 0:
                start = self.ftcount - 1
        elif self.ftcount == -1 and not start == 0:
            warnings.warn("Layer is unable to check if slice is within range of data.",
             RuntimeWarning)

        self.stepsign = int(math.copysign(1, step))
        self.stop = stop
        self.start = start
        self.step = step

        self.next_index = start
        log.debug("Next index: %d", self.next_index)

        # Set OGR_L_SetNextByIndex only if within range
        if start >= 0 and (self.ftcount == -1 or self.start < self.ftcount):
            exc_wrap_int(OGR_L_SetNextByIndex(session.cogr_layer, self.next_index))
        session.cursor_interrupted = False

        self.feat_builder = FeatureBuilder(driver=collection.driver)

    def __iter__(self):
        return self

    def _next(self):
        """Internal method to set read cursor to next item"""
        cdef Session session = self.collection.session

        # Check if next_index is valid
        if self.next_index < 0:
            raise StopIteration

        # GeoJSON driver with gdal 2.1 - 2.2 returns last feature
        # if index greater than number of features
        if self.ftcount >= 0 and self.next_index >= self.ftcount:
            raise StopIteration

        if self.stepsign == 1:
            if self.next_index < self.start or (self.stop is not None and self.next_index >= self.stop):
                raise StopIteration
        else:
            if self.next_index > self.start or (self.stop is not None and self.next_index <= self.stop):
                raise StopIteration

        # Set read cursor to next_item position
        if session.cursor_interrupted:
            if not self.fastindex and not self.next_index == 0:
                warnings.warn(
                    "Sequential read of iterator was interrupted. Resetting iterator. "
                    "This can negatively impact the performance.", RuntimeWarning
                )
            exc_wrap_int(OGR_L_SetNextByIndex(session.cogr_layer, self.next_index))
            session.cursor_interrupted = False

        else:
            if self.step > 1 and self.fastindex:
                exc_wrap_int(OGR_L_SetNextByIndex(session.cogr_layer, self.next_index))

            elif self.step > 1 and not self.fastindex and not self.next_index == self.start:
                # OGR's default implementation of SetNextByIndex is
                # calling ResetReading() and then calling GetNextFeature
                # n times. We can shortcut that if we know the previous
                # index.  OGR_L_GetNextFeature increments cursor by 1,
                # therefore self.step - 1 as one increment was performed
                # when feature is read.
                for _ in range(self.step - 1):
                    try:
                        cogr_feature = OGR_L_GetNextFeature(session.cogr_layer)
                        if cogr_feature == NULL:
                            raise StopIteration
                    finally:
                        _deleteOgrFeature(cogr_feature)

            elif self.step > 1 and not self.fastindex and self.next_index == self.start:
                exc_wrap_int(OGR_L_SetNextByIndex(session.cogr_layer, self.next_index))

            elif self.step == 0:
                # OGR_L_GetNextFeature increments read cursor by one
                pass

            elif self.step < 0:
                exc_wrap_int(OGR_L_SetNextByIndex(session.cogr_layer, self.next_index))

        # set the next index
        self.next_index += self.step
        log.debug("Next index: %d", self.next_index)

    def __next__(self):
        cdef OGRFeatureH cogr_feature = NULL
        cdef Session session = self.collection.session

        if not session or not session.isactive:
            raise FionaValueError("Session is inactive, dataset is closed or layer is unavailable.")

        # Update read cursor
        self._next()

        # Get the next feature.
        cogr_feature = OGR_L_GetNextFeature(session.cogr_layer)
        if cogr_feature == NULL:
            raise StopIteration

        try:
            return self.feat_builder.build(
                cogr_feature,
                encoding=self.collection.session._get_internal_encoding(),
                bbox=False,
                driver=self.collection.driver,
                ignore_fields=self.collection.ignore_fields,
                ignore_geometry=self.collection.ignore_geometry,
            )
        finally:
            _deleteOgrFeature(cogr_feature)


cdef class ItemsIterator(Iterator):

    def __next__(self):
        cdef long fid
        cdef OGRFeatureH cogr_feature = NULL
        cdef Session session = self.collection.session

        if not session or not session.isactive:
            raise FionaValueError("Session is inactive, dataset is closed or layer is unavailable.")

        # Update read cursor
        self._next()

        # Get the next feature.
        cogr_feature = OGR_L_GetNextFeature(session.cogr_layer)
        if cogr_feature == NULL:
            raise StopIteration

        try:
            fid = OGR_F_GetFID(cogr_feature)
            feature = self.feat_builder.build(
                cogr_feature,
                encoding=self.collection.session._get_internal_encoding(),
                bbox=False,
                driver=self.collection.driver,
                ignore_fields=self.collection.ignore_fields,
                ignore_geometry=self.collection.ignore_geometry,
            )
        else:
            return fid, feature
        finally:
            _deleteOgrFeature(cogr_feature)


cdef class KeysIterator(Iterator):

    def __next__(self):
        cdef long fid
        cdef OGRFeatureH cogr_feature = NULL
        cdef Session session = self.collection.session

        if not session or not session.isactive:
            raise FionaValueError("Session is inactive, dataset is closed or layer is unavailable.")

        # Update read cursor
        self._next()

        # Get the next feature.
        cogr_feature = OGR_L_GetNextFeature(session.cogr_layer)
        if cogr_feature == NULL:
            raise StopIteration

        fid = OGR_F_GetFID(cogr_feature)
        _deleteOgrFeature(cogr_feature)

        return fid


def _remove(path, driver=None):
    """Deletes an OGR data source
    """
    cdef void *cogr_driver
    cdef void *cogr_ds
    cdef int result
    cdef char *driver_c

    if driver is None:
        # attempt to identify the driver by opening the dataset
        try:
            cogr_ds = gdal_open_vector(path.encode("utf-8"), 0, None, {})
        except (DriverError, FionaNullPointerError):
            raise DatasetDeleteError(f"Failed to remove data source {path}")
        cogr_driver = GDALGetDatasetDriver(cogr_ds)
        GDALClose(cogr_ds)
    else:
        cogr_driver = GDALGetDriverByName(driver.encode("utf-8"))

    if cogr_driver == NULL:
        raise DatasetDeleteError(f"Null driver when attempting to delete {path}")

    if not OGR_Dr_TestCapability(cogr_driver, ODrCDeleteDataSource):
        raise DatasetDeleteError("Driver does not support dataset removal operation")

    result = GDALDeleteDataset(cogr_driver, path.encode('utf-8'))
    if result != OGRERR_NONE:
        raise DatasetDeleteError(f"Failed to remove data source {path}")


def _remove_layer(path, layer, driver=None):
    cdef void *cogr_ds
    cdef int layer_index

    if isinstance(layer, int):
        layer_index = layer
        layer_str = str(layer_index)
    else:
        layer_names = _listlayers(path)
        try:
            layer_index = layer_names.index(layer)
        except ValueError:
            raise ValueError(f'Layer "{layer}" does not exist in datasource: {path}')
        layer_str = f'"{layer}"'

    if layer_index < 0:
        layer_names = _listlayers(path)
        layer_index = len(layer_names) + layer_index

    try:
        cogr_ds = gdal_open_vector(path.encode("utf-8"), 1, None, {})
    except (DriverError, FionaNullPointerError):
        raise DatasetDeleteError(f"Failed to remove data source {path}")

    result = GDALDatasetDeleteLayer(cogr_ds, layer_index)
    GDALClose(cogr_ds)

    if result == OGRERR_UNSUPPORTED_OPERATION:
        raise DatasetDeleteError(f"Removal of layer {layer_str} not supported by driver")
    elif result != OGRERR_NONE:
        raise DatasetDeleteError(f"Failed to remove layer {layer_str} from datasource: {path}")


def _listlayers(path, **kwargs):
    """Provides a list of the layers in an OGR data source.
    """
    cdef void *cogr_ds = NULL
    cdef void *cogr_layer = NULL
    cdef const char *path_c
    cdef const char *name_c

    # Open OGR data source.
    path_b = strencode(path)
    path_c = path_b
    cogr_ds = gdal_open_vector(path_c, 0, None, kwargs)

    # Loop over the layers to get their names.
    layer_count = GDALDatasetGetLayerCount(cogr_ds)
    layer_names = []
    for i in range(layer_count):
        cogr_layer = GDALDatasetGetLayer(cogr_ds, i)
        name_c = OGR_L_GetName(cogr_layer)
        name_b = name_c
        layer_names.append(name_b.decode('utf-8'))

    # Close up data source.
    if cogr_ds != NULL:
        GDALClose(cogr_ds)
    cogr_ds = NULL

    return layer_names


def _listdir(path):
    """List all files in path, if path points to a directory"""
    cdef const char *path_c
    cdef int n
    cdef char** papszFiles
    cdef VSIStatBufL st_buf

    try:
        path_b = path.encode('utf-8')
    except UnicodeDecodeError:
        path_b = path
    path_c = path_b
    if not VSIStatL(path_c, &st_buf) == 0:
        raise FionaValueError(f"Path '{path}' does not exist.")
    if not VSI_ISDIR(st_buf.st_mode):
        raise FionaValueError(f"Path '{path}' is not a directory.")

    papszFiles = VSIReadDir(path_c)
    n = CSLCount(<CSLConstList>papszFiles)
    files = []
    for i in range(n):
        files.append(papszFiles[i].decode("utf-8"))
    CSLDestroy(papszFiles)

    return files


def buffer_to_virtual_file(bytesbuf, ext=''):
    """Maps a bytes buffer to a virtual file.

    `ext` is empty or begins with a period and contains at most one period.
    """

    vsi_filename = f"/vsimem/{uuid4().hex}{ext}"
    vsi_cfilename = vsi_filename if not isinstance(vsi_filename, str) else vsi_filename.encode('utf-8')

    vsi_handle = VSIFileFromMemBuffer(vsi_cfilename, <unsigned char *>bytesbuf, len(bytesbuf), 0)

    if vsi_handle == NULL:
        raise OSError('failed to map buffer to file')
    if VSIFCloseL(vsi_handle) != 0:
        raise OSError('failed to close mapped file handle')

    return vsi_filename


def remove_virtual_file(vsi_filename):
    vsi_cfilename = vsi_filename if not isinstance(vsi_filename, str) else vsi_filename.encode('utf-8')
    return VSIUnlink(vsi_cfilename)


cdef class MemoryFileBase:
    """Base for a BytesIO-like class backed by an in-memory file."""

    cdef VSILFILE * _vsif

    def __init__(self, file_or_bytes=None, dirname=None, filename=None, ext=''):
        """A file in an in-memory filesystem.

        Parameters
        ----------
        file_or_bytes : file or bytes
            A file opened in binary mode or bytes
        filename : str
            A filename for the in-memory file under /vsimem
        ext : str
            A file extension for the in-memory file under /vsimem. Ignored if
            filename was provided.

        """
        if file_or_bytes:
            if hasattr(file_or_bytes, 'read'):
                initial_bytes = file_or_bytes.read()
            elif isinstance(file_or_bytes, bytes):
                initial_bytes = file_or_bytes
            else:
                raise TypeError(
                    "Constructor argument must be a file opened in binary "
                    "mode or bytes.")
        else:
            initial_bytes = b''

        # Make an in-memory directory specific to this dataset to help organize
        # auxiliary files.
        self._dirname = dirname or str(uuid4().hex)
        VSIMkdir(f"/vsimem/{self._dirname}".encode("utf-8"), 0666)

        if filename:
            # GDAL's SRTMHGT driver requires the filename to be "correct" (match
            # the bounds being written)
            self.name = f"/vsimem/{self._dirname}/{filename}"
        else:
            # GDAL 2.1 requires a .zip extension for zipped files.
            self.name = f"/vsimem/{self._dirname}/{self._dirname}{ext}"

        name_b = self.name.encode('utf-8')
        self._initial_bytes = initial_bytes
        cdef unsigned char *buffer = self._initial_bytes

        if self._initial_bytes:
            self._vsif = VSIFileFromMemBuffer(
               name_b, buffer, len(self._initial_bytes), 0)
            self.mode = "r"
        else:
            self._vsif = NULL
            self.mode = "r+"

        self.closed = False

    def _open(self):
        """Ensure that the instance has a valid VSI file handle."""
        cdef VSILFILE *fp = NULL
        name_b = self.name.encode('utf-8')

        if not self.exists():
            fp = VSIFOpenL(name_b, "w")
            if fp == NULL:
                raise OSError("VSIFOpenL failed")
            else:
                VSIFCloseL(fp)
            self._vsif = NULL

        if self._vsif == NULL:
            fp = VSIFOpenL(name_b, self.mode.encode("utf-8"))
            if fp == NULL:
                log.error("VSIFOpenL failed: name=%r, mode=%r", self.name, self.mode)
                raise OSError("VSIFOpenL failed")
            else:
                self._vsif = fp

    def _ensure_extension(self, drivername=None):
        """Ensure that the instance's name uses a file extension supported by the driver."""
        # Avoid a crashing bug with GDAL versions < 2.
        if get_gdal_version_tuple() < (2, ):
            return

        recommended_extension = _get_metadata_item(drivername, "DMD_EXTENSION")
        if recommended_extension is not None:
            if not recommended_extension.startswith("."):
                recommended_extension = "." + recommended_extension
            root, ext = os.path.splitext(self.name)
            if not ext:
                log.info("Setting extension: root=%r, extension=%r", root, recommended_extension)
                self.name = root + recommended_extension

    def exists(self):
        """Test if the in-memory file exists.

        Returns
        -------
        bool
            True if the in-memory file exists.

        """
        cdef VSIStatBufL st_buf
        name_b = self.name.encode('utf-8')
        return VSIStatL(name_b, &st_buf) == 0

    def __len__(self):
        """Length of the file's buffer in number of bytes.

        Returns
        -------
        int

        """
        if not self.getbuffer():
            return 0        
        return self.getbuffer().size

    def getbuffer(self):
        """Return a view on bytes of the file, or None."""
        cdef unsigned char *buffer = NULL
        cdef vsi_l_offset buffer_len = 0
        cdef unsigned char [:] buff_view

        name_b = self.name.encode('utf-8')
        buffer = VSIGetMemFileBuffer(name_b, &buffer_len, 0)

        if buffer == NULL or buffer_len == 0:
            return None
        else:
            buff_view = <unsigned char [:buffer_len]>buffer
            return buff_view

    def close(self):
        """Close and tear down VSI file and directory."""
        if self._vsif != NULL:
            VSIFCloseL(self._vsif)
        self._vsif = NULL
        # As soon as support for GDAL < 3 is dropped, we can switch
        # to VSIRmdirRecursive.
        VSIUnlink(self.name.encode("utf-8"))
        VSIRmdir(self._dirname.encode("utf-8"))
        self.closed = True

    def seek(self, offset, whence=0):
        self._open()
        return VSIFSeekL(self._vsif, offset, whence)

    def tell(self):
        self._open()
        if self._vsif != NULL:
            return VSIFTellL(self._vsif)
        else:
            return 0

    def read(self, size=-1):
        """Read size bytes from MemoryFile."""
        cdef bytes result
        cdef unsigned char *buffer = NULL
        cdef vsi_l_offset buffer_len = 0

        if size < 0:
            name_b = self.name.encode('utf-8')
            buffer = VSIGetMemFileBuffer(name_b, &buffer_len, 0)
            size = buffer_len

        buffer = <unsigned char *>CPLMalloc(size)

        self._open()

        try:
            objects_read = VSIFReadL(buffer, 1, size, self._vsif)
            result = <bytes>buffer[:objects_read]
            return result

        finally:
            CPLFree(buffer)

    def write(self, data):
        """Write data bytes to MemoryFile"""
        cdef const unsigned char *view = <bytes>data
        n = len(data)
        self._open()
        result = VSIFWriteL(view, 1, n, self._vsif)
        VSIFFlushL(self._vsif)
        return result


def _get_metadata_item(driver, metadata_item):
    """Query metadata items
    Parameters
    ----------
    driver : str
        Driver to query
    metadata_item : str or None
        Metadata item to query
    Returns
    -------
    str or None
        Metadata item
    """
    cdef const char* metadata_c = NULL
    cdef void *cogr_driver

    if get_gdal_version_tuple() < (2, ):
        return None

    if driver is None:
        return None

    driver_b = strencode(driver)
    cogr_driver = GDALGetDriverByName(driver_b)
    if cogr_driver == NULL:
        raise FionaValueError(f"Could not find driver '{driver}'")

    metadata_c = GDALGetMetadataItem(cogr_driver, metadata_item.encode('utf-8'), NULL)

    metadata = None
    if metadata_c != NULL:
        metadata = metadata_c
        metadata = metadata.decode('utf-8')
        if len(metadata) == 0:
            metadata = None

    return metadata
