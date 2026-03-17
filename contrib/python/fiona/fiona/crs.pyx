# cython: boundscheck=False, embedsignature=True

"""Coordinate reference systems, the CRS class and supporting functions.

A coordinate reference system (CRS) defines how a dataset's pixels map
to locations on, for example, a globe or the Earth. A CRS may be local
or global. The GIS field shares a number of authority files that define
CRS. "EPSG:32618" is the name of a regional CRS from the European
Petroleum Survey Group authority file. "OGC:CRS84" is the name of a
global CRS from the Open Geospatial Consortium authority. Custom CRS can
be described in text using several formats. Rasterio's CRS class is our
abstraction for coordinate reference systems.

A fiona.Collection's crs property is an instance of CRS. CRS are also
used to define transformations between coordinate reference systems.
These transformations are performed by the PROJ library. Rasterio does
not call PROJ functions directly, but invokes them via calls to GDAL's
"OSR*" functions.

"""

from collections import defaultdict
import json
import logging
import pickle
import typing
import warnings
import re

import fiona._env
from fiona._err import CPLE_BaseError, CPLE_NotSupportedError
from fiona.compat import DICT_TYPES
from fiona.errors import CRSError, FionaDeprecationWarning
from fiona.enums import WktVersion

from fiona._env cimport _safe_osr_release
from fiona._err cimport exc_wrap_ogrerr, exc_wrap_int, exc_wrap_pointer


log = logging.getLogger(__name__)


_RE_PROJ_PARAM = re.compile(r"""
    \+              # parameter starts with '+' character
    (?P<param>\w+)    # capture parameter name
    \=?             # match both key only and key-value parameters
    (?P<value>\S+)? # capture all characters up to next space (None if no value)
    \s*?            # consume remaining whitespace, if any
""", re.X)


cdef void osr_set_traditional_axis_mapping_strategy(OGRSpatialReferenceH hSrs):
    OSRSetAxisMappingStrategy(hSrs, OAMS_TRADITIONAL_GIS_ORDER)


cdef class CRS:
    """A geographic or projected coordinate reference system.

    .. versionadded:: 1.9.0

    CRS objects may be created by passing PROJ parameters as keyword
    arguments to the standard constructor or by passing EPSG codes, PROJ
    mappings, PROJ strings, or WKT strings to the from_epsg, from_dict,
    from_string, or from_wkt static methods.

    Examples
    --------

    The from_dict method takes PROJ parameters as keyword arguments.

    >>> crs = CRS.from_dict(proj="aea")

    EPSG codes may be used with the from_epsg method.

    >>> crs = CRS.from_epsg(3005)

    The from_string method takes a variety of input.

    >>> crs = CRS.from_string("EPSG:3005")

    """
    def __init__(self, initialdata=None, **kwargs):
        """Make a CRS from a PROJ dict or mapping.

        Parameters
        ----------
        initialdata : mapping, optional
            A dictionary or other mapping
        kwargs : mapping, optional
            Another mapping. Will be overlaid on the initialdata.

        Returns
        -------
        CRS

        """
        cdef CRS tmp
        self._data = {}
        self._epsg = None
        self._wkt = None

        if initialdata or kwargs:
            tmp = CRS.from_dict(initialdata=initialdata, **kwargs)
            self._osr = OSRClone(tmp._osr)
            self._wkt = tmp._wkt
            self._data = tmp.data
            self._epsg = tmp._epsg

    @property
    def data(self):
        """A PROJ4 dict representation of the CRS.
        """
        if not self._data:
            self._data = self.to_dict()
        return self._data

    @property
    def is_valid(self):
        """Test that the CRS is a geographic or projected CRS.

        Returns
        -------
        bool

        """
        return self.is_geographic or self.is_projected

    @property
    def is_epsg_code(self):
        """Test if the CRS is defined by an EPSG code.

        Returns
        -------
        bool

        """
        try:
            return bool(self.to_epsg())
        except CRSError:
            return False

    @property
    def wkt(self):
        """An OGC WKT representation of the CRS

        Returns
        -------
        str

        """
        if not self._wkt:
            self._wkt = self.to_wkt()
        return self._wkt

    @property
    def is_geographic(self):
        """Test if the CRS is a geographic coordinate reference system.

        Returns
        -------
        bool

        Raises
        ------
        CRSError

        """
        try:
            return bool(OSRIsGeographic(self._osr) == 1)
        except CPLE_BaseError as exc:
            raise CRSError(str(exc))

    @property
    def is_projected(self):
        """Test if the CRS is a projected coordinate reference system.

        Returns
        -------
        bool

        Raises
        ------
        CRSError

        """
        try:
            return bool(OSRIsProjected(self._osr) == 1)
        except CPLE_BaseError as exc:
            raise CRSError(str(exc))

    @property
    def linear_units(self):
        """Get a short name for the linear units of the CRS.

        Returns
        -------
        units : str
            "m", "ft", etc.

        Raises
        ------
        CRSError

        """
        try:
            return self.linear_units_factor[0]
        except CRSError:
            return "unknown"

    @property
    def linear_units_factor(self):
        """Get linear units and the conversion factor to meters of the CRS.

        Returns
        -------
        units : str
            "m", "ft", etc.
        factor : float
            Ratio of one unit to one meter.

        Raises
        ------
        CRSError

        """
        cdef char *units_c = NULL
        cdef double to_meters

        try:
            if self.is_projected:
                to_meters = OSRGetLinearUnits(self._osr, &units_c)
            else:
                raise CRSError("Linear units factor is not defined for non projected CRS")
        except CPLE_BaseError as exc:
            raise CRSError(str(exc))
        else:
            units_b = units_c
            return (units_b.decode('utf-8'), to_meters)

    @property
    def units_factor(self):
        """Get units and the conversion factor of the CRS.

        Returns
        -------
        units : str
            "m", "ft", etc.
        factor : float
            Ratio of one unit to one radian if the CRS is geographic
            otherwise, it is to one meter.

        Raises
        ------
        CRSError

        """
        cdef char *units_c = NULL
        cdef double factor

        try:
            if self.is_geographic:
                factor = OSRGetAngularUnits(self._osr, &units_c)
            else:
                factor = OSRGetLinearUnits(self._osr, &units_c)
        except CPLE_BaseError as exc:
            raise CRSError(exc)
        else:
            units_b = units_c
            return (units_b.decode('utf-8'), factor)

    def to_dict(self, projjson=False):
        """Convert CRS to a PROJ dict.

        .. note:: If there is a corresponding EPSG code, it will be used
           when returning PROJ parameter dict.

        .. versionadded:: 1.9.0

        Parameters
        ----------
        projjson: bool, default=False
            If True, will convert to PROJ JSON dict (Requites GDAL 3.1+
            and PROJ 6.2+).  If False, will convert to PROJ parameter
            dict.

        Returns
        -------
        dict

        """
        cdef OGRSpatialReferenceH osr = NULL
        cdef char *proj_c = NULL

        if projjson:
            text = self._projjson()
            return json.loads(text) if text else {}

        epsg_code = self.to_epsg()

        if epsg_code:
            return {"init": f"epsg:{epsg_code}"}
        else:
            try:
                osr = exc_wrap_pointer(OSRClone(self._osr))
                exc_wrap_ogrerr(OSRExportToProj4(osr, &proj_c))

            except CPLE_BaseError as exc:
                return {}
                # raise CRSError(f"The WKT could not be parsed. {exc}"

            else:
                proj_b = proj_c
                proj = proj_b.decode('utf-8')

            finally:
                CPLFree(proj_c)
                _safe_osr_release(osr)

            def parse(v):
                try:
                    return int(v)
                except ValueError:
                    pass
                try:
                    return float(v)
                except ValueError:
                    return v

            rv = {}
            for param in _RE_PROJ_PARAM.finditer(proj):
                key, value = param.groups()
                if key not in all_proj_keys:
                    continue

                if value is None or value.lower() == "true":
                    rv[key] = True
                elif value.lower() == "false":
                    continue
                else:
                    rv[key] = parse(value)
            return rv

    def to_proj4(self):
        """Convert to a PROJ4 representation.

        Returns
        -------
        str

        """
        return " ".join([f"+{key}={val}" for key, val in self.data.items()])

    def to_wkt(self, morph_to_esri_dialect=False, version=None):
        """Convert to a OGC WKT representation.

        .. versionadded:: 1.9.0

        Parameters
        ----------
        morph_to_esri_dialect : bool, optional
            Whether or not to morph to the Esri dialect of WKT Only
            applies to GDAL versions < 3. This parameter will be removed
            in a future version of fiona (2.0.0).
        version : WktVersion or str, optional
            The version of the WKT output.
            Defaults to GDAL's default (WKT1_GDAL for GDAL 3).

        Returns
        -------
        str

        Raises
        ------
        CRSError

        """
        cdef char *conv_wkt = NULL
        cdef const char* options_wkt[2]
        options_wkt[0] = NULL
        options_wkt[1] = NULL

        try:
            if OSRGetName(self._osr) != NULL:
                if morph_to_esri_dialect:
                    warnings.warn(
                        "'morph_to_esri_dialect' ignored with GDAL 3+. "
                        "Use 'version=WktVersion.WKT1_ESRI' instead."
                    )
                if version:
                    version = WktVersion(version).value
                    wkt_format = f"FORMAT={version}".encode("utf-8")
                    options_wkt[0] = wkt_format
                exc_wrap_ogrerr(OSRExportToWktEx(self._osr, &conv_wkt, options_wkt))
        except (CPLE_BaseError, ValueError) as exc:
            raise CRSError(f"Cannot convert to WKT. {exc}") from exc

        else:
            if conv_wkt != NULL:
                return conv_wkt.decode('utf-8')
            else:
                return ''
        finally:
            CPLFree(conv_wkt)


    def to_epsg(self, confidence_threshold=70):
        """Convert to the best match EPSG code.

        For a CRS created using an EPSG code, that same value is
        returned.  For other CRS, including custom CRS, an attempt is
        made to match it to definitions in the EPSG authority file.
        Matches with a confidence below the threshold are discarded.

        Parameters
        ----------
        confidence_threshold : int
            Percent match confidence threshold (0-100).

        Returns
        -------
        int or None

        Raises
        ------
        CRSError

        """
        if self._epsg is not None:
            return self._epsg
        else:
            matches = self._matches(confidence_threshold=confidence_threshold)
            if "EPSG" in matches:
                self._epsg = int(matches["EPSG"][0])
                return self._epsg
            else:
                return None

    def to_authority(self, confidence_threshold=70):
        """Convert to the best match authority name and code.

        For a CRS created using an EPSG code, that same value is
        returned.  For other CRS, including custom CRS, an attempt is
        made to match it to definitions in authority files.  Matches
        with a confidence below the threshold are discarded.

        Parameters
        ----------
        confidence_threshold : int
            Percent match confidence threshold (0-100).

        Returns
        -------
        name : str
            Authority name.
        code : str
            Code from the authority file.

        or None

        """
        matches = self._matches(confidence_threshold=confidence_threshold)
        # Note: before version 1.2.7 this function only paid attention
        # to EPSG as an authority, which is why it takes priority over
        # others even if they were a better match.
        if "EPSG" in matches:
            return "EPSG", matches["EPSG"][0]
        elif "OGC" in matches:
            return "OGC", matches["OGC"][0]
        elif "ESRI" in matches:
            return "ESRI", matches["ESRI"][0]
        else:
            return None

    def _matches(self, confidence_threshold=70):
        """Find matches in authority files.

        Returns
        -------
        dict : {name: [codes]}
            A dictionary in which capitalized authority names are the
            keys and lists of codes ordered by match confidence,
            descending, are the values.

        """
        cdef OGRSpatialReferenceH osr = NULL
        cdef OGRSpatialReferenceH *matches = NULL
        cdef int *confidences = NULL
        cdef int num_matches = 0
        cdef int i = 0
        cdef char *c_code = NULL
        cdef char *c_name = NULL

        results = defaultdict(list)

        try:
            osr = exc_wrap_pointer(OSRClone(self._osr))
            matches = OSRFindMatches(osr, NULL, &num_matches, &confidences)

            for i in range(num_matches):
                confidence = confidences[i]
                c_code = OSRGetAuthorityCode(matches[i], NULL)
                c_name = OSRGetAuthorityName(matches[i], NULL)

                if c_code != NULL and c_name != NULL and confidence >= confidence_threshold:
                    code = c_code.decode('utf-8')
                    name = c_name.decode('utf-8')
                    results[name].append(code)

            return results

        finally:
            _safe_osr_release(osr)
            OSRFreeSRSArray(matches)
            CPLFree(confidences)

    def to_string(self):
        """Convert to a PROJ4 or WKT string.

        The output will be reduced as much as possible by attempting a
        match to CRS defined in authority files.

        Notes
        -----
        Mapping keys are tested against the ``all_proj_keys`` list.
        Values of ``True`` are omitted, leaving the key bare:
        {'no_defs': True} -> "+no_defs" and items where the value is
        otherwise not a str, int, or float are omitted.

        Returns
        -------
        str

        Raises
        ------
        CRSError

        """
        auth = self.to_authority()
        if auth:
            return ":".join(auth)
        else:
            return self.to_wkt() or self.to_proj4()

    @staticmethod
    def from_epsg(code):
        """Make a CRS from an EPSG code.

        Parameters
        ----------
        code : int or str
            An EPSG code. Strings will be converted to integers.

        Notes
        -----
        The input code is not validated against an EPSG database.

        Returns
        -------
        CRS

        Raises
        ------
        CRSError

        """
        cdef CRS obj = CRS.__new__(CRS)

        try:
            code = int(code)
        except OverflowError as err:
            raise CRSError(f"Not in the range of valid EPSG codes: {code}") from err
        except TypeError as err:
            raise CRSError(f"Not a valid EPSG codes: {code}") from err

        if code <= 0:
            raise CRSError("EPSG codes are positive integers")

        try:
            exc_wrap_ogrerr(exc_wrap_int(OSRImportFromEPSG(obj._osr, <int>code)))
        except OverflowError as err:
            raise CRSError(f"Not in the range of valid EPSG codes: {code}") from err
        except CPLE_BaseError as exc:
            raise CRSError(f"The EPSG code is unknown. {exc}")
        else:
            osr_set_traditional_axis_mapping_strategy(obj._osr)
            obj._epsg = code
            return obj

    @staticmethod
    def from_proj4(proj):
        """Make a CRS from a PROJ4 string.

        Parameters
        ----------
        proj : str
            A PROJ4 string like "+proj=longlat ..."

        Returns
        -------
        CRS

        Raises
        ------
        CRSError

        """
        cdef CRS obj = CRS.__new__(CRS)

        # Filter out nonsensical items that might have crept in.
        items_filtered = []
        for param in _RE_PROJ_PARAM.finditer(proj):
            value = param.group('value')
            if value is None:
                items_filtered.append(param.group())
            elif value.lower() == "false":
                continue
            else:
                items_filtered.append(param.group())
                
        proj = ' '.join(items_filtered)
        proj_b = proj.encode('utf-8')

        try:
            exc_wrap_ogrerr(exc_wrap_int(OSRImportFromProj4(obj._osr, <const char *>proj_b)))
        except CPLE_BaseError as exc:
            raise CRSError(f"The PROJ4 dict could not be understood. {exc}")
        else:
            osr_set_traditional_axis_mapping_strategy(obj._osr)
            return obj

    @staticmethod
    def from_dict(initialdata=None, **kwargs):
        """Make a CRS from a dict of PROJ parameters or PROJ JSON.

        Parameters
        ----------
        initialdata : mapping, optional
            A dictionary or other mapping
        kwargs : mapping, optional
            Another mapping. Will be overlaid on the initialdata.

        Returns
        -------
        CRS

        Raises
        ------
        CRSError

        """
        if initialdata is not None:
            data = dict(initialdata.items())
        else:
            data = {}
        data.update(**kwargs)

        if not ("init" in data or "proj" in data):
            # We've been given a PROJ JSON-encoded text.
            return CRS.from_user_input(json.dumps(data))

        # "+init=epsg:xxxx" is deprecated in GDAL. If we find this, we will
        # extract the epsg code and dispatch to from_epsg.
        if 'init' in data and data['init'].lower().startswith('epsg:'):
            epsg_code = int(data['init'].split(':')[1])
            return CRS.from_epsg(epsg_code)

        # Continue with the general case.
        pjargs = []
        for key in data.keys() & all_proj_keys:
            val = data[key]
            if val is None or val is True:
                pjargs.append(f"+{key}")
            elif val is False:
                pass
            else:
                pjargs.append(f"+{key}={val}")

        proj = ' '.join(pjargs)
        b_proj = proj.encode('utf-8')

        cdef CRS obj = CRS.__new__(CRS)

        try:
            exc_wrap_ogrerr(OSRImportFromProj4(obj._osr, <const char *>b_proj))
        except CPLE_BaseError as exc:
            raise CRSError(f"The PROJ4 dict could not be understood. {exc}")
        else:
            osr_set_traditional_axis_mapping_strategy(obj._osr)
            return obj

    @staticmethod
    def from_wkt(wkt, morph_from_esri_dialect=False):
        """Make a CRS from a WKT string.

        Parameters
        ----------
        wkt : str
            A WKT string.
        morph_from_esri_dialect : bool, optional
            If True, items in the input using Esri's dialect of WKT
            will be replaced by OGC standard equivalents.

        Returns
        -------
        CRS

        Raises
        ------
        CRSError

        """
        cdef char *wkt_c = NULL

        if not isinstance(wkt, str):
            raise ValueError("A string is expected")

        wkt_b= wkt.encode('utf-8')
        wkt_c = wkt_b

        cdef CRS obj = CRS.__new__(CRS)

        try:
            errcode = exc_wrap_ogrerr(OSRImportFromWkt(obj._osr, &wkt_c))
        except CPLE_BaseError as exc:
            raise CRSError(f"The WKT could not be parsed. {exc}")
        else:
            osr_set_traditional_axis_mapping_strategy(obj._osr)
            return obj

    @staticmethod
    def from_user_input(value, morph_from_esri_dialect=False):
        """Make a CRS from a variety of inputs.

        Parameters
        ----------
        value : object
            User input of many different kinds.
        morph_from_esri_dialect : bool, optional
            If True, items in the input using Esri's dialect of WKT
            will be replaced by OGC standard equivalents.

        Returns
        -------
        CRS

        Raises
        ------
        CRSError

        """
        cdef const char *text_c = NULL
        cdef CRS obj

        if isinstance(value, CRS):
            return value
        elif hasattr(value, "to_wkt") and callable(value.to_wkt):
            return CRS.from_wkt(value.to_wkt(), morph_from_esri_dialect=morph_from_esri_dialect)
        elif isinstance(value, int):
            return CRS.from_epsg(value)
        elif isinstance(value, DICT_TYPES):
            return CRS(**value)

        elif isinstance(value, str):
            text_b = value.encode('utf-8')
            text_c = text_b
            obj = CRS.__new__(CRS)
            try:
                errcode = exc_wrap_ogrerr(OSRSetFromUserInput(obj._osr, text_c))
            except CPLE_BaseError as exc:
                raise CRSError(f"The WKT could not be parsed. {exc}")
            else:
                osr_set_traditional_axis_mapping_strategy(obj._osr)
                return obj

        else:
            raise CRSError(f"CRS is invalid: {value!r}")

    @staticmethod
    def from_authority(auth_name, code):
        """Make a CRS from an authority name and code.

        .. versionadded:: 1.9.0

        Parameters
        ----------
        auth_name: str
            The name of the authority.
        code : int or str
            The code used by the authority.

        Returns
        -------
        CRS

        Raises
        ------
        CRSError

        """
        return CRS.from_string(f"{auth_name}:{code}")

    @staticmethod
    def from_string(value, morph_from_esri_dialect=False):
        """Make a CRS from an EPSG, PROJ, or WKT string

        Parameters
        ----------
        value : str
            An EPSG, PROJ, or WKT string.
        morph_from_esri_dialect : bool, optional
            If True, items in the input using Esri's dialect of WKT
            will be replaced by OGC standard equivalents.

        Returns
        -------
        CRS

        Raises
        ------
        CRSError

        """
        try:
            value = value.strip()
        except AttributeError:
            pass

        if not value:
            raise CRSError(f"CRS is empty or invalid: {value!r}")

        elif value.upper().startswith('EPSG:') and "+" not in value:
            auth, val = value.split(':')
            if not val:
                raise CRSError(f"Invalid CRS: {value!r}")
            return CRS.from_epsg(val)

        elif value.startswith('{') or value.startswith('['):
            # may be json, try to decode it
            try:
                val = json.loads(value, strict=False)
            except ValueError:
                raise CRSError('CRS appears to be JSON but is not valid')

            if not val:
                raise CRSError("CRS is empty JSON")
            else:
                return CRS.from_dict(**val)

        elif value.endswith("]"):
            return CRS.from_wkt(value, morph_from_esri_dialect=morph_from_esri_dialect)
        elif "=" in value:
            return CRS.from_proj4(value)
        else:
            return CRS.from_user_input(value, morph_from_esri_dialect=morph_from_esri_dialect)

    def __cinit__(self):
        self._osr = OSRNewSpatialReference(NULL)

    def __dealloc__(self):
        _safe_osr_release(self._osr)

    def __hash__(self):
        return hash(self.wkt)

    def __getitem__(self, item):
        return self.data[item]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def get(self, item):
        return self.data.get(item)

    def items(self):
        return self.data.items()

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def __bool__(self):
        return bool(self.wkt)

    __nonzero__ = __bool__

    def __getstate__(self):
        return self.to_wkt()

    def __setstate__(self, state):
        cdef CRS tmp
        tmp = CRS.from_wkt(state)
        self._osr = OSRClone(tmp._osr)
        self._wkt = tmp._wkt
        self._data = tmp.data
        self._epsg = tmp._epsg

    def __copy__(self):
        return pickle.loads(pickle.dumps(self))

    def __str__(self):
        return self.to_string()

    def __repr__(self):
        epsg_code = self.to_epsg()
        if epsg_code:
            return f"CRS.from_epsg({epsg_code})"
        else:
            return f"CRS.from_wkt('{self.wkt}')"

    def __eq__(self, other):
        cdef OGRSpatialReferenceH osr_s = NULL
        cdef OGRSpatialReferenceH osr_o = NULL
        cdef CRS crs_o

        try:
            crs_o = CRS.from_user_input(other)
        except CRSError:
            return False

        epsg_s = self.to_epsg()
        epsg_o = crs_o.to_epsg()

        if epsg_s is not None and epsg_o is not None and epsg_s == epsg_o:
            return True

        else:
            try:
                osr_s = exc_wrap_pointer(OSRClone(self._osr))
                osr_o = exc_wrap_pointer(OSRClone(crs_o._osr))
                return bool(OSRIsSame(osr_s, osr_o) == 1)

            finally:
                _safe_osr_release(osr_s)
                _safe_osr_release(osr_o)


    def _matches(self, confidence_threshold=70):
        """Find matches in authority files.

        Parameters
        ----------
        confidence_threshold : int
            Percent match confidence threshold (0-100).

        Returns
        -------
        dict : {name: [codes]}
            A dictionary in which capitalized authority names are the
            keys and lists of codes ordered by match confidence,
            descending, are the values.

        """
        cdef OGRSpatialReferenceH osr = NULL
        cdef OGRSpatialReferenceH *matches = NULL
        cdef int *confidences = NULL
        cdef int num_matches = 0
        cdef int i = 0
        cdef char *c_code = NULL
        cdef char *c_name = NULL

        results = defaultdict(list)

        try:
            osr = exc_wrap_pointer(OSRClone(self._osr))
            matches = OSRFindMatches(osr, NULL, &num_matches, &confidences)

            for i in range(num_matches):
                confidence = confidences[i]
                c_code = OSRGetAuthorityCode(matches[i], NULL)
                c_name = OSRGetAuthorityName(matches[i], NULL)

                if c_code != NULL and c_name != NULL and confidence >= confidence_threshold:
                    code = c_code.decode('utf-8')
                    name = c_name.decode('utf-8')
                    results[name].append(code)

            return results

        finally:
            _safe_osr_release(osr)
            OSRFreeSRSArray(matches)
            CPLFree(confidences)

    def _projjson(self):
        """Get a PROJ JSON representation.

        For internal use only.

        .. versionadded:: 1.9.0

        .. note:: Requires GDAL 3.1+ and PROJ 6.2+

        Returns
        -------
        projjson : str
            PROJ JSON-encoded text.

        Raises
        ------
        CRSError

        """
        cdef char *conv_json = NULL
        cdef const char* options[2]

        try:
            if OSRGetName(self._osr) != NULL:
                options[0] = b"MULTILINE=NO"
                options[1] = NULL
                exc_wrap_ogrerr(OSRExportToPROJJSON(self._osr, &conv_json, options))
        except CPLE_BaseError as exc:
            raise CRSError(f"Cannot convert to PROJ JSON. {exc}")

        else:
            if conv_json != NULL:
                return conv_json.decode('utf-8')
            else:
                return ''
        finally:
            CPLFree(conv_json)


def epsg_treats_as_latlong(input_crs):
    """Test if the CRS is in latlon order

    .. versionadded:: 1.9.0

    From GDAL docs:

    > This method returns TRUE if EPSG feels this geographic coordinate
    system should be treated as having lat/long coordinate ordering.

    > Currently this returns TRUE for all geographic coordinate systems with
    an EPSG code set, and axes set defining it as lat, long.

    > FALSE will be returned for all coordinate systems that are not
    geographic, or that do not have an EPSG code set.

    > **Note**

    > Important change of behavior since GDAL 3.0.
    In previous versions, geographic CRS imported with importFromEPSG()
    would cause this method to return FALSE on them, whereas now it returns
    TRUE, since importFromEPSG() is now equivalent to importFromEPSGA().

    Parameters
    ----------
    input_crs : CRS
        Coordinate reference system, as a fiona CRS object
        Example: CRS({'init': 'EPSG:4326'})

    Returns
    -------
    bool

    """
    cdef CRS crs

    if not isinstance(input_crs, CRS):
        crs = CRS.from_user_input(input_crs)
    else:
        crs = input_crs

    try:
        return bool(OSREPSGTreatsAsLatLong(crs._osr) == 1)
    except CPLE_BaseError as exc:
        raise CRSError(str(exc))


def epsg_treats_as_northingeasting(input_crs):
    """Test if the CRS should be treated as having northing/easting coordinate ordering

    .. versionadded:: 1.9.0

    From GDAL docs:

    > This method returns TRUE if EPSG feels this projected coordinate
    system should be treated as having northing/easting coordinate ordering.

    > Currently this returns TRUE for all projected coordinate systems with
    an EPSG code set, and axes set defining it as northing, easting.

    > FALSE will be returned for all coordinate systems that are not
    projected, or that do not have an EPSG code set.

    > **Note**

    > Important change of behavior since GDAL 3.0.
    In previous versions, projected CRS with northing, easting axis order
    imported with importFromEPSG() would cause this method to return FALSE
    on them, whereas now it returns TRUE, since importFromEPSG() is now 
    equivalent to importFromEPSGA().

    Parameters
    ----------
    input_crs : CRS
        Coordinate reference system, as a fiona CRS object
        Example: CRS({'init': 'EPSG:4326'})

    Returns
    -------
    bool

    """
    cdef CRS crs

    if not isinstance(input_crs, CRS):
        crs = CRS.from_user_input(input_crs)
    else:
        crs = input_crs

    try:
        return bool(OSREPSGTreatsAsNorthingEasting(crs._osr) == 1)
    except CPLE_BaseError as exc:
        raise CRSError(str(exc))


# Below is the big list of PROJ4 parameters from
# http://trac.osgeo.org/proj/wiki/GenParms.
# It is parsed into a list of parameter keys ``all_proj_keys``.

_param_data = """
+a         Semimajor radius of the ellipsoid axis
+alpha     ? Used with Oblique Mercator and possibly a few others
+axis      Axis orientation (new in 4.8.0)
+b         Semiminor radius of the ellipsoid axis
+datum     Datum name (see `proj -ld`)
+ellps     Ellipsoid name (see `proj -le`)
+init      Initialize from a named CRS
+k         Scaling factor (old name)
+k_0       Scaling factor (new name)
+lat_0     Latitude of origin
+lat_1     Latitude of first standard parallel
+lat_2     Latitude of second standard parallel
+lat_ts    Latitude of true scale
+lon_0     Central meridian
+lonc      ? Longitude used with Oblique Mercator and possibly a few others
+lon_wrap  Center longitude to use for wrapping (see below)
+nadgrids  Filename of NTv2 grid file to use for datum transforms (see below)
+no_defs   Don't use the /usr/share/proj/proj_def.dat defaults file
+over      Allow longitude output outside -180 to 180 range, disables wrapping (see below)
+pm        Alternate prime meridian (typically a city name, see below)
+proj      Projection name (see `proj -l`)
+south     Denotes southern hemisphere UTM zone
+to_meter  Multiplier to convert map units to 1.0m
+towgs84   3 or 7 term datum transform parameters (see below)
+units     meters, US survey feet, etc.
+vto_meter vertical conversion to meters.
+vunits    vertical units.
+x_0       False easting
+y_0       False northing
+zone      UTM zone
+a         Semimajor radius of the ellipsoid axis
+alpha     ? Used with Oblique Mercator and possibly a few others
+azi
+b         Semiminor radius of the ellipsoid axis
+belgium
+beta
+czech
+e         Eccentricity of the ellipsoid = sqrt(1 - b^2/a^2) = sqrt( f*(2-f) )
+ellps     Ellipsoid name (see `proj -le`)
+es        Eccentricity of the ellipsoid squared
+f         Flattening of the ellipsoid (often presented as an inverse, e.g. 1/298)
+gamma
+geoc
+guam
+h
+k         Scaling factor (old name)
+K
+k_0       Scaling factor (new name)
+lat_0     Latitude of origin
+lat_1     Latitude of first standard parallel
+lat_2     Latitude of second standard parallel
+lat_b
+lat_t
+lat_ts    Latitude of true scale
+lon_0     Central meridian
+lon_1
+lon_2
+lonc      ? Longitude used with Oblique Mercator and possibly a few others
+lsat
+m
+M
+n
+no_cut
+no_off
+no_rot
+ns
+o_alpha
+o_lat_1
+o_lat_2
+o_lat_c
+o_lat_p
+o_lon_1
+o_lon_2
+o_lon_c
+o_lon_p
+o_proj
+over
+p
+path
+proj      Projection name (see `proj -l`)
+q
+R
+R_a
+R_A       Compute radius such that the area of the sphere is the same as the area of the ellipsoid
+rf        Reciprocal of the ellipsoid flattening term (e.g. 298)
+R_g
+R_h
+R_lat_a
+R_lat_g
+rot
+R_V
+s
+south     Denotes southern hemisphere UTM zone
+sym
+t
+theta
+tilt
+to_meter  Multiplier to convert map units to 1.0m
+units     meters, US survey feet, etc.
+vopt
+W
+westo
+wktext
+x_0       False easting
+y_0       False northing
+zone      UTM zone
"""

all_proj_keys = set(line.split(' ', 1)[0][1:] for line in filter(None, _param_data.splitlines()))
all_proj_keys.add('no_mayo')


def from_epsg(val):
    """Given an integer code, returns an EPSG-like mapping.

    .. deprecated:: 1.9.0
       This function will be removed in version 2.0. Please use
       CRS.from_epsg() instead.

    """
    warnings.warn(
        "This function will be removed in version 2.0. Please use CRS.from_epsg() instead.",
        FionaDeprecationWarning,
        stacklevel=2,
    )
    return CRS.from_epsg(val)


def from_string(val):
    """Turn a PROJ.4 string into a mapping of parameters.

    .. deprecated:: 1.9.0
       This function will be removed in version 2.0. Please use
       CRS.from_string() instead.

    """
    warnings.warn(
        "This function will be removed in version 2.0. Please use CRS.from_string() instead.",
        FionaDeprecationWarning,
        stacklevel=2,
    )
    return CRS.from_string(val)


def to_string(val):
    """Turn a parameter mapping into a more conventional PROJ.4 string.

    .. deprecated:: 1.9.0
       This function will be removed in version 2.0. Please use
       CRS.to_string() instead.

    """
    warnings.warn(
        "This function will be removed in version 2.0. Please use CRS.to_string() instead.",
        FionaDeprecationWarning,
        stacklevel=2,
    )
    return CRS.from_user_input(val).to_string()
