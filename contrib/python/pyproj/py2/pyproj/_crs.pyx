import re
import warnings
from collections import OrderedDict

from pyproj.compat import cstrencode, pystrdecode
from pyproj._datadir cimport get_pyproj_context
from pyproj.enums import WktVersion, ProjVersion
from pyproj.exceptions import CRSError


cdef cstrdecode(const char *instring):
    if instring != NULL:
        return pystrdecode(instring)
    return None


cdef decode_or_undefined(const char* instring):
    pystr = cstrdecode(instring)
    if pystr is None:
        return "undefined"
    return pystr


def is_wkt(proj_string):
    """
    Check if the input projection string is in the Well-Known Text format.

    Parameters
    ----------
    proj_string: str
        The projection string.

    Returns
    -------
    bool: True if the string is in the Well-Known Text format
    """
    tmp_string = cstrencode(proj_string)
    return proj_context_guess_wkt_dialect(NULL, tmp_string) != PJ_GUESSED_NOT_WKT


def is_proj(proj_string):
    """
    Check if the input projection string is in the PROJ format.

    Parameters
    ----------
    proj_string: str
        The projection string.

    Returns
    -------
    bool: True if the string is in the PROJ format
    """
    return not is_wkt(proj_string) and "=" in proj_string


cdef _to_wkt(PJ_CONTEXT* projctx, PJ* projobj, version=WktVersion.WKT2_2018, pretty=False):
    """
    Convert a PJ object to a wkt string.

    Parameters
    ----------
    projctx: PJ_CONTEXT*
    projobj: PJ*
    wkt_out_type: PJ_WKT_TYPE
    pretty: bool

    Return
    ------
    str or None
    """
    # get the output WKT format
    supported_wkt_types = {
        WktVersion.WKT2_2015: PJ_WKT2_2015,
        WktVersion.WKT2_2015_SIMPLIFIED: PJ_WKT2_2015_SIMPLIFIED,
        WktVersion.WKT2_2018: PJ_WKT2_2018,
        WktVersion.WKT2_2018_SIMPLIFIED: PJ_WKT2_2018_SIMPLIFIED,
        WktVersion.WKT1_GDAL: PJ_WKT1_GDAL,
        WktVersion.WKT1_ESRI: PJ_WKT1_ESRI
    }
    cdef PJ_WKT_TYPE wkt_out_type
    wkt_out_type = supported_wkt_types[WktVersion(version)]
 
    cdef const char* options_wkt[2]
    multiline = b"MULTILINE=NO"
    if pretty:
        multiline = b"MULTILINE=YES"
    options_wkt[0] = multiline
    options_wkt[1] = NULL
    cdef const char* proj_string
    proj_string = proj_as_wkt(
        projctx,
        projobj,
        wkt_out_type,
        options_wkt)
    return cstrdecode(proj_string)


cdef _to_proj4(PJ_CONTEXT* projctx, PJ* projobj, version):
    """
    Convert the projection to a PROJ string.

    Parameters
    ----------
    version: ~pyproj.enums.ProjVersion
        The version of the PROJ string output. 

    Returns
    -------
    str: The PROJ string.
    """
    # get the output PROJ string format
    supported_prj_types = {
        ProjVersion.PROJ_4: PJ_PROJ_4,
        ProjVersion.PROJ_5: PJ_PROJ_5,
    }
    cdef PJ_PROJ_STRING_TYPE proj_out_type
    proj_out_type = supported_prj_types[ProjVersion(version)]

    # convert projection to string
    cdef const char* proj_string
    proj_string = proj_as_proj_string(
        projctx,
        projobj,
        proj_out_type,
        NULL)
    return cstrdecode(proj_string)


cdef PJ* _from_authority(
    auth_name, code, PJ_CATEGORY category, int use_proj_alternative_grid_names=False
):
    CRSError.clear()
    b_auth_name = cstrencode(auth_name)
    cdef char *c_auth_name = b_auth_name
    b_code = cstrencode(str(code))
    cdef char *c_code = b_code
    return proj_create_from_database(
        get_pyproj_context(),
        c_auth_name,
        c_code,
        category,
        use_proj_alternative_grid_names,
        NULL
    )


cdef PJ* _from_string(proj_string, expected_types):
    CRSError.clear()
    cdef PJ* base_pj = proj_create(
        get_pyproj_context(),
        cstrencode(proj_string)
    )
    if base_pj is NULL:
        return NULL

    if proj_get_type(base_pj) not in expected_types:
        proj_destroy(base_pj)
        base_pj = NULL
    return base_pj


cdef class Axis:
    """
    Coordinate System Axis

    Attributes
    ----------
    name: str
    abbrev: str
    direction: str
    unit_conversion_factor: float
    unit_name: str
    unit_auth_code: str
    unit_code: str

    """
    def __cinit__(self):
        self.name = "undefined"
        self.abbrev = "undefined"
        self.direction = "undefined"
        self.unit_conversion_factor = float("NaN")
        self.unit_name = "undefined"
        self.unit_auth_code = "undefined"
        self.unit_code = "undefined"

    def __str__(self):
        return "{abbrev}[{direction}]: {name} ({unit_name})".format(
            name=self.name,
            direction=self.direction,
            abbrev=self.abbrev,
            unit_name=self.unit_name,
        )

    def __repr__(self):
        return ("Axis(name={name}, abbrev={abbrev}, direction={direction}, "
                "unit_auth_code={unit_auth_code}, unit_code={unit_code}, "
                "unit_name={unit_name})").format(
            name=self.name,
            abbrev=self.abbrev,
            direction=self.direction,
            unit_name=self.unit_name,
            unit_auth_code=self.unit_auth_code,
            unit_code=self.unit_code,
        )

    @staticmethod
    cdef create(PJ_CONTEXT* projcontext, PJ* projobj, int index):
        cdef Axis axis_info = Axis()
        cdef const char * name = NULL
        cdef const char * abbrev = NULL
        cdef const char * direction = NULL
        cdef const char * unit_name = NULL
        cdef const char * unit_auth_code = NULL
        cdef const char * unit_code = NULL
        if not proj_cs_get_axis_info(
                projcontext,
                projobj,
                index,
                &name,
                &abbrev,
                &direction,
                &axis_info.unit_conversion_factor,
                &unit_name,
                &unit_auth_code,
                &unit_code):
            return None
        axis_info.name = decode_or_undefined(name)
        axis_info.abbrev = decode_or_undefined(abbrev)
        axis_info.direction = decode_or_undefined(direction)
        axis_info.unit_name = decode_or_undefined(unit_name)
        axis_info.unit_auth_code = decode_or_undefined(unit_auth_code)
        axis_info.unit_code = decode_or_undefined(unit_code)
        return axis_info


cdef class AreaOfUse:
    """
    Area of Use for CRS

    Attributes
    ----------
    west: float
        West bound of area of use.
    south: float
        South bound of area of use.
    east: float
        East bound of area of use.
    north: float
        North bound of area of use.
    name: str
        Name of area of use.

    """
    def __cinit__(self):
        self.west = float("NaN")
        self.south = float("NaN")
        self.east = float("NaN")
        self.north = float("NaN")
        self.name = "undefined"

    def __str__(self):
        return "- name: {name}\n" \
               "- bounds: {bounds}".format(
            name=self.name, bounds=self.bounds)

    def __repr__(self):
        return ("AreaOfUse(name={name}, west={west}, south={south},"
                " east={east}, north={north})").format(
            name=self.name,
            west=self.west,
            south=self.south,
            east=self.east,
            north=self.north
        )

    @staticmethod
    cdef create(PJ_CONTEXT* projcontext, PJ* projobj):
        cdef AreaOfUse area_of_use = AreaOfUse()
        cdef const char * area_name = NULL
        if not proj_get_area_of_use(
                projcontext,
                projobj,
                &area_of_use.west,
                &area_of_use.south,
                &area_of_use.east,
                &area_of_use.north,
                &area_name):
            return None
        area_of_use.name = decode_or_undefined(area_name)
        return area_of_use

    @property
    def bounds(self):
        return self.west, self.south, self.east, self.north


cdef class Base:
    def __cinit__(self):
        self.projobj = NULL
        self.projctx = NULL
        self.name = "undefined"

    def __dealloc__(self):
        """destroy projection definition"""
        if self.projobj != NULL:
            proj_destroy(self.projobj)
        if self.projctx != NULL:
            proj_context_destroy(self.projctx)

    def __init__(self):
        self.projctx = get_pyproj_context()

    def _set_name(self):
        """
        Set the name of the PJ
        """
        # get proj information
        cdef const char* proj_name = proj_get_name(self.projobj)
        self.name = decode_or_undefined(proj_name)

    def to_wkt(self, version="WKT2_2018", pretty=False):
        """
        Convert the projection to a WKT string.

        Version options:
          - WKT2_2015
          - WKT2_2015_SIMPLIFIED
          - WKT2_2018
          - WKT2_2018_SIMPLIFIED
          - WKT1_GDAL
          - WKT1_ESRI


        Parameters
        ----------
        version: ~pyproj.enums.WktVersion
            The version of the WKT output.
            Default is :attr:`~pyproj.enums.WktVersion.WKT2_2018`.
        pretty: bool
            If True, it will set the output to be a multiline string. Defaults to False.
 
        Returns
        -------
        str: The WKT string.
        """
        return _to_wkt(self.projctx, self.projobj, version, pretty=pretty)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.to_wkt(pretty=True)

    def _is_exact_same(self, Base other):
        return proj_is_equivalent_to(
            self.projobj, other.projobj, PJ_COMP_STRICT) == 1

    def _is_equivalent(self, Base other):
        return proj_is_equivalent_to(
            self.projobj, other.projobj, PJ_COMP_EQUIVALENT) == 1

    def __eq__(self, other):
        if not isinstance(other, Base):
            return False
        return self._is_equivalent(other)

    def is_exact_same(self, other):
        """Compares projection objects to see if they are exactly the same."""
        if not isinstance(other, Base):
            return False
        return self._is_exact_same(other)


_COORD_SYSTEM_TYPE_MAP = {
    PJ_CS_TYPE_UNKNOWN: "unknown",
    PJ_CS_TYPE_CARTESIAN: "cartesian",
    PJ_CS_TYPE_ELLIPSOIDAL: "ellipsoidal",
    PJ_CS_TYPE_VERTICAL: "vertical",
    PJ_CS_TYPE_SPHERICAL: "spherical",
    PJ_CS_TYPE_ORDINAL: "ordinal",
    PJ_CS_TYPE_PARAMETRIC: "parametric",
    PJ_CS_TYPE_DATETIMETEMPORAL: "datetimetemporal",
    PJ_CS_TYPE_TEMPORALCOUNT: "temporalcount",
    PJ_CS_TYPE_TEMPORALMEASURE: "temporalmeasure",
}

cdef class CoordinateSystem(Base):
    """
    Coordinate System for CRS

    Attributes
    ----------
    name: str
        The name of the coordinate system.

    """
    def __cinit__(self):
        self._axis_list = None

    @staticmethod
    cdef create(PJ* coord_system_pj):
        cdef CoordinateSystem coord_system = CoordinateSystem()
        coord_system.projobj = coord_system_pj
        cdef PJ_COORDINATE_SYSTEM_TYPE cs_type = proj_cs_get_type(
            coord_system.projctx,
            coord_system.projobj,
        )
        try:
            coord_system.name = _COORD_SYSTEM_TYPE_MAP[cs_type]
        except KeyError:
            raise CRSError("Not a coordinate system.")
        return coord_system

    @property
    def axis_list(self):
        """
        Returns
        -------
        list[Axis]: The Axis list for the coordinate system.
        """
        if self._axis_list is not None:
            return self._axis_list
        self._axis_list = []
        cdef int num_axes = 0
        num_axes = proj_cs_get_axis_count(
            self.projctx,
            self.projobj
        )
        for axis_idx from 0 <= axis_idx < num_axes:
            self._axis_list.append(
                Axis.create(
                    self.projctx,
                    self.projobj,
                    axis_idx
                )
            )
        return self._axis_list


cdef class Ellipsoid(Base):
    """
    Ellipsoid for CRS

    Attributes
    ----------
    name: str
        The name of the ellipsoid.
    is_semi_minor_computed: int
        1 if True, 0 if False
    ellipsoid_loaded: bool
        True if it is loaded without errors.

    """
    def __cinit__(self):
        # load in ellipsoid information if applicable
        self._semi_major_metre = float("NaN")
        self._semi_minor_metre = float("NaN")
        self.is_semi_minor_computed = False
        self._inv_flattening = float("NaN")
        self.ellipsoid_loaded = False

    @staticmethod
    cdef create(PJ* ellipsoid_pj):
        cdef Ellipsoid ellips = Ellipsoid()
        ellips.projobj = ellipsoid_pj
        cdef int is_semi_minor_computed = 0
        try:
            proj_ellipsoid_get_parameters(
                ellips.projctx,
                ellips.projobj,
                &ellips._semi_major_metre,
                &ellips._semi_minor_metre,
                &is_semi_minor_computed,
                &ellips._inv_flattening)
            ellips.ellipsoid_loaded = True
            ellips.is_semi_minor_computed = is_semi_minor_computed == 1
        except Exception:
            pass
        ellips._set_name()
        return ellips

    @staticmethod
    def from_authority(auth_name, code):
        """
        Create an Ellipsoid from an authority code.

        Parameters
        ----------
        auth_name: str
            Name ot the authority.
        code: str or int
            The code used by the authority.

        Returns
        -------
        Ellipsoid
        """
        cdef PJ* ellipsoid_pj = _from_authority(
            auth_name,
            code,
            PJ_CATEGORY_ELLIPSOID,
        )
        if ellipsoid_pj == NULL:
            raise CRSError(
                "Invalid authority or code ({0}, {1})".format(auth_name, code)
            )
        return Ellipsoid.create(ellipsoid_pj)

    @staticmethod
    def from_epsg(code):
        """
        Create an Ellipsoid from an EPSG code.

        Parameters
        ----------
        code: str or int
            The code used by the EPSG.

        Returns
        -------
        Ellipsoid
        """
        return Ellipsoid.from_authority("EPSG", code)

    @staticmethod
    def from_string(ellipsoid_string):
        """
        Create an Ellipsoid from a string.

        Examples:
          - urn:ogc:def:ellipsoid:EPSG::7001
          - ELLIPSOID["Airy 1830",6377563.396,299.3249646,
            LENGTHUNIT["metre",1],
            ID["EPSG",7001]]

        Parameters
        ----------
        ellipsoid_string: str
            Ellipsoid string.

        Returns
        -------
        Ellipsoid
        """
        cdef PJ* ellipsoid_pj = _from_string(
            ellipsoid_string,
            (PJ_TYPE_ELLIPSOID,)
        )
        if ellipsoid_pj == NULL:
            raise CRSError(
                "Invalid ellipsoid string: {}".format(
                    pystrdecode(ellipsoid_string)
                )
            )

        return Ellipsoid.create(ellipsoid_pj)

    @property
    def semi_major_metre(self):
        """
        The ellipsoid semi major metre.

        Returns
        -------
        float or None: The semi major metre if the projection is an ellipsoid.
        """
        if self.ellipsoid_loaded:
            return self._semi_major_metre
        return float("NaN")

    @property
    def semi_minor_metre(self):
        """
        The ellipsoid semi minor metre.

        Returns
        -------
        float or None: The semi minor metre if the projection is an ellipsoid
            and the value was com
            puted.
        """
        if self.ellipsoid_loaded and self.is_semi_minor_computed:
            return self._semi_minor_metre
        return float("NaN")

    @property
    def inverse_flattening(self):
        """
        The ellipsoid inverse flattening.

        Returns
        -------
        float or None: The inverse flattening if the projection is an ellipsoid.
        """
        if self.ellipsoid_loaded:
            return self._inv_flattening
        return float("NaN")


cdef class PrimeMeridian(Base):
    """
    Prime Meridian for CRS

    Attributes
    ----------
    name: str
        The name of the prime meridian.
    unit_name: str
        The unit name for the prime meridian.

    """
    def __cinit__(self):
        self.unit_name = None

    @staticmethod
    cdef create(PJ* prime_meridian_pj):
        cdef PrimeMeridian prime_meridian = PrimeMeridian()
        prime_meridian.projobj = prime_meridian_pj
        cdef const char * unit_name
        proj_prime_meridian_get_parameters(
            prime_meridian.projctx,
            prime_meridian.projobj,
            &prime_meridian.longitude,
            &prime_meridian.unit_conversion_factor,
            &unit_name,
        )
        prime_meridian.unit_name = decode_or_undefined(unit_name)
        prime_meridian._set_name()
        return prime_meridian

    @staticmethod
    def from_authority(auth_name, code):
        """
        Create a PrimeMeridian from an authority code.

        Parameters
        ----------
        auth_name: str
            Name ot the authority.
        code: str or int
            The code used by the authority.

        Returns
        -------
        PrimeMeridian
        """
        cdef PJ* prime_meridian_pj = _from_authority(
            auth_name,
            code,
            PJ_CATEGORY_PRIME_MERIDIAN,
        )
        if prime_meridian_pj == NULL:
            raise CRSError(
                "Invalid authority or code ({0}, {1})".format(auth_name, code)
            )
        return PrimeMeridian.create(prime_meridian_pj)

    @staticmethod
    def from_epsg(code):
        """
        Create a PrimeMeridian from an EPSG code.

        Parameters
        ----------
        code: str or int
            The code used by EPSG.

        Returns
        -------
        PrimeMeridian
        """
        return PrimeMeridian.from_authority("EPSG", code)


    @staticmethod
    def from_string(prime_meridian_string):
        """
        Create an PrimeMeridian from a string.

        Examples:
          - urn:ogc:def:meridian:EPSG::8901
          - PRIMEM["Greenwich",0,
            ANGLEUNIT["degree",0.0174532925199433],
            ID["EPSG",8901]]

        Parameters
        ----------
        prime_meridian_string: str
            prime meridian string.

        Returns
        -------
        PrimeMeridian
        """
        cdef PJ* prime_meridian_pj = _from_string(
            prime_meridian_string,
            (PJ_TYPE_PRIME_MERIDIAN,)
        )
        if prime_meridian_pj == NULL:
            raise CRSError(
                "Invalid prime meridian string: {}".format(
                    pystrdecode(prime_meridian_string)
                )
            )

        return PrimeMeridian.create(prime_meridian_pj)


cdef class Datum(Base):
    """
    Datum for CRS. If it is a compound CRS it is the horizontal datum.

    Attributes
    ----------
    name: str
        The name of the datum.

    """
    def __cinit__(self):
        self._ellipsoid = None
        self._prime_meridian = None

    @staticmethod
    cdef create(PJ* datum_pj):
        cdef Datum datum = Datum()
        datum.projobj = datum_pj
        datum._set_name()
        return datum

    @staticmethod
    def from_authority(auth_name, code):
        """
        Create a Datum from an authority code.

        Parameters
        ----------
        auth_name: str
            Name ot the authority.
        code: str or int
            The code used by the authority.

        Returns
        -------
        Datum
        """
        cdef PJ* datum_pj = _from_authority(
            auth_name,
            code,
            PJ_CATEGORY_DATUM,
        )
        if datum_pj == NULL:
            raise CRSError(
                "Invalid authority or code ({0}, {1})".format(auth_name, code)
            )
        return Datum.create(datum_pj)

    @staticmethod
    def from_epsg(code):
        """
        Create a Datum from an EPSG code.

        Parameters
        ----------
        code: str or int
            The code used by EPSG.

        Returns
        -------
        Datum
        """
        return Datum.from_authority("EPSG", code)

    @staticmethod
    def from_string(datum_string):
        """
        Create a Datum from a string.

        Examples:
          - urn:ogc:def:datum:EPSG::6326
          - DATUM["World Geodetic System 1984",
            ELLIPSOID["WGS 84",6378137,298.257223563,
            LENGTHUNIT["metre",1]],
            ID["EPSG",6326]]

        Parameters
        ----------
        datum_string: str
            Datum string.

        Returns
        -------
        Datum
        """
        cdef PJ* datum_pj = _from_string(
            datum_string,
            (
                PJ_TYPE_GEODETIC_REFERENCE_FRAME,
                PJ_TYPE_DYNAMIC_GEODETIC_REFERENCE_FRAME,
                PJ_TYPE_VERTICAL_REFERENCE_FRAME,
                PJ_TYPE_DYNAMIC_VERTICAL_REFERENCE_FRAME,
                PJ_TYPE_DATUM_ENSEMBLE,
            )
        )
        if datum_pj == NULL:
            raise CRSError(
                "Invalid datum string: {}".format(
                    pystrdecode(datum_string)
                )
            )

        return Datum.create(datum_pj)

    @property
    def ellipsoid(self):
        """
        Returns
        -------
        Ellipsoid: The ellipsoid object with associated attributes.
        """
        if self._ellipsoid is not None:
            return None if self._ellipsoid is False else self._ellipsoid
        cdef PJ* ellipsoid_pj = proj_get_ellipsoid(self.projctx, self.projobj)
        if ellipsoid_pj == NULL:
            self._ellipsoid = False
            return None
        self._ellipsoid = Ellipsoid.create(ellipsoid_pj)
        return self._ellipsoid

    @property
    def prime_meridian(self):
        """
        Returns
        -------
        PrimeMeridian: The CRS prime meridian object with associated attributes.
        """
        if self._prime_meridian is not None:
            return None if self._prime_meridian is False else self._prime_meridian
        cdef PJ* prime_meridian_pj = proj_get_prime_meridian(self.projctx, self.projobj)
        if prime_meridian_pj == NULL:
            self._prime_meridian = False
            return None
        self._prime_meridian = PrimeMeridian.create(prime_meridian_pj)
        return self._prime_meridian


cdef class Param:
    """
    Coordinate operation parameter.

    Attributes
    ----------
    name: str
        The name of the parameter.
    auth_name: str
        The authority name of the parameter (i.e. EPSG).
    code: str
        The code of the parameter (i.e. 9807).
    value: str or double
        The value of the parameter.
    unit_conversion_factor: double
        The factor to convert to meters.
    unit_name: str
        The name of the unit.
    unit_auth_name: str
        The authority name of the unit (i.e. EPSG).
    unit_code: str
        The code of the unit (i.e. 9807).
    unit_category: str
        The category of the unit (“unknown”, “none”, “linear”, 
        “angular”, “scale”, “time” or “parametric”).

    """
    def __cinit__(self):
        self.name = "undefined"
        self.auth_name = "undefined"
        self.code = "undefined"
        self.value = "undefined"
        self.unit_conversion_factor = float("nan")
        self.unit_name = "undefined"
        self.unit_auth_name = "undefined"
        self.unit_code = "undefined"
        self.unit_category = "undefined"

    @staticmethod
    cdef create(PJ_CONTEXT* projcontext, PJ* projobj, int param_idx):
        cdef Param param = Param()
        cdef char *out_name
        cdef char *out_auth_name
        cdef char *out_code
        cdef char *out_value
        cdef char *out_value_string
        cdef char *out_unit_name
        cdef char *out_unit_auth_name
        cdef char *out_unit_code
        cdef char *out_unit_category
        cdef double value_double
        proj_coordoperation_get_param(
            projcontext,
            projobj,
            param_idx,
            &out_name,
            &out_auth_name,
            &out_code,
            &value_double,
            &out_value_string,
            &param.unit_conversion_factor,
            &out_unit_name,
            &out_unit_auth_name,
            &out_unit_code,
            &out_unit_category
        )
        param.name = decode_or_undefined(out_name)
        param.auth_name = decode_or_undefined(out_auth_name)
        param.code = decode_or_undefined(out_code)
        param.unit_name = decode_or_undefined(out_unit_name)
        param.unit_auth_name = decode_or_undefined(out_unit_auth_name)
        param.unit_code = decode_or_undefined(out_unit_code)
        param.unit_category = decode_or_undefined(out_unit_category)
        value_string = cstrdecode(out_value_string)
        param.value = value_double if value_string is None else value_string
        return param

    def __str__(self):
        return "{auth_name}:{auth_code}".format(self.auth_name, self.auth_code)

    def __repr__(self):
        return ("Param(name={name}, auth_name={auth_name}, code={code}, "
                "value={value}, unit_name={unit_name}, unit_auth_name={unit_auth_name}, "
                "unit_code={unit_code}, unit_category={unit_category})").format(
            name=self.name,
            auth_name=self.auth_name,
            code=self.code,
            value=self.value,
            unit_name=self.unit_name,
            unit_auth_name=self.unit_auth_name,
            unit_code=self.unit_code,
            unit_category=self.unit_category,
        )



cdef class Grid:
    """
    Coordinate operation grid.

    Attributes
    ----------
    short_name: str
        The short name of the grid.
    full_name: str
        The full name of the grid.
    package_name: str
        The the package name where the grid might be found.
    url: str
        The grid URL or the package URL where the grid might be found.
    direct_download: int
        If 1, *url* can be downloaded directly.
    open_license: int
        If 1, the grid is released with an open license.
    available: int
        If 1, the grid is available at runtime. 

    """
    def __cinit__(self):
        self.short_name = "undefined"
        self.full_name = "undefined"
        self.package_name = "undefined"
        self.url = "undefined"
        self.direct_download = False
        self.open_license = False
        self.available = False

    @staticmethod
    cdef create(PJ_CONTEXT* projcontext, PJ* projobj, int grid_idx):
        cdef Grid grid = Grid()
        cdef char *out_short_name
        cdef char *out_full_name
        cdef char *out_package_name
        cdef char *out_url
        cdef int direct_download = 0
        cdef int open_license = 0
        cdef int available = 0
        proj_coordoperation_get_grid_used(
            projcontext,
            projobj,
            grid_idx,
            &out_short_name,
            &out_full_name,
            &out_package_name,
            &out_url,
            &direct_download,
            &open_license,
            &available
        )
        grid.short_name = decode_or_undefined(out_short_name)
        grid.full_name = decode_or_undefined(out_full_name)
        grid.package_name = decode_or_undefined(out_package_name)
        grid.url = decode_or_undefined(out_url)
        grid.direct_download = direct_download == 1
        grid.open_license = open_license == 1
        grid.available = available == 1
        return grid

    def __str__(self):
        return self.full_name

    def __repr__(self):
        return ("Grid(short_name={short_name}, full_name={full_name}, package_name={package_name}, "
                "url={url}, direct_download={direct_download}, open_license={open_license}, "
                "available={available})").format(
            short_name=self.short_name,
            full_name=self.full_name,
            package_name=self.package_name,
            url=self.url,
            direct_download=self.direct_download,
            open_license=self.open_license,
            available=self.available
        )


cdef class CoordinateOperation(Base):
    """
    Coordinate operation for CRS.

    Attributes
    ----------
    name: str
        The name of the method(projection) with authority information.
    method_name: str
        The method (projection) name.
    method_auth_name: str
        The method authority name.
    method_code: str
        The method code.
    is_instantiable: int
        If 1, a coordinate operation can be instantiated as a PROJ pipeline.
        This also checks that referenced grids are available. 
    has_ballpark_transformation: int
        If 1, the coordinate operation has a “ballpark” transformation, 
        that is a very approximate one, due to lack of more accurate transformations. 
    accuracy: float
        The accuracy (in metre) of a coordinate operation. 

    """
    def __cinit__(self):
        self._params = None
        self._grids = None
        self.method_name = "undefined"
        self.method_auth_name = "undefined"
        self.method_code = "undefined"
        self.is_instantiable = False
        self.has_ballpark_transformation = False
        self.accuracy = float("nan")
        self._towgs84 = None

    @staticmethod
    cdef create(PJ* coord_operation_pj):
        cdef CoordinateOperation coord_operation = CoordinateOperation()
        coord_operation.projobj = coord_operation_pj
        cdef char *out_method_name = NULL
        cdef char *out_method_auth_name = NULL
        cdef char *out_method_code = NULL
        proj_coordoperation_get_method_info(
            coord_operation.projctx,
            coord_operation.projobj,
            &out_method_name,
            &out_method_auth_name,
            &out_method_code
        )
        coord_operation._set_name()
        coord_operation.method_name = decode_or_undefined(out_method_name)
        coord_operation.method_auth_name = decode_or_undefined(out_method_auth_name)
        coord_operation.method_code = decode_or_undefined(out_method_code)
        coord_operation.accuracy = proj_coordoperation_get_accuracy(
            coord_operation.projctx,
            coord_operation.projobj
        )
        coord_operation.is_instantiable = proj_coordoperation_is_instantiable(
            coord_operation.projctx,
            coord_operation.projobj
        ) == 1
        coord_operation.has_ballpark_transformation = \
            proj_coordoperation_has_ballpark_transformation(
                coord_operation.projctx,
                coord_operation.projobj
            ) == 1

        return coord_operation

    @staticmethod
    def from_authority(auth_name, code, use_proj_alternative_grid_names=False):
        """
        Create a CoordinateOperation from an authority code.

        Parameters
        ----------
        auth_name: str
            Name ot the authority.
        code: str or int
            The code used by the authority.
        use_proj_alternative_grid_names: bool, optional
            Use the PROJ alternative grid names. Default is False.

        Returns
        -------
        CoordinateOperation
        """
        cdef PJ* coord_operation_pj = _from_authority(
            auth_name,
            code,
            PJ_CATEGORY_COORDINATE_OPERATION,
            use_proj_alternative_grid_names,
        )
        if coord_operation_pj == NULL:
            raise CRSError(
                "Invalid authority or code ({0}, {1})".format(auth_name, code)
            )
        return CoordinateOperation.create(coord_operation_pj)

    @staticmethod
    def from_epsg(code, use_proj_alternative_grid_names=False):
        """
        Create a CoordinateOperation from an EPSG code.

        Parameters
        ----------
        code: str or int
            The code used by EPSG.
        use_proj_alternative_grid_names: bool, optional
            Use the PROJ alternative grid names. Default is False.

        Returns
        -------
        CoordinateOperation
        """
        return CoordinateOperation.from_authority(
            "EPSG", code, use_proj_alternative_grid_names
        )

    @staticmethod
    def from_string(coordinate_operation_string):
        """
        Create a CoordinateOperation from a string.

        Example:
          - urn:ogc:def:coordinateOperation:EPSG::1671

        Parameters
        ----------
        coordinate_operation_string: str
            Coordinate operation string.

        Returns
        -------
        CoordinateOperation
        """
        cdef PJ* coord_operation_pj = _from_string(
            coordinate_operation_string,
            (
                PJ_TYPE_CONVERSION,
                PJ_TYPE_TRANSFORMATION,
                PJ_TYPE_CONCATENATED_OPERATION,
                PJ_TYPE_OTHER_COORDINATE_OPERATION,
            )
        )
        if coord_operation_pj == NULL:
            raise CRSError(
                "Invalid coordinate operation string: {}".format(
                    pystrdecode(coordinate_operation_string)
                )
            )

        return CoordinateOperation.create(coord_operation_pj)

    @property
    def params(self):
        """
        Returns
        -------
        list[Param]: The coordinate operation parameters.
        """
        if self._params is not None:
            return self._params
        self._params = []
        cdef int num_params = 0
        num_params = proj_coordoperation_get_param_count(
            self.projctx,
            self.projobj
        )
        for param_idx from 0 <= param_idx < num_params:
            self._params.append(
                Param.create(
                    self.projctx,
                    self.projobj,
                    param_idx
                )
            )
        return self._params

    @property
    def grids(self):
        """
        Returns
        -------
        list[Grid]: The coordinate operation grids.
        """
        if self._grids is not None:
            return self._grids
        self._grids = []
        cdef int num_grids = 0
        num_grids = proj_coordoperation_get_grid_used_count(
            self.projctx,
            self.projobj
        )
        for grid_idx from 0 <= grid_idx < num_grids:
            self._grids.append(
                Grid.create(
                    self.projctx,
                    self.projobj,
                    grid_idx
                )
            )
        return self._grids

    def to_proj4(self, version=ProjVersion.PROJ_5):
        """
        Convert the projection to a PROJ string.

        Parameters
        ----------
        version: ~pyproj.enums.ProjVersion
            The version of the PROJ string output. 
            Default is :attr:`~pyproj.enums.ProjVersion.PROJ_5`.

        Returns
        -------
        str: The PROJ string.
        """
        return _to_proj4(self.projctx, self.projobj, version)

    @property
    def towgs84(self):
        """
        Returns
        -------
        list(float): A list of 3 or 7 towgs84 values if they exist.
            Otherwise an empty list.
        """
        if self._towgs84 is not None:
            return self._towgs84
        towgs84_dict = OrderedDict(
            (
                ('X-axis translation', None), 
                ('Y-axis translation', None),
                ('Z-axis translation', None),
                ('X-axis rotation', None),
                ('Y-axis rotation', None),
                ('Z-axis rotation', None),
                ('Scale difference', None),
            )
        )
        for param in self.params:
            if param.name in towgs84_dict:
                towgs84_dict[param.name] = param.value
        self._towgs84 = [val for val in towgs84_dict.values() if val is not None]
        return self._towgs84


_CRS_TYPE_MAP = {
    PJ_TYPE_UNKNOWN: "Unknown CRS",
    PJ_TYPE_CRS: "CRS",
    PJ_TYPE_GEODETIC_CRS: "Geodetic CRS",
    PJ_TYPE_GEOCENTRIC_CRS: "Geocentric CRS",
    PJ_TYPE_GEOGRAPHIC_CRS: "Geographic CRS",
    PJ_TYPE_GEOGRAPHIC_2D_CRS: "Geographic 2D CRS",
    PJ_TYPE_GEOGRAPHIC_3D_CRS: "Geographic 3D CRS",
    PJ_TYPE_VERTICAL_CRS: "Vertical CRS",
    PJ_TYPE_PROJECTED_CRS: "Projected CRS",
    PJ_TYPE_COMPOUND_CRS: "Compound CRS",
    PJ_TYPE_TEMPORAL_CRS: "Temporal CRS",
    PJ_TYPE_ENGINEERING_CRS: "Engineering CRS",
    PJ_TYPE_BOUND_CRS: "Bound CRS",
    PJ_TYPE_OTHER_CRS: "Other CRS",
}



cdef class _CRS(Base):
    """
    The cython CRS class to be used as the base for the
    python CRS class.
    """
    def __cinit__(self):
        self._ellipsoid = None
        self._area_of_use = None
        self._prime_meridian = None
        self._datum = None
        self._sub_crs_list = None
        self._source_crs = None
        self._target_crs = None
        self._geodetic_crs = None
        self._coordinate_system = None
        self._coordinate_operation = None
        self.type_name = "undefined"

    def __init__(self, proj_string):
        self.projctx = get_pyproj_context()
        # initialize projection
        self.projobj = proj_create(self.projctx, cstrencode(proj_string))
        if self.projobj is NULL:
            raise CRSError(
                "Invalid projection: {}".format(pystrdecode(proj_string)))
        # make sure the input is a CRS
        if not proj_is_crs(self.projobj):
            raise CRSError("Input is not a CRS: {}".format(proj_string))
        # set proj information
        self.srs = pystrdecode(proj_string)
        self._type = proj_get_type(self.projobj)
        self.type_name = _CRS_TYPE_MAP[self._type]
        self._set_name()

    @property
    def axis_info(self):
        """
        Returns
        -------
        list[Axis]: The list of axis information.
        """
        return self.coordinate_system.axis_list if self.coordinate_system else []

    @property
    def area_of_use(self):
        """
        Returns
        -------
        AreaOfUse: The area of use object with associated attributes.
        """
        if self._area_of_use is not None:
            return self._area_of_use
        self._area_of_use = AreaOfUse.create(self.projctx, self.projobj)
        return self._area_of_use

    @property
    def ellipsoid(self):
        """
        Returns
        -------
        Ellipsoid: The ellipsoid object with associated attributes.
        """
        if self._ellipsoid is not None:
            return None if self._ellipsoid is False else self._ellipsoid
        cdef PJ* ellipsoid_pj = proj_get_ellipsoid(self.projctx, self.projobj)
        if ellipsoid_pj == NULL:
            self._ellipsoid = False
            return None
        self._ellipsoid = Ellipsoid.create(ellipsoid_pj)
        return self._ellipsoid

    @property
    def prime_meridian(self):
        """
        Returns
        -------
        PrimeMeridian: The CRS prime meridian object with associated attributes.
        """
        if self._prime_meridian is not None:
            return None if self._prime_meridian is True else self._prime_meridian
        cdef PJ* prime_meridian_pj = proj_get_prime_meridian(self.projctx, self.projobj)
        if prime_meridian_pj == NULL:
            self._prime_meridian = False
            return None
        self._prime_meridian = PrimeMeridian.create(prime_meridian_pj)
        return self._prime_meridian

    @property
    def datum(self):
        """
        Returns
        -------
        Datum: The datum.
        """
        if self._datum is not None:
            return None if self._datum is False else self._datum
        cdef PJ* datum_pj = proj_crs_get_datum(self.projctx, self.projobj)
        if datum_pj == NULL:
            datum_pj = proj_crs_get_horizontal_datum(self.projctx, self.projobj)
        if datum_pj == NULL:
            self._datum = False
            return None
        self._datum = Datum.create(datum_pj)
        return self._datum

    @property
    def coordinate_system(self):
        """
        Returns
        -------
        CoordinateSystem: The coordinate system.
        """
        if self._coordinate_system is not None:
            return None if self._coordinate_system is False else self._coordinate_system

        cdef PJ* coord_system_pj = proj_crs_get_coordinate_system(
            self.projctx,
            self.projobj
        )
        if coord_system_pj == NULL:
            self._coordinate_system = False
            return None

        self._coordinate_system = CoordinateSystem.create(coord_system_pj)
        return self._coordinate_system

    @property
    def coordinate_operation(self):
        """
        Returns
        -------
        CoordinateOperation: The coordinate operation.
        """
        if self._coordinate_operation is not None:
            return None if self._coordinate_operation is False else self._coordinate_operation
        cdef PJ* coord_pj = NULL
        coord_pj = proj_crs_get_coordoperation(
            self.projctx,
            self.projobj
        )
        if coord_pj == NULL:
            self._coordinate_operation = False
            return None
        self._coordinate_operation = CoordinateOperation.create(coord_pj)
        return self._coordinate_operation

    @property
    def source_crs(self):
        """
        Returns
        -------
        CRS: The the base CRS of a BoundCRS or a DerivedCRS/ProjectedCRS,
            or the source CRS of a CoordinateOperation.
        """
        if self._source_crs is not None:
            return None if self._source_crs is False else self._source_crs
        cdef PJ * projobj
        projobj = proj_get_source_crs(self.projctx, self.projobj)
        if projobj == NULL:
            self._source_crs = False
            return None
        try:
            self._source_crs = self.__class__(_to_wkt(self.projctx, projobj))
        finally:
            proj_destroy(projobj)
        return self._source_crs

    @property
    def target_crs(self):
        """
        Returns
        -------
        CRS: The hub CRS of a BoundCRS or the target CRS of a CoordinateOperation.
        """
        if self._target_crs is not None:
            return None if self._target_crs is False else self._target_crs
        cdef PJ * projobj
        projobj = proj_get_target_crs(self.projctx, self.projobj)
        if projobj == NULL:
            self._target_crs = False
            return None
        try:
            self._target_crs = self.__class__(_to_wkt(self.projctx, projobj))
        finally:
            proj_destroy(projobj)
        return self._target_crs

    @property
    def sub_crs_list(self):
        """
        If the CRS is a compound CRS, it will return a list of sub CRS objects.

        Returns
        -------
        list[CRS]
        """
        if self._sub_crs_list is not None:
            return self._sub_crs_list
        cdef int iii = 0
        cdef PJ * projobj = proj_crs_get_sub_crs(self.projctx, self.projobj, iii)
        self._sub_crs_list = []
        while projobj != NULL:
            try:
                self._sub_crs_list.append(self.__class__(_to_wkt(self.projctx, projobj)))
            finally:
                proj_destroy(projobj) # deallocate temp proj
            iii += 1
            projobj = proj_crs_get_sub_crs(self.projctx, self.projobj, iii)

        return self._sub_crs_list

    @property
    def geodetic_crs(self):
        """
        Returns
        -------
        pyproj.CRS: The the geodeticCRS / geographicCRS from the CRS.
        """
        if self._geodetic_crs is not None:
            return self._geodetic_crs if self. _geodetic_crs is not False else None
        cdef PJ * projobj
        projobj = proj_crs_get_geodetic_crs(self.projctx, self.projobj)
        if projobj == NULL:
            self._geodetic_crs = False
            return None
        try:
            self._geodetic_crs = self.__class__(_to_wkt(self.projctx, projobj))
            return self._geodetic_crs
        finally:
            proj_destroy(projobj) # deallocate temp proj


    def to_proj4(self, version=ProjVersion.PROJ_4):
        """
        Convert the projection to a PROJ string.

        .. warning:: You will likely lose important projection
          information when converting to a PROJ string from
          another format. See: https://proj.org/faq.html#what-is-the-best-format-for-describing-coordinate-reference-systems

        Parameters
        ----------
        version: ~pyproj.enums.ProjVersion
            The version of the PROJ string output. 
            Default is :attr:`~pyproj.enums.ProjVersion.PROJ_4`.

        Returns
        -------
        str: The PROJ string.
        """
        return _to_proj4(self.projctx, self.projobj, version)

    def to_geodetic(self):
        """
        .. note:: This is replaced with :attr:`CRS.geodetic_crs`

        Returns
        -------
        pyproj.CRS: The geodetic CRS from this CRS.
        """
        warnings.warn(
            "This method is deprecated an has been replaced with `CRS.geodetic_crs`.",
            DeprecationWarning,
        )
        return self.geodetic_crs

    def to_epsg(self, min_confidence=70):
        """
        Return the EPSG code best matching the CRS
        or None if it a match is not found.

        Example: 

        >>> from pyproj import CRS
        >>> ccs = CRS("epsg:4328")
        >>> ccs.to_epsg()
        4328

        If the CRS is bound, you can attempt to get an epsg code from
        the source CRS:

        >>> from pyproj import CRS
        >>> ccs = CRS("+proj=geocent +datum=WGS84 +towgs84=0,0,0")
        >>> ccs.to_epsg()
        >>> ccs.source_crs.to_epsg()
        4978
        >>> ccs == CRS.from_epsg(4978)
        False

        Parameters
        ----------
        min_confidence: int, optional
            A value between 0-100 where 100 is the most confident. Default is 70.

        Returns
        -------
        int or None: The best matching EPSG code matching the confidence level.
        """
        auth_info = self.to_authority(
            auth_name="EPSG",
            min_confidence=min_confidence
        )
        if auth_info is not None and auth_info[0].upper() == "EPSG":
            return int(auth_info[1])
        return None
    
    def to_authority(self, auth_name=None, min_confidence=70):
        """
        Return the authority name and code best matching the CRS
        or None if it a match is not found.

        Example: 

        >>> from pyproj import CRS
        >>> ccs = CRS("epsg:4328")
        >>> ccs.to_authority()
        ('EPSG', '4328')

        If the CRS is bound, you can get an authority from
        the source CRS:

        >>> from pyproj import CRS
        >>> ccs = CRS("+proj=geocent +datum=WGS84 +towgs84=0,0,0")
        >>> ccs.to_authority()
        >>> ccs.source_crs.to_authority()
        ('EPSG', '4978')
        >>> ccs == CRS.from_authorty('EPSG', '4978')
        False

        Parameters
        ----------
        auth_name: str, optional
            The name of the authority to filter by.
        min_confidence: int, optional
            A value between 0-100 where 100 is the most confident. Default is 70.

        Returns
        -------
        tuple(str, str) or None: The best matching (<auth_name>, <code>)
            matching the confidence level.
        """
        # get list of possible matching projections
        cdef PJ_OBJ_LIST *proj_list = NULL
        cdef int *out_confidence_list = NULL
        cdef int out_confidence = -9999
        cdef int num_proj_objects = -9999
        cdef char *user_auth_name = NULL

        if auth_name is not None:
            b_auth_name = cstrencode(auth_name)
            user_auth_name = b_auth_name

        try:
            proj_list  = proj_identify(self.projctx,
                self.projobj,
                user_auth_name,
                NULL,
                &out_confidence_list
            )
            if proj_list != NULL:
                num_proj_objects = proj_list_get_count(proj_list)
            if out_confidence_list != NULL and num_proj_objects > 0:
                out_confidence = out_confidence_list[0]
        finally:
            if out_confidence_list != NULL:
                proj_int_list_destroy(out_confidence_list)

        # check to make sure that the projection found is valid
        if proj_list == NULL or num_proj_objects <= 0 or out_confidence < min_confidence:
            if proj_list != NULL:
                proj_list_destroy(proj_list)
            return None

        # retrieve the best matching projection
        cdef PJ* proj
        try:
            proj = proj_list_get(self.projctx, proj_list, 0)
        finally:
            proj_list_destroy(proj_list)
        if proj == NULL:
            return None

        # convert the matching projection to the EPSG code
        cdef const char* code
        cdef const char* out_auth_name
        try:
            code = proj_get_id_code(proj, 0)
            out_auth_name = proj_get_id_auth_name(proj, 0)
            if out_auth_name != NULL and code != NULL:
                return pystrdecode(out_auth_name), pystrdecode(code)
        finally:
            proj_destroy(proj)

        return None

    def _is_crs_property(self, property_name, property_types, sub_crs_index=0):
        """
        This method will check for a property on the CRS.
        It will check if it has the property on the sub CRS
        if it is a compount CRS and will check if the source CRS
        has the property if it is a bound CRS.

        Parameters
        ----------
        property_name: str
            The name of the CRS property.
        property_types: tuple(PJ_TYPE)
            The types to check for for the property.
        sub_crs_index: int, optional
            THe index of the CRS in the sub CRS list. Default is 0.

        Returns
        -------
        bool: True if the CRS has this property.
        """
        if self.sub_crs_list:
            sub_crs = self.sub_crs_list[sub_crs_index]
            if sub_crs.is_bound:
                is_property = getattr(sub_crs.source_crs, property_name)
            else:
                is_property = getattr(sub_crs, property_name)
        elif self.is_bound:
            is_property = getattr(self.source_crs, property_name)
        else:
            is_property = self._type in property_types
        return is_property


    @property
    def is_geographic(self):
        """
        This checks if the CRS is geographic.
        It will check if it has a geographic CRS
        in the sub CRS if it is a compount CRS and will check if 
        the source CRS is geographic if it is a bound CRS.

        Returns
        -------
        bool: True if the CRS is in geographic (lon/lat) coordinates.
        """
        return self._is_crs_property(
            "is_geographic", 
            (
                PJ_TYPE_GEOGRAPHIC_CRS,
                PJ_TYPE_GEOGRAPHIC_2D_CRS,
                PJ_TYPE_GEOGRAPHIC_3D_CRS
            )
        )

    @property
    def is_projected(self):
        """
        This checks if the CRS is projected.
        It will check if it has a projected CRS
        in the sub CRS if it is a compount CRS and will check if
        the source CRS is projected if it is a bound CRS.

        Returns
        -------
        bool: True if CRS is projected.
        """
        return self._is_crs_property(
            "is_projected", 
            (PJ_TYPE_PROJECTED_CRS,)
        )

    @property
    def is_vertical(self):
        """
        This checks if the CRS is vertical.
        It will check if it has a vertical CRS
        in the sub CRS if it is a compount CRS and will check if 
        the source CRS is vertical if it is a bound CRS.

        Returns
        -------
        bool: True if CRS is vertical.
        """
        return self._is_crs_property(
            "is_vertical", 
            (PJ_TYPE_VERTICAL_CRS,),
            sub_crs_index=1
        )

    @property
    def is_bound(self):
        """
        Returns
        -------
        bool: True if CRS is bound.
        """
        return self._type == PJ_TYPE_BOUND_CRS

    @property
    def is_valid(self):
        """
        Returns
        -------
        bool: True if CRS is valid.
        """
        warnings.warn(
            "CRS.is_valid is deprecated.",
            DeprecationWarning,
        )
        return self._type != PJ_TYPE_UNKNOWN

    @property
    def is_engineering(self):
        """
        Returns
        -------
        bool: True if CRS is local/engineering.
        """
        return self._type == PJ_TYPE_ENGINEERING_CRS

    @property
    def is_geocentric(self):
        """
        This checks if the CRS is geocentric and
        takes into account if the CRS is bound.

        Returns
        -------
        bool: True if CRS is in geocentric (x/y) coordinates.
        """
        if self.is_bound:
            return self.source_crs.is_geocentric
        return self._type == PJ_TYPE_GEOCENTRIC_CRS
