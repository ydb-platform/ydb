include "proj.pxi"

import warnings
from collections import namedtuple

from libc.stdlib cimport free, malloc

from pyproj._compat cimport cstrdecode, cstrencode
from pyproj._context cimport pyproj_context_create

from pyproj.aoi import AreaOfUse
from pyproj.enums import PJType


cdef dict _PJ_TYPE_MAP = {
    PJType.UNKNOWN: PJ_TYPE_UNKNOWN,
    PJType.ELLIPSOID: PJ_TYPE_ELLIPSOID,
    PJType.PRIME_MERIDIAN: PJ_TYPE_PRIME_MERIDIAN,
    PJType.GEODETIC_REFERENCE_FRAME: PJ_TYPE_GEODETIC_REFERENCE_FRAME,
    PJType.DYNAMIC_GEODETIC_REFERENCE_FRAME: PJ_TYPE_DYNAMIC_GEODETIC_REFERENCE_FRAME,
    PJType.VERTICAL_REFERENCE_FRAME: PJ_TYPE_VERTICAL_REFERENCE_FRAME,
    PJType.DYNAMIC_VERTICAL_REFERENCE_FRAME: PJ_TYPE_DYNAMIC_VERTICAL_REFERENCE_FRAME,
    PJType.DATUM_ENSEMBLE: PJ_TYPE_DATUM_ENSEMBLE,
    PJType.CRS: PJ_TYPE_CRS,
    PJType.GEODETIC_CRS: PJ_TYPE_GEODETIC_CRS,
    PJType.GEOCENTRIC_CRS: PJ_TYPE_GEOCENTRIC_CRS,
    PJType.GEOGRAPHIC_CRS: PJ_TYPE_GEOGRAPHIC_CRS,
    PJType.GEOGRAPHIC_2D_CRS: PJ_TYPE_GEOGRAPHIC_2D_CRS,
    PJType.GEOGRAPHIC_3D_CRS: PJ_TYPE_GEOGRAPHIC_3D_CRS,
    PJType.VERTICAL_CRS: PJ_TYPE_VERTICAL_CRS,
    PJType.PROJECTED_CRS: PJ_TYPE_PROJECTED_CRS,
    PJType.COMPOUND_CRS: PJ_TYPE_COMPOUND_CRS,
    PJType.TEMPORAL_CRS: PJ_TYPE_TEMPORAL_CRS,
    PJType.ENGINEERING_CRS: PJ_TYPE_ENGINEERING_CRS,
    PJType.BOUND_CRS: PJ_TYPE_BOUND_CRS,
    PJType.OTHER_CRS: PJ_TYPE_OTHER_CRS,
    PJType.CONVERSION: PJ_TYPE_CONVERSION,
    PJType.TRANSFORMATION: PJ_TYPE_TRANSFORMATION,
    PJType.CONCATENATED_OPERATION: PJ_TYPE_CONCATENATED_OPERATION,
    PJType.OTHER_COORDINATE_OPERATION: PJ_TYPE_OTHER_COORDINATE_OPERATION,
    PJType.DERIVED_PROJECTED_CRS: PJ_TYPE_DERIVED_PROJECTED_CRS,
}

cdef dict _INV_PJ_TYPE_MAP = {value: key for key, value in _PJ_TYPE_MAP.items()}


cdef PJ_TYPE get_pj_type(pj_type) except *:
    if not isinstance(pj_type, PJType):
        pj_type = PJType.create(pj_type)
    return _PJ_TYPE_MAP[pj_type]


def get_authorities():
    """
    .. versionadded:: 2.4.0

    See: :c:func:`proj_get_authorities_from_database`

    Returns
    -------
    list[str]:
        Authorities in PROJ database.
    """
    cdef PJ_CONTEXT* context = pyproj_context_create()
    cdef PROJ_STRING_LIST proj_auth_list = NULL
    with nogil:
        proj_auth_list = proj_get_authorities_from_database(context)
    if proj_auth_list == NULL:
        return []
    cdef int iii = 0
    try:
        auth_list = []
        while proj_auth_list[iii] != NULL:
            auth_list.append(proj_auth_list[iii])
            iii += 1
    finally:
        proj_string_list_destroy(proj_auth_list)
    return auth_list


def get_codes(str auth_name not None, pj_type not None, bint allow_deprecated=False):
    """
    .. versionadded:: 2.4.0

    See: :c:func:`proj_get_codes_from_database`

    Parameters
    ----------
    auth_name: str
        The name of the authority.
    pj_type: pyproj.enums.PJType
        The type of object to get the authorities.
    allow_deprecated: bool, default=False
        Allow a deprecated code in the return.

    Returns
    -------
    list[str]:
        Codes associated with authorities in PROJ database.
    """
    cdef PJ_TYPE cpj_type = get_pj_type(pj_type)
    cdef PROJ_STRING_LIST proj_code_list = NULL
    cdef PJ_CONTEXT* context = pyproj_context_create()
    cdef const char* c_auth_name = NULL

    b_auth_name = cstrencode(auth_name)
    c_auth_name = b_auth_name
    with nogil:
        proj_code_list = proj_get_codes_from_database(
            context,
            c_auth_name,
            cpj_type,
            allow_deprecated,
        )
    if proj_code_list == NULL:
        return []
    cdef int iii = 0
    try:
        code_list = []
        while proj_code_list[iii] != NULL:
            code_list.append(proj_code_list[iii])
            iii += 1
    finally:
        proj_string_list_destroy(proj_code_list)
    return code_list


CRSInfo = namedtuple(
    "CRSInfo",
    [
        "auth_name",
        "code",
        "name",
        "type",
        "deprecated",
        "area_of_use",
        "projection_method_name",
    ],
)
CRSInfo.__doc__ = """
.. versionadded:: 3.0.0

CRS Information

Parameters
----------
auth_name: str
    Authority name.
code: str
    Object code.
name: str
    Object name.
type: PJType
    The type of CRS
deprecated: bool
    Whether the object is deprecated.
area_of_use: AreaOfUse | None
    The area of use for the CRS if valid.
projection_method_name: str | None
    Name of the projection method for a projected CRS.
"""


def query_crs_info(
    str auth_name=None,
    pj_types=None,
    area_of_interest=None,
    bint contains=False,
    bint allow_deprecated=False,
 ):
    """
    .. versionadded:: 3.0.0

    Query for CRS information from the PROJ database.

    See: :c:func:`proj_get_crs_info_list_from_database`

    Parameters
    ----------
    auth_name: str, optional
        The name of the authority. Default is all authorities.
    pj_types: pyproj.enums.PJType | list[pyproj.enums.PJType | None, optional
        The type(s) of CRS to get the information (i.e. the types with CRS in the name).
        If None is provided, it will use all of types (i.e. PJType.CRS).
    area_of_interest: AreaOfInterest, optional
        Filter returned CRS by the area of interest. Default method is intersection.
    contains: bool, default=False
        Only works if the area of interest is passed in.
        If True, then only CRS whose area of use entirely contains the specified
        bounding box will be returned. If False, then only CRS whose area of use
        intersects the specified bounding box will be returned.
    allow_deprecated: bool, default=False
        Allow a deprecated code in the return.

    Returns
    -------
    list[CRSInfo]:
        CRS information from the PROJ database.
    """
    cdef PJ_TYPE *pj_type_list = NULL
    cdef PROJ_CRS_LIST_PARAMETERS *query_params = NULL
    cdef PROJ_CRS_INFO **crs_info_list = NULL
    cdef const char* c_auth_name = NULL
    cdef int result_count = 0
    cdef int pj_type_count = 0
    cdef int iii = 0
    cdef bytes b_auth_name
    cdef PJ_CONTEXT* context = pyproj_context_create()

    if auth_name is not None:
        b_auth_name = cstrencode(auth_name)
        c_auth_name = b_auth_name
    try:
        if pj_types is not None:
            if isinstance(pj_types, (PJType, str)):
                pj_types = (pj_types,)
            pj_type_count = len(pj_types)
            pj_type_list = <PJ_TYPE*>malloc(
                pj_type_count * sizeof(PJ_TYPE)
            )
            for iii in range(pj_type_count):
                pj_type_list[iii] = get_pj_type(pj_types[iii])

        query_params = proj_get_crs_list_parameters_create()
        query_params.types = pj_type_list
        query_params.typesCount = pj_type_count
        query_params.allow_deprecated = bool(allow_deprecated)
        if area_of_interest:
            query_params.crs_area_of_use_contains_bbox = bool(contains)
            query_params.bbox_valid = True
            query_params.west_lon_degree = area_of_interest.west_lon_degree
            query_params.south_lat_degree = area_of_interest.south_lat_degree
            query_params.east_lon_degree = area_of_interest.east_lon_degree
            query_params.north_lat_degree = area_of_interest.north_lat_degree

        with nogil:
            crs_info_list = proj_get_crs_info_list_from_database(
                context,
                c_auth_name,
                query_params,
                &result_count)
    finally:
        if query_params != NULL:
            proj_get_crs_list_parameters_destroy(query_params)
        if pj_type_list != NULL:
            free(pj_type_list)
    if crs_info_list == NULL:
        return []
    try:
        code_list = []
        iii = 0
        while crs_info_list[iii] != NULL:
            area_of_use = None
            if crs_info_list[iii].bbox_valid:
                area_of_use = AreaOfUse(
                    west=crs_info_list[iii].west_lon_degree,
                    south=crs_info_list[iii].south_lat_degree,
                    east=crs_info_list[iii].east_lon_degree,
                    north=crs_info_list[iii].north_lat_degree,
                    name=cstrdecode(crs_info_list[iii].area_name),
                )
            code_list.append(CRSInfo(
                auth_name=crs_info_list[iii].auth_name,
                code=crs_info_list[iii].code,
                name=crs_info_list[iii].name,
                type=_INV_PJ_TYPE_MAP[crs_info_list[iii].type],
                deprecated=bool(crs_info_list[iii].deprecated),
                area_of_use=area_of_use,
                projection_method_name=cstrdecode(
                    crs_info_list[iii].projection_method_name
                )
            ))
            iii += 1
    finally:
        proj_crs_info_list_destroy(crs_info_list)
    return code_list


def query_utm_crs_info(
    str datum_name=None,
    area_of_interest=None,
    bint contains=False,
 ):
    """
    .. versionadded:: 3.0.0

    Query for EPSG UTM CRS information from the PROJ database.

    See: :c:func:`proj_get_crs_info_list_from_database`

    Parameters
    ----------
    datum_name: str, optional
        The name of the datum in the CRS name ('NAD27', 'NAD83', 'WGS 84', ...).
    area_of_interest: AreaOfInterest, optional
        Filter returned CRS by the area of interest. Default method is intersection.
    contains: bool, default=False
        Only works if the area of interest is passed in.
        If True, then only CRS whose area of use entirely contains the specified
        bounding box will be returned. If False, then only CRS whose area of use
        intersects the specified bounding box will be returned.

    Returns
    -------
    list[CRSInfo]:
        UTM CRS information from the PROJ database.
    """
    projected_crs = query_crs_info(
        auth_name="EPSG",
        pj_types=PJType.PROJECTED_CRS,
        area_of_interest=area_of_interest,
        contains=contains,
    )
    utm_crs = [crs for crs in projected_crs if "UTM zone" in crs.name]
    if datum_name is None:
        return utm_crs
    datum_name = datum_name.replace(" ", "")
    return [
        crs for crs in utm_crs
        if datum_name == crs.name.split("/")[0].replace(" ", "")
    ]


Unit = namedtuple(
    "Unit",
    [
        "auth_name",
        "code",
        "name",
        "category",
        "conv_factor",
        "proj_short_name",
        "deprecated",
    ],
)
Unit.__doc__ = """
.. versionadded:: 3.0.0

Parameters
----------
auth_name: str
    Authority name.
code: str
    Object code.
name: str
    Object name. For example "metre", "US survey foot", etc.
category: str
    Category of the unit: one of "linear", "linear_per_time", "angular",
    "angular_per_time", "scale", "scale_per_time" or "time".
conv_factor: float
    Conversion factor to apply to transform from that unit to the
    corresponding SI unit (metre for "linear", radian for "angular", etc.).
    It might be 0 in some cases to indicate no known conversion factor.
proj_short_name: str, optional
    PROJ short name, like "m", "ft", "us-ft", etc... Might be None.
deprecated: bool
    Whether the object is deprecated.
"""


def get_units_map(str auth_name=None, str category=None, bint allow_deprecated=False):
    """
    .. versionadded:: 2.2.0
    .. versionadded:: 3.0.0 query PROJ database.

    Get the units available in the PROJ database.

    See: :c:func:`proj_get_units_from_database`

    Parameters
    ----------
    auth_name: str, optional
        The authority name to filter by (e.g. EPSG, PROJ). Default is all.
    category: str, optional
        Category of the unit: one of "linear", "linear_per_time", "angular",
        "angular_per_time", "scale", "scale_per_time" or "time". Default is all.
    allow_deprecated: bool, default=False
        Whether or not to allow deprecated units.

    Returns
    -------
    dict[str, Unit]
    """
    cdef const char* c_auth_name = NULL
    cdef const char* c_category = NULL
    cdef bytes b_auth_name
    cdef bytes b_category
    if auth_name is not None:
        b_auth_name = cstrencode(auth_name)
        c_auth_name = b_auth_name
    if category is not None:
        b_category = cstrencode(category)
        c_category = b_category

    cdef int num_units = 0
    cdef PJ_CONTEXT* context = pyproj_context_create()
    cdef PROJ_UNIT_INFO** db_unit_list = NULL
    with nogil:
        db_unit_list = proj_get_units_from_database(
            context,
            c_auth_name,
            c_category,
            bool(allow_deprecated),
            &num_units,
        )
    units_map = {}
    try:
        for iii in range(num_units):
            proj_short_name = None
            if db_unit_list[iii].proj_short_name != NULL:
                proj_short_name = db_unit_list[iii].proj_short_name
            name = db_unit_list[iii].name
            units_map[name] = Unit(
                auth_name=db_unit_list[iii].auth_name,
                code=db_unit_list[iii].code,
                name=name,
                category=db_unit_list[iii].category,
                conv_factor=db_unit_list[iii].conv_factor,
                proj_short_name=proj_short_name,
                deprecated=bool(db_unit_list[iii].deprecated),
            )
    finally:
        proj_unit_list_destroy(db_unit_list)
    return units_map


def get_database_metadata(str key not None):
    """
    Return metadata from the database.

    See: :c:func:`proj_context_get_database_metadata`

    Available keys:

    - DATABASE.LAYOUT.VERSION.MAJOR
    - DATABASE.LAYOUT.VERSION.MINOR
    - EPSG.VERSION
    - EPSG.DATE
    - ESRI.VERSION
    - ESRI.DATE
    - IGNF.SOURCE
    - IGNF.VERSION
    - IGNF.DATE
    - NKG.SOURCE
    - NKG.VERSION
    - NKG.DATE
    - PROJ.VERSION
    - PROJ_DATA.VERSION : PROJ-data version most compatible with this database.

    Parameters
    ----------
    key: str
        The name of the metadata item to get data for.

    Returns
    -------
    str | None:
        The metatada information if available.
    """
    cdef const char* metadata = NULL
    metadata = proj_context_get_database_metadata(
        pyproj_context_create(),
        cstrencode(key),
    )
    if metadata == NULL:
        return None
    return metadata
