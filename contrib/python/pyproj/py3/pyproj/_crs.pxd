include "proj.pxi"


from pyproj.enums import WktVersion

from cpython cimport bool


cdef extern from "proj_experimental.h":
    PJ *proj_crs_promote_to_3D(PJ_CONTEXT *ctx,
                               const char* crs_3D_name,
                               const PJ* crs_2D)

    PJ *proj_crs_demote_to_2D(PJ_CONTEXT *ctx,
                              const char *crs_2D_name,
                              const PJ *crs_3D)

cdef tuple _get_concatenated_operations(PJ_CONTEXT*context, PJ*concatenated_operation)
cdef _to_proj4(
    PJ_CONTEXT* context,
    PJ* projobj,
    object version,
    bint pretty,
)
cdef _to_wkt(
    PJ_CONTEXT* context,
    PJ* projobj,
    object version,
    bint pretty,
    bool output_axis_rule=*,
)

cdef class Axis:
    cdef readonly str name
    cdef readonly str abbrev
    cdef readonly str direction
    cdef readonly double unit_conversion_factor
    cdef readonly str unit_name
    cdef readonly str unit_auth_code
    cdef readonly str unit_code

    @staticmethod
    cdef Axis create(PJ_CONTEXT* context, PJ* projobj, int index)

cdef create_area_of_use(PJ_CONTEXT* context, PJ* projobj)

cdef class Base:
    cdef PJ *projobj
    cdef PJ_CONTEXT* context
    cdef readonly object _context_manager
    cdef readonly str name
    cdef readonly str _remarks
    cdef readonly str _scope
    cdef _set_base_info(self)

cdef class _CRSParts(Base):
    pass


cdef class Ellipsoid(_CRSParts):
    cdef readonly double semi_major_metre
    cdef readonly double semi_minor_metre
    cdef readonly bint is_semi_minor_computed
    cdef readonly double inverse_flattening

    @staticmethod
    cdef Ellipsoid create(PJ_CONTEXT* context, PJ* ellipsoid_pj)


cdef class PrimeMeridian(_CRSParts):
    cdef readonly double longitude
    cdef readonly double unit_conversion_factor
    cdef readonly str unit_name

    @staticmethod
    cdef PrimeMeridian create(PJ_CONTEXT* context, PJ* prime_meridian_pj)


cdef class Datum(_CRSParts):
    cdef readonly str type_name
    cdef readonly object _ellipsoid
    cdef readonly object _prime_meridian

    @staticmethod
    cdef Datum create(PJ_CONTEXT* context, PJ* datum_pj)


cdef class CoordinateSystem(_CRSParts):
    cdef readonly list _axis_list

    @staticmethod
    cdef CoordinateSystem create(PJ_CONTEXT* context, PJ* coordinate_system_pj)


cdef class Param:
    cdef readonly str name
    cdef readonly str auth_name
    cdef readonly str code
    cdef readonly object value
    cdef readonly double unit_conversion_factor
    cdef readonly str unit_name
    cdef readonly str unit_auth_name
    cdef readonly str unit_code
    cdef readonly str unit_category

    @staticmethod
    cdef Param create(PJ_CONTEXT* context, PJ* projobj, int param_idx)


cdef class Grid:
    cdef readonly str short_name
    cdef readonly str full_name
    cdef readonly str package_name
    cdef readonly str url
    cdef readonly bint direct_download
    cdef readonly bint open_license
    cdef readonly bint available

    @staticmethod
    cdef Grid create(PJ_CONTEXT* context, PJ* projobj, int grid_idx)


cdef class CoordinateOperation(_CRSParts):
    cdef readonly list _params
    cdef readonly list _grids
    cdef readonly object _area_of_use
    cdef readonly str method_name
    cdef readonly str method_auth_name
    cdef readonly str method_code
    cdef readonly double accuracy
    cdef readonly bint is_instantiable
    cdef readonly bint has_ballpark_transformation
    cdef readonly list _towgs84
    cdef readonly tuple _operations
    cdef readonly str type_name

    @staticmethod
    cdef CoordinateOperation create(PJ_CONTEXT* context, PJ* coordinate_operation_pj)


cdef class _CRS(Base):
    cdef PJ_TYPE _type
    cdef PJ_PROJ_INFO projpj_info
    cdef readonly str srs
    cdef readonly str _type_name
    cdef readonly Ellipsoid _ellipsoid
    cdef readonly object _area_of_use
    cdef readonly PrimeMeridian _prime_meridian
    cdef readonly Datum _datum
    cdef readonly list _sub_crs_list
    cdef readonly _CRS _source_crs
    cdef readonly _CRS _target_crs
    cdef readonly _CRS _geodetic_crs
    cdef readonly CoordinateSystem _coordinate_system
    cdef readonly CoordinateOperation _coordinate_operation
