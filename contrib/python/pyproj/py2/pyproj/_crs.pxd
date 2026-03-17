include "proj.pxi"

cdef class Axis:
    cdef readonly object name
    cdef readonly object abbrev
    cdef readonly object direction
    cdef readonly double unit_conversion_factor
    cdef readonly object unit_name
    cdef readonly object unit_auth_code
    cdef readonly object unit_code

    @staticmethod
    cdef create(PJ_CONTEXT* projcontext, PJ* projobj, int index)

cdef class AreaOfUse:
    cdef readonly double west
    cdef readonly double south
    cdef readonly double east
    cdef readonly double north
    cdef readonly object name

    @staticmethod
    cdef create(PJ_CONTEXT* projcontext, PJ* projobj)


cdef class Base:
    cdef PJ *projobj
    cdef PJ_CONTEXT *projctx
    cdef readonly object name


cdef class Ellipsoid(Base):
    cdef double _semi_major_metre
    cdef double _semi_minor_metre
    cdef readonly object is_semi_minor_computed
    cdef double _inv_flattening
    cdef readonly object ellipsoid_loaded

    @staticmethod
    cdef create(PJ* ellipsoid_pj)

cdef class PrimeMeridian(Base):
    cdef readonly double longitude
    cdef readonly double unit_conversion_factor
    cdef readonly object unit_name

    @staticmethod
    cdef create(PJ* prime_meridian_pj)


cdef class Datum(Base):
    cdef readonly object _ellipsoid
    cdef readonly object _prime_meridian

    @staticmethod
    cdef create(PJ* datum_pj)


cdef class CoordinateSystem(Base):
    cdef readonly object _axis_list

    @staticmethod
    cdef create(PJ* coordinate_system_pj)


cdef class Param:
    cdef readonly object name
    cdef readonly object auth_name
    cdef readonly object code
    cdef readonly object value
    cdef readonly double unit_conversion_factor
    cdef readonly object unit_name
    cdef readonly object unit_auth_name
    cdef readonly object unit_code
    cdef readonly object unit_category

    @staticmethod
    cdef create(PJ_CONTEXT* projcontext, PJ* projobj, int param_idx)


cdef class Grid:
    cdef readonly object short_name
    cdef readonly object full_name
    cdef readonly object package_name
    cdef readonly object url
    cdef readonly object direct_download
    cdef readonly object open_license
    cdef readonly object available

    @staticmethod
    cdef create(PJ_CONTEXT* projcontext, PJ* projobj, int grid_idx)


cdef class CoordinateOperation(Base):
    cdef readonly object _params
    cdef readonly object _grids
    cdef readonly object method_name
    cdef readonly object method_auth_name
    cdef readonly object method_code
    cdef readonly double accuracy
    cdef readonly object is_instantiable
    cdef readonly object has_ballpark_transformation
    cdef readonly object _towgs84

    @staticmethod
    cdef create(PJ* coordinate_operation_pj)


cdef class _CRS(Base):
    cdef PJ_TYPE _type
    cdef PJ_PROJ_INFO projpj_info
    cdef char *pjinitstring
    cdef readonly object srs
    cdef readonly object type_name
    cdef object _ellipsoid
    cdef object _area_of_use
    cdef object _prime_meridian
    cdef object _datum
    cdef object _sub_crs_list
    cdef object _source_crs
    cdef object _target_crs
    cdef object _geodetic_crs
    cdef object _coordinate_system
    cdef object _coordinate_operation