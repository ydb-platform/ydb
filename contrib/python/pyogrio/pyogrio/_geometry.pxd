from pyogrio._ogr cimport *

cdef str get_geometry_type(void *ogr_layer)
cdef OGRwkbGeometryType get_geometry_type_code(str geometry_type) except *
