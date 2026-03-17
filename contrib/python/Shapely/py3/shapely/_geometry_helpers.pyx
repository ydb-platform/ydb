cimport cython
from cpython cimport PyObject
from cython cimport view
from libc.stdint cimport uintptr_t

import numpy as np

cimport numpy as np

import shapely

from shapely._geos cimport (
    GEOSContextHandle_t,
    GEOSCoordSeq_clone_r,
    GEOSCoordSeq_getSize_r,
    GEOSCoordSequence,
    GEOSGeom_clone_r,
    GEOSGeom_createCollection_r,
    GEOSGeom_createEmptyPolygon_r,
    GEOSGeom_createLinearRing_r,
    GEOSGeom_createLineString_r,
    GEOSGeom_createPoint_r,
    GEOSGeom_createPolygon_r,
    GEOSGeom_destroy_r,
    GEOSGeom_getCoordSeq_r,
    GEOSGeometry,
    GEOSGeomTypeId_r,
    GEOSGetExteriorRing_r,
    GEOSGetGeometryN_r,
    GEOSGetInteriorRingN_r,
    get_geos_handle,
)
from shapely._pygeos_api cimport (
    import_shapely_c_api,
    PGERR_NAN_COORD,
    PGERR_SUCCESS,
    PGERR_GEOS_EXCEPTION,
    PGERR_LINEARRING_NCOORDS,
    PyGEOS_CoordSeq_FromBuffer,
    PyGEOS_CreateGeometry,
    PyGEOS_GetGEOSGeometry,
    ShapelyHandleNan,
)

# initialize Shapely C API
import_shapely_c_api()


def _check_out_array(object out, Py_ssize_t size):
    if out is None:
        return np.empty(shape=(size, ), dtype=object)
    if not isinstance(out, np.ndarray):
        raise TypeError("out array must be of numpy.ndarray type")
    if not out.flags.writeable:
        raise TypeError("out array must be writeable")
    if out.dtype != object:
        raise TypeError("out array dtype must be object")
    if out.ndim != 1:
        raise TypeError("out must be a one-dimensional array.")
    if out.shape[0] < size:
        raise ValueError(f"out array is too small ({out.shape[0]} < {size})")
    return out


@cython.boundscheck(False)
@cython.wraparound(False)
cdef int _create_simple_geometry(
    GEOSContextHandle_t geos_handle,
    const double[:, :] coord_view,
    Py_ssize_t idx,
    unsigned int n_coords,
    unsigned int dims,
    int geometry_type,
    char is_ring,
    int handle_nan,
    GEOSGeometry **geom,
) noexcept nogil:
    """
    Helper function to create a geometry with single CoordinateSequence
    (Point, LineString, LinearRing).
    """
    cdef GEOSCoordSequence *seq = NULL
    cdef int errstate
    cdef unsigned int actual_n_coords = 0

    errstate = PyGEOS_CoordSeq_FromBuffer(
        geos_handle, &coord_view[idx, 0], n_coords, dims,
        is_ring, handle_nan, &seq
    )
    if errstate != PGERR_SUCCESS:
        return errstate

    if geometry_type == 0:
        geom[0] = GEOSGeom_createPoint_r(geos_handle, seq)
    elif geometry_type == 1:
        geom[0] = GEOSGeom_createLineString_r(geos_handle, seq)
    elif geometry_type == 2:
        # check the resulting size to prevent invalid rings
        if GEOSCoordSeq_getSize_r(geos_handle, seq, &actual_n_coords) == 0:
            return PGERR_GEOS_EXCEPTION
        if 0 < actual_n_coords < 4:
            return PGERR_LINEARRING_NCOORDS
        geom[0] = GEOSGeom_createLinearRing_r(geos_handle, seq)

    if geom[0] == NULL:
        return PGERR_GEOS_EXCEPTION

    return PGERR_SUCCESS


def _create_simple_geometry_raise_error(int errstate):
    """
    Handle error raising for _create_simple_geometry above (so that the function
    itself can be fully C function without Python error checking).
    """
    if errstate == PGERR_NAN_COORD:
        raise ValueError(
            "A NaN, Inf or -Inf coordinate was supplied. Remove the "
            "coordinate or adapt the 'handle_nan' parameter."
        )
    elif errstate == PGERR_LINEARRING_NCOORDS:
        # the error equals PGERR_LINEARRING_NCOORDS (in shapely/src/geos.h)
        raise ValueError("A linearring requires at least 4 coordinates.")
    else:
        # GEOSException is raised by get_geos_handle
        return


@cython.boundscheck(False)
@cython.wraparound(False)
def simple_geometries_1d(object coordinates, object indices, int geometry_type, int handle_nan, object out = None):
    cdef Py_ssize_t idx = 0
    cdef unsigned int coord_idx = 0
    cdef Py_ssize_t geom_idx = 0
    cdef unsigned int n_coords = 0
    cdef unsigned int ring_closure = 0
    cdef GEOSGeometry *geom = NULL
    cdef GEOSCoordSequence *seq = NULL

    # Cast input arrays and define memoryviews for later usage
    coordinates = np.asarray(coordinates, dtype=np.float64, order="C")
    if coordinates.ndim != 2:
        raise TypeError("coordinates must be a two-dimensional array.")

    indices = np.asarray(indices, dtype=np.intp)  # intp is what bincount takes
    if indices.ndim != 1:
        raise TypeError("indices must be a one-dimensional array.")

    if coordinates.shape[0] != indices.shape[0]:
        raise ValueError("geometries and indices do not have equal size.")

    cdef unsigned int dims = coordinates.shape[1]
    if dims not in {2, 3}:
        raise ValueError("coordinates should be N by 2 or N by 3.")

    if geometry_type not in {0, 1, 2}:
        raise ValueError(f"Invalid geometry_type: {geometry_type}.")

    cdef char is_ring = 1 if geometry_type == 2 else 0

    if coordinates.shape[0] == 0:
        # return immediately if there are no geometries to return
        return np.empty(shape=(0, ), dtype=np.object_)

    if np.any(indices[1:] < indices[:indices.shape[0] - 1]):
        raise ValueError("The indices must be sorted.")

    cdef const double[:, :] coord_view = coordinates

    # get the geometry count per collection (this raises on negative indices)
    cdef unsigned int[:] coord_counts = np.bincount(indices).astype(np.uint32)

    # The final target array
    cdef Py_ssize_t n_geoms = coord_counts.shape[0]
    # Allow missing indices only if 'out' was given explicitly (if 'out' is not
    # supplied by the user, we would have to come up with an output value ourselves).
    cdef char allow_missing = out is not None
    out = _check_out_array(out, n_geoms)
    cdef object[:] out_view = out

    with get_geos_handle() as geos_handle:
        for geom_idx in range(n_geoms):
            n_coords = coord_counts[geom_idx]

            if n_coords == 0:
                if allow_missing:
                    continue
                else:
                    raise ValueError(
                        f"Index {geom_idx} is missing from the input indices."
                    )
            errstate = _create_simple_geometry(
                geos_handle, coord_view, idx, n_coords, dims, geometry_type,
                is_ring, handle_nan, &geom
            )
            if errstate != PGERR_SUCCESS:
                return _create_simple_geometry_raise_error(errstate)

            idx += n_coords

            out_view[geom_idx] = PyGEOS_CreateGeometry(geom, geos_handle)

    return out



cdef const GEOSGeometry* GetRingN(GEOSContextHandle_t handle, GEOSGeometry* polygon, int n):
    if n == 0:
        return GEOSGetExteriorRing_r(handle, polygon)
    else:
        return GEOSGetInteriorRingN_r(handle, polygon, n - 1)



@cython.boundscheck(False)
@cython.wraparound(False)
def get_parts(object[:] array, bint extract_rings=0):
    cdef Py_ssize_t geom_idx = 0
    cdef Py_ssize_t part_idx = 0
    cdef Py_ssize_t idx = 0
    cdef Py_ssize_t count
    cdef GEOSGeometry *geom = NULL
    cdef const GEOSGeometry *part = NULL

    if extract_rings:
        counts = shapely.get_num_interior_rings(array)
        is_polygon = (shapely.get_type_id(array) == 3) & (~shapely.is_empty(array))
        counts += is_polygon
        count = counts.sum()
    else:
        counts = shapely.get_num_geometries(array)
        count = counts.sum()

    if count == 0:
        # return immediately if there are no geometries to return
        return (
            np.empty(shape=(0, ), dtype=object),
            np.empty(shape=(0, ), dtype=np.intp)
        )

    parts = np.empty(shape=(count, ), dtype=object)
    index = np.empty(shape=(count, ), dtype=np.intp)

    cdef int[:] counts_view = counts
    cdef object[:] parts_view = parts
    cdef np.intp_t[:] index_view = index

    with get_geos_handle() as geos_handle:
        for geom_idx in range(array.size):
            if counts_view[geom_idx] <= 0:
                # No parts to return, skip this item
                continue

            if PyGEOS_GetGEOSGeometry(<PyObject *>array[geom_idx], &geom) == 0:
                raise TypeError("One of the arguments is of incorrect type. "
                                "Please provide only Geometry objects.")

            if geom == NULL:
                continue

            for part_idx in range(counts_view[geom_idx]):
                index_view[idx] = geom_idx

                if extract_rings:
                    part = GetRingN(geos_handle, geom, part_idx)
                else:
                    part = GEOSGetGeometryN_r(geos_handle, geom, part_idx)
                if part == NULL:
                    return  # GEOSException is raised by get_geos_handle

                # clone the geometry to keep it separate from the inputs
                part = GEOSGeom_clone_r(geos_handle, part)
                if part == NULL:
                    return  # GEOSException is raised by get_geos_handle

                # cast part back to <GEOSGeometry> to discard const qualifier
                # pending issue #227
                parts_view[idx] = PyGEOS_CreateGeometry(<GEOSGeometry *>part, geos_handle)

                idx += 1

    return parts, index


@cython.boundscheck(False)
@cython.wraparound(False)
cdef void _deallocate_arr(GEOSContextHandle_t handle, np.intp_t[:] arr, Py_ssize_t last_geom_i) noexcept nogil:
    """Deallocate a temporary geometry array to prevent memory leaks"""
    cdef Py_ssize_t i = 0
    cdef GEOSGeometry *g

    for i in range(last_geom_i):
        g = <GEOSGeometry *>arr[i]
        if g != NULL:
            GEOSGeom_destroy_r(handle, <GEOSGeometry *>arr[i])


@cython.boundscheck(False)
@cython.wraparound(False)
def collections_1d(object geometries, object indices, int geometry_type = 7, object out = None):
    """Converts geometries + indices to collections

    Allowed geometry type conversions are:
    - linearrings to polygons
    - points to multipoints
    - linestrings/linearrings to multilinestrings
    - polygons to multipolygons
    - any to geometrycollections
    """
    cdef Py_ssize_t geom_idx_1 = 0
    cdef Py_ssize_t coll_idx = 0
    cdef unsigned int coll_size = 0
    cdef Py_ssize_t coll_geom_idx = 0
    cdef GEOSGeometry *geom = NULL
    cdef GEOSGeometry *coll = NULL
    cdef int expected_type = -1
    cdef int expected_type_alt = -1
    cdef int curr_type = -1

    if geometry_type == 3:  # POLYGON
        expected_type = 2
    elif geometry_type == 4:  # MULTIPOINT
        expected_type = 0
    elif geometry_type == 5:  # MULTILINESTRING
        expected_type = 1
        expected_type_alt = 2
    elif geometry_type == 6:  # MULTIPOLYGON
        expected_type = 3
    elif geometry_type == 7:
        pass
    else:
        raise ValueError(f"Invalid geometry_type: {geometry_type}.")

    # Cast input arrays and define memoryviews for later usage
    geometries = np.asarray(geometries, dtype=object)
    if geometries.ndim != 1:
        raise TypeError("geometries must be a one-dimensional array.")

    indices = np.asarray(indices, dtype=np.intp)  # intp is what bincount takes
    if indices.ndim != 1:
        raise TypeError("indices must be a one-dimensional array.")

    if geometries.shape[0] != indices.shape[0]:
        raise ValueError("geometries and indices do not have equal size.")

    if geometries.shape[0] == 0:
        # return immediately if there are no geometries to return
        return np.empty(shape=(0, ), dtype=object)

    if np.any(indices[1:] < indices[:indices.shape[0] - 1]):
        raise ValueError("The indices should be sorted.")

    # get the geometry count per collection (this raises on negative indices)
    cdef int[:] collection_size = np.bincount(indices).astype(np.int32)

    # A temporary array for the geometries that will be given to CreateCollection.
    # Its size equals max(collection_size) to accommodate the largest collection.
    temp_geoms = np.empty(shape=(np.max(collection_size), ), dtype=np.intp)
    cdef np.intp_t[:] temp_geoms_view = temp_geoms

    # The final target array
    cdef Py_ssize_t n_colls = collection_size.shape[0]
    # Allow missing indices only if 'out' was given explicitly (if 'out' is not
    # supplied by the user, we would have to come up with an output value ourselves).
    cdef char allow_missing = out is not None
    out = _check_out_array(out, n_colls)
    cdef object[:] out_view = out

    with get_geos_handle() as geos_handle:
        for coll_idx in range(n_colls):
            if collection_size[coll_idx] == 0:
                if allow_missing:
                    continue
                else:
                    raise ValueError(
                        f"Index {coll_idx} is missing from the input indices."
                    )
            coll_size = 0

            # fill the temporary array with geometries belonging to this collection
            for coll_geom_idx in range(collection_size[coll_idx]):
                if PyGEOS_GetGEOSGeometry(<PyObject *>geometries[geom_idx_1 + coll_geom_idx], &geom) == 0:
                    _deallocate_arr(geos_handle, temp_geoms_view, coll_size)
                    raise TypeError(
                        "One of the arguments is of incorrect type. Please provide only Geometry objects."
                    )

                # ignore missing values
                if geom == NULL:
                    continue

                # Check geometry subtype for non-geometrycollections
                if geometry_type != 7:
                    curr_type = GEOSGeomTypeId_r(geos_handle, geom)
                    if curr_type == -1:
                        _deallocate_arr(geos_handle, temp_geoms_view, coll_size)
                        return  # GEOSException is raised by get_geos_handle
                    if curr_type != expected_type and curr_type != expected_type_alt:
                        _deallocate_arr(geos_handle, temp_geoms_view, coll_size)
                        raise TypeError(
                            f"One of the arguments has unexpected geometry type {curr_type}."
                        )

                # assign to the temporary geometry array
                geom = GEOSGeom_clone_r(geos_handle, geom)
                if geom == NULL:
                    _deallocate_arr(geos_handle, temp_geoms_view, coll_size)
                    return  # GEOSException is raised by get_geos_handle
                temp_geoms_view[coll_size] = <np.intp_t>geom
                coll_size += 1

            # create the collection
            if geometry_type != 3:  # Collection
                coll = GEOSGeom_createCollection_r(
                    geos_handle,
                    geometry_type,
                    <GEOSGeometry**> &temp_geoms_view[0],
                    coll_size
                )
            elif coll_size != 0:  # Polygon, non-empty
                coll = GEOSGeom_createPolygon_r(
                    geos_handle,
                    <GEOSGeometry*> temp_geoms_view[0],
                    <GEOSGeometry**> NULL if coll_size <= 1 else <GEOSGeometry**> &temp_geoms_view[1],
                    coll_size - 1
                )
            else:  # Polygon, empty
                coll = GEOSGeom_createEmptyPolygon_r(
                    geos_handle
                )

            if coll == NULL:
                return  # GEOSException is raised by get_geos_handle

            out_view[coll_idx] = PyGEOS_CreateGeometry(coll, geos_handle)

            geom_idx_1 += collection_size[coll_idx]

    return out


@cython.boundscheck(False)
@cython.wraparound(False)
def _from_ragged_array_multi_linear(
    const double[:, ::1] coordinates,
    const np.int64_t[:] offsets1,
    const np.int64_t[:] offsets2,
    int geometry_type,
):
    """
    Create Polygons or MultiLineStrings from coordinate and offset arrays.

    Polygon (geometry_type 3): linear_type is a LinearRing (2)
    MultiLineString (geometry_type 5): linear_type is a LineString (1)
    """
    cdef:
        Py_ssize_t n_total_coords, n_rings, n_geoms
        Py_ssize_t i, k
        Py_ssize_t i1, i2, k1, k2
        Py_ssize_t n_coords, linear_idx
        int errstate
        GEOSContextHandle_t geos_handle
        GEOSGeometry *linear = NULL
        GEOSGeometry *geom = NULL

    n_total_coords = coordinates.shape[0]
    n_rings = offsets1.shape[0] - 1
    n_geoms = offsets2.shape[0] - 1

    if offsets2[n_geoms] > n_rings:
        raise ValueError(
            f"Number of rings indicated by the geometry offsets ({offsets2[n_geoms]}) "
            f"larger than indicated by the shape of the linear offsets array ({n_rings})"
        )

    if offsets1[n_rings] > n_total_coords:
        raise ValueError(
            f"Number of coordinates indicated by the linear offsets ({offsets1[n_rings]}) "
            f"larger than the shape of the coordinates array ({n_total_coords})"
        )

    # A temporary array for the geometries that will be given to CreatePolygon/Collection.
    # For simplicity, we use n_rings instead of calculating the max needed size
    # as max(diff(offsets2)) (trading performance for a bit more memory usage)
    temp_linear = np.empty(shape=(n_rings, ), dtype=np.intp)
    cdef np.intp_t[:] temp_linear_view = temp_linear
    # A temporary array for resulting geometries
    temp_geoms = np.empty(shape=(n_geoms, ), dtype=np.intp)
    cdef np.intp_t[:] temp_geoms_view = temp_geoms

    # The final target array
    result = np.empty(shape=(n_geoms, ), dtype=object)
    cdef object[:] result_view = result

    cdef unsigned int dims = coordinates.shape[1]
    if dims not in {2, 3}:
        raise ValueError("coordinates should be N by 2 or N by 3.")

    cdef int linear_type
    cdef char is_ring
    if geometry_type == 3:
        # Polygon
        linear_type = 2
        is_ring = 1
    else:
        # MultiLineString
        linear_type = 1
        is_ring = 0
    cdef int handle_nan = 0

    with get_geos_handle() as geos_handle:
        with nogil:
            # iterating through the Polygons/MultiLineStrings
            for i in range(n_geoms):

                # each geometry can consist of multiple rings/lines
                # (for polygon: exterior ring + potentially interior rings(s))
                i1 = offsets2[i]
                i2 = offsets2[i + 1]

                # iterating through the linear elements
                linear_idx = 0
                for k in range(i1, i2):

                    # each ring/line consists of certain number of coords
                    k1 = offsets1[k]
                    k2 = offsets1[k + 1]
                    n_coords = k2 - k1
                    errstate = _create_simple_geometry(
                        geos_handle, coordinates, k1, n_coords, dims, linear_type,
                        is_ring, handle_nan, &linear
                    )
                    if errstate != PGERR_SUCCESS:
                        _deallocate_arr(geos_handle, temp_linear_view, linear_idx)
                        _deallocate_arr(geos_handle, temp_geoms_view, i - 1)
                        with gil:
                            return _create_simple_geometry_raise_error(errstate)

                    temp_linear_view[linear_idx] = <np.intp_t>linear
                    linear_idx += 1

                if geometry_type == 3:
                    # create Polygon
                    if linear_idx > 0:
                        geom = GEOSGeom_createPolygon_r(
                            geos_handle,
                            <GEOSGeometry*> temp_linear_view[0],
                            <GEOSGeometry**> &temp_linear_view[1 if linear_idx > 1 else 0],
                            linear_idx - 1
                        )
                    else:
                        geom = GEOSGeom_createEmptyPolygon_r(geos_handle)
                else:
                    # create MultiLineString collection
                    geom = GEOSGeom_createCollection_r(
                        geos_handle,
                        geometry_type,
                        <GEOSGeometry**> &temp_linear_view[0],
                        linear_idx
                    )
                if geom == NULL:
                    _deallocate_arr(geos_handle, temp_linear_view, linear_idx - 1)
                    _deallocate_arr(geos_handle, temp_geoms_view, i - 1)
                    with gil:
                        return  # GEOSException is raised by get_geos_handle

                temp_geoms_view[i] = <np.intp_t>geom

        for i in range(n_geoms):
            result_view[i] = PyGEOS_CreateGeometry(<GEOSGeometry *>temp_geoms_view[i], geos_handle)

    return result


@cython.boundscheck(False)
@cython.wraparound(False)
def _from_ragged_array_multipolygon(
    const double[:, ::1] coordinates,
    const np.int64_t[:] offsets1,
    const np.int64_t[:] offsets2,
    const np.int64_t[:] offsets3,
):
    """
    Create MultiPolygons from coordinate and offset arrays.
    """
    cdef:
        Py_ssize_t n_total_coords, n_rings, n_parts, n_geoms
        Py_ssize_t i, j, k
        Py_ssize_t i1, i2, j1, j2, k1, k2
        Py_ssize_t n_coords, rings_idx, parts_idx
        int errstate
        GEOSContextHandle_t geos_handle
        GEOSGeometry *ring = NULL
        GEOSGeometry *part = NULL
        GEOSGeometry *geom = NULL

    n_total_coords = coordinates.shape[0]
    n_rings = offsets1.shape[0] - 1
    n_parts = offsets2.shape[0] - 1
    n_geoms = offsets3.shape[0] - 1

    if offsets3[n_geoms] > n_parts:
        raise ValueError(
            f"Number of geometry parts indicated by the geometry offsets ({offsets3[n_geoms]}) "
            f"larger than indicated by the shape of the part offsets array ({n_parts})"
        )

    if offsets2[n_parts] > n_rings:
        raise ValueError(
            f"Number of rings indicated by the part offsets ({offsets2[n_parts]}) "
            f"larger than indicated by the shape of the linear offsets array ({n_rings})"
        )

    if offsets1[n_rings] > n_total_coords:
        raise ValueError(
            f"Number of coordinates indicated by the linear offsets ({offsets1[n_rings]}) "
            f"larger than the shape of the coordinates array ({n_total_coords})"
        )

    # A temporary array for the geometries that will be given to CreatePolygon
    # and CreateCollection. For simplicity, we use n_rings/n_parts instead of
    # calculating the max needed size (trading performance for a bit more memory usage)
    temp_rings = np.empty(shape=(n_rings, ), dtype=np.intp)
    cdef np.intp_t[:] temp_rings_view = temp_rings
    temp_parts = np.empty(shape=(n_parts, ), dtype=np.intp)
    cdef np.intp_t[:] temp_parts_view = temp_parts
    # A temporary array for resulting geometries
    temp_geoms = np.empty(shape=(n_geoms, ), dtype=np.intp)
    cdef np.intp_t[:] temp_geoms_view = temp_geoms

    # The final target array
    result = np.empty(shape=(n_geoms, ), dtype=object)
    cdef object[:] result_view = result

    cdef unsigned int dims = coordinates.shape[1]
    if dims not in {2, 3}:
        raise ValueError("coordinates should be N by 2 or N by 3.")

    cdef int ring_type = 2
    cdef int geometry_type = 6  # MultiPolygon
    cdef char is_ring = 1
    cdef int handle_nan = 0

    with get_geos_handle() as geos_handle:
        with nogil:
            # iterating through the MultiPolygons
            for i in range(n_geoms):

                # getting the indices for their parts
                i1 = offsets3[i]
                i2 = offsets3[i + 1]

                # Iterating through the geometry parts
                parts_idx = 0
                for j in range(i1, i2):

                    # each part (polygon) can consist of multiple rings
                    # (exterior ring + potentially interior rings(s))
                    j1 = offsets2[j]
                    j2 = offsets2[j + 1]

                    # iterating through the rings
                    rings_idx = 0
                    for k in range(j1, j2):

                        # each ring consists of certain number of coords
                        k1 = offsets1[k]
                        k2 = offsets1[k + 1]
                        n_coords = k2 - k1
                        errstate = _create_simple_geometry(
                            geos_handle, coordinates, k1, n_coords, dims, ring_type,
                            is_ring, handle_nan, &ring
                        )
                        if errstate != PGERR_SUCCESS:
                            _deallocate_arr(geos_handle, temp_rings_view, rings_idx)
                            _deallocate_arr(geos_handle, temp_parts_view, parts_idx - 1)
                            _deallocate_arr(geos_handle, temp_geoms_view, i - 1)
                            with gil:
                                return _create_simple_geometry_raise_error(errstate)

                        temp_rings_view[rings_idx] = <np.intp_t>ring
                        rings_idx += 1

                    part = GEOSGeom_createPolygon_r(
                        geos_handle,
                        <GEOSGeometry*> temp_rings_view[0],
                        <GEOSGeometry**> &temp_rings_view[1 if rings_idx > 1 else 0],
                        rings_idx - 1
                    )
                    if part == NULL:
                        _deallocate_arr(geos_handle, temp_rings_view, rings_idx - 1)
                        _deallocate_arr(geos_handle, temp_parts_view, parts_idx)
                        _deallocate_arr(geos_handle, temp_geoms_view, i - 1)
                        with gil:
                            return  # GEOSException is raised by get_geos_handle

                    temp_parts_view[parts_idx] = <np.intp_t>part
                    parts_idx += 1

                geom = GEOSGeom_createCollection_r(
                    geos_handle,
                    geometry_type,
                    <GEOSGeometry**> &temp_parts_view[0],
                    parts_idx
                    )
                if geom == NULL:
                    _deallocate_arr(geos_handle, temp_parts_view, parts_idx - 1)
                    _deallocate_arr(geos_handle, temp_geoms_view, i - 1)
                    with gil:
                        return  # GEOSException is raised by get_geos_handle

                temp_geoms_view[i] = <np.intp_t>geom

        for i in range(n_geoms):
            result_view[i] = PyGEOS_CreateGeometry(<GEOSGeometry *>temp_geoms_view[i], geos_handle)

    return result


def _geom_factory(uintptr_t g):

    with get_geos_handle() as geos_handle:
        geom = PyGEOS_CreateGeometry(<GEOSGeometry *>g, geos_handle)

    return geom


def linestring_to_linearring(object line):
    cdef GEOSGeometry *geom = NULL
    cdef const GEOSCoordSequence *seq = NULL
    cdef GEOSCoordSequence *seq_cloned = NULL
    cdef GEOSGeometry *ring = NULL
    cdef int geom_type

    with get_geos_handle() as geos_handle:
        if PyGEOS_GetGEOSGeometry(<PyObject*> line, &geom) == 0:
            raise TypeError(
                "The argument is of incorrect type. Please provide a Geometry object."
            )
        if geom == NULL:
            raise TypeError(
                "The argument is of incorrect type. Please provide a Geometry object."
            )

        geom_type = GEOSGeomTypeId_r(geos_handle, geom)
        if geom_type != 1:
            raise TypeError(
                "The argument is of incorrect type. Please provide a LineString object."
            )

        seq = GEOSGeom_getCoordSeq_r(geos_handle, geom)
        if seq == NULL:
            return

        seq_cloned = GEOSCoordSeq_clone_r(geos_handle, seq)
        if seq_cloned == NULL:
            return

        ring = GEOSGeom_createLinearRing_r(geos_handle, seq_cloned)
        if ring == NULL:
            return

        result = PyGEOS_CreateGeometry(ring, geos_handle)

    return result
