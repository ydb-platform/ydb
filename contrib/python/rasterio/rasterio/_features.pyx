"""Feature extraction"""

import logging
import warnings
from contextlib import ExitStack

import numpy as np

from rasterio import dtypes
from rasterio.dtypes import (
    _getnpdtype,
    bool_,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,
    float16,
    float32,
    float64,
)
from rasterio.enums import MergeAlg

from rasterio._err cimport exc_wrap_int, exc_wrap_pointer
from rasterio._io cimport DatasetReaderBase, DatasetWriterBase, MemoryDataset, io_auto
from rasterio.env import _GDAL_AT_LEAST_3_11, _GDAL_AT_LEAST_3_12_1


log = logging.getLogger(__name__)


def _shapes(image, mask, connectivity, transform):
    """
    Return a generator of (polygon, value) for each each set of adjacent pixels
    of the same value.

    Parameters
    ----------
    image : array or dataset object opened in 'r' mode or Band or tuple(dataset, bidx)
        Data type must be one of rasterio.int8, rasterio.int16, rasterio.int32,
        rasterio.uint8, rasterio.uint16, rasterio.float16, rasterio.float32, or rasterio.float64.
    mask : numpy.ndarray or rasterio Band object
        Values of False or 0 will be excluded from feature generation
        Must evaluate to bool (rasterio.bool_ or rasterio.uint8)
    connectivity : int
        Use 4 or 8 pixel connectivity for grouping pixels into features
    transform : Affine
        If not provided, feature coordinates will be generated based on pixel
        coordinates

    Returns
    -------
    Generator of (polygon, value)
        Yields a pair of (polygon, value) for each feature found in the image.
        Polygons are GeoJSON-like dicts and the values are the associated value
        from the image, in the data type of the image.
        Note: due to floating point precision issues, values returned from a
        floating point image may not exactly match the original values.

    """
    cdef int retval
    cdef int rows
    cdef int cols
    cdef GDALRasterBandH band = NULL
    cdef GDALRasterBandH maskband = NULL
    cdef GDALDriverH driver = NULL
    cdef OGRDataSourceH fs = NULL
    cdef OGRLayerH layer = NULL
    cdef OGRFieldDefnH fielddefn = NULL
    cdef char **options = NULL
    cdef MemoryDataset mem_ds = None
    cdef MemoryDataset mask_ds = None
    cdef ShapeIterator shape_iter = None
    cdef int fieldtp
    cdef bint is_float = _getnpdtype(image.dtype).kind == "f"
    cdef dict oft_dtypes = {
       int8: OFTInteger,
       int16: OFTInteger,
       int32: OFTInteger,
       int64: OFTInteger64,
       uint8: OFTInteger,
       uint16: OFTInteger,
       uint32: OFTInteger64,
       uint64: OFTInteger64,
       float32: OFTReal,
       float64: OFTReal,
    }
    if _GDAL_AT_LEAST_3_11:
        oft_dtypes[float16] = OFTReal

    cdef str dtype_name = _getnpdtype(image.dtype).name
    if (fieldtp := oft_dtypes.get(dtype_name, -1)) == -1:
        raise ValueError(f"image dtype must be one of: {', '.join(oft_dtypes)}")

    truncated_dtypes = (uint64,)
    if not _GDAL_AT_LEAST_3_12_1:
        truncated_dtypes += (float64,)

    if dtype_name in truncated_dtypes:
        internal_dtype = "float32" if is_float else "int64"
        warnings.warn(
            f"The low-level implementation uses a {internal_dtype} buffer. "
            "Truncation issues may occur."
        )

    if connectivity not in (4, 8):
        raise ValueError("Connectivity Option must be 4 or 8")

    with ExitStack() as exit_stack:
        if dtypes.is_ndarray(image):
            mem_ds = exit_stack.enter_context(MemoryDataset(image, transform=transform))
            band = mem_ds.band(1)
        elif isinstance(image, tuple):
            rdr = image.ds
            band = (<DatasetReaderBase?>rdr).band(image.bidx)
        else:
            raise ValueError("Invalid source image")

        if mask is not None:
            if mask.shape != image.shape:
                raise ValueError("Mask must have same shape as image")

            if _getnpdtype(mask.dtype).name not in (bool_, uint8):
                raise ValueError(
                    "Mask must be dtype rasterio.bool_ or rasterio.uint8"
                )

            if dtypes.is_ndarray(mask):
                # A boolean mask must be converted to uint8 for GDAL
                mask_ds = exit_stack.enter_context(
                    MemoryDataset(mask.astype(np.uint8), transform=transform)
                )
                maskband = mask_ds.band(1)
            elif isinstance(mask, tuple):
                mrdr = mask.ds
                maskband = (<DatasetReaderBase?>mrdr).band(mask.bidx)

        # Create an in-memory feature store.
        driver = OGRGetDriverByName("Memory")
        if driver == NULL:
            raise ValueError("NULL driver")
        fs = OGR_Dr_CreateDataSource(driver, "temp", NULL)
        if fs == NULL:
            raise ValueError("NULL feature dataset")

        # And a layer.
        layer = OGR_DS_CreateLayer(fs, "polygons", NULL, 3, NULL)
        if layer == NULL:
            raise ValueError("NULL layer")

        fielddefn = OGR_Fld_Create("image_value", fieldtp)
        if fielddefn == NULL:
            raise ValueError("NULL field definition")
        OGR_L_CreateField(layer, fielddefn, 1)
        OGR_Fld_Destroy(fielddefn)

        try:
            if connectivity == 8:
                options = CSLSetNameValue(options, "8CONNECTED", "8")

            if is_float:
                GDALFPolygonize(band, maskband, layer, 0, options, NULL, NULL)
            else:
                GDALPolygonize(band, maskband, layer, 0, options, NULL, NULL)
        finally:
            if options:
                CSLDestroy(options)

    try:
        # Yield Fiona-style features
        shape_iter = ShapeIterator()
        shape_iter.layer = layer
        shape_iter.fieldtype = fieldtp
        for s, v in shape_iter:
            yield s, v

    finally:
        if fs != NULL:
            OGR_DS_Destroy(fs)


def _sieve(image, size, out, mask, connectivity):
    """Remove small polygon regions from a raster.

    Parameters
    ----------
    image : ndarray or Band
        The source is a 2 or 3-D ndarray, or a single or a multiple
        Rasterio Band object.  Must be of type rasterio.int16,
        rasterio.int32, rasterio.uint8 or rasterio.uint16.
    size : int
        minimum polygon size (number of pixels) to retain.
    out : numpy ndarray
        Array of same shape and data type as `image` in which to store
        results.
    mask : numpy ndarray or rasterio Band object
        Values of False or 0 will be excluded from feature generation.
        Must evaluate to bool (rasterio.bool_ or rasterio.uint8)
    connectivity : int
        Use 4 or 8 pixel connectivity for grouping pixels into features.

    """
    cdef int retval
    cdef int rows
    cdef int cols
    cdef MemoryDataset in_mem_ds = None
    cdef MemoryDataset out_mem_ds = None
    cdef MemoryDataset mask_mem_ds = None
    cdef GDALRasterBandH in_band = NULL
    cdef GDALRasterBandH out_band = NULL
    cdef GDALRasterBandH mask_band = NULL

    valid_dtypes = (int16, int32, uint8, uint16)

    if _getnpdtype(image.dtype).name not in valid_dtypes:
        valid_types_str = ', '.join(('rasterio.{0}'.format(t) for t in valid_dtypes))
        raise ValueError(
            "image dtype must be one of: {0}".format(valid_types_str))

    if size <= 0:
        raise ValueError('size must be greater than 0')
    elif type(size) == float:
        raise ValueError('size must be an integer number of pixels')
    elif size > (image.shape[0] * image.shape[1]):
        raise ValueError('size must be smaller than size of image')

    if connectivity not in (4, 8):
        raise ValueError('connectivity must be 4 or 8')

    if out.shape != image.shape:
        raise ValueError('out raster shape must be same as image shape')

    if _getnpdtype(image.dtype).name != _getnpdtype(out.dtype).name:
        raise ValueError('out raster must match dtype of image')

    with ExitStack() as exit_stack:
        if dtypes.is_ndarray(image):
            if len(image.shape) == 2:
                image = image.reshape(1, *image.shape)
            src_count = image.shape[0]
            src_bidx = list(range(1, src_count + 1))
            in_mem_ds = exit_stack.enter_context(MemoryDataset(image))
            src_dataset = in_mem_ds

        elif isinstance(image, tuple):
            src_dataset, src_bidx, dtype, shape = image
            if isinstance(src_bidx, int):
                src_bidx = [src_bidx]

        else:
            raise ValueError("Invalid source image")

        if dtypes.is_ndarray(out):
            log.debug("out array: %r", out)
            if len(out.shape) == 2:
                out = out.reshape(1, *out.shape)
            dst_count = out.shape[0]
            dst_bidx = list(range(1, dst_count + 1))
            out_mem_ds = exit_stack.enter_context(MemoryDataset(out))
            dst_dataset = out_mem_ds

        elif isinstance(out, tuple):
            dst_dataset, dst_bidx, _, _ = out
            if isinstance(dst_bidx, int):
                dst_bidx = [dst_bidx]

        else:
            raise ValueError("Invalid out image")

        if mask is not None:
            if mask.shape != image.shape[-2:]:
                raise ValueError("Mask must have same shape as image")

            if _getnpdtype(mask.dtype) not in (bool_, uint8):
                raise ValueError("Mask must be dtype rasterio.bool_ or "
                                "rasterio.uint8")

            if dtypes.is_ndarray(mask):
                # A boolean mask must be converted to uint8 for GDAL
                mask_mem_ds = exit_stack.enter_context(MemoryDataset(mask.astype(np.uint8)))
                mask_band = mask_mem_ds.band(1)

            elif isinstance(mask, tuple):
                mask_reader = mask.ds
                mask_band = (<DatasetReaderBase?>mask_reader).band(mask.bidx)

        for i, j in zip(src_bidx, dst_bidx):
            in_band = (<DatasetReaderBase?>src_dataset).band(i)
            out_band = (<DatasetReaderBase?>dst_dataset).band(j)
            GDALSieveFilter(in_band, mask_band, out_band, size, connectivity, NULL, NULL, NULL)
            io_auto(out[i - 1], out_band, False)

    if out.shape[0] == 1:
        out = out[0]

    return out


def _rasterize(shapes, image, transform, all_touched, merge_alg):
    """Burns input geometries into `image`.

    The `image` array is modified in place.

    Parameters
    ----------
    shapes : iterable of (geometry, value) pairs
        `geometry` is a GeoJSON-like object.
    image : numpy.ndarray or open dataset object
        Array in which to store results.
    transform : Affine transformation object, optional
        Transformation from pixel coordinates of `image` to the
        coordinate system of the input `shapes`. See the `transform`
        property of dataset objects.
    all_touched : boolean, optional
        If True, all pixels touched by geometries will be burned in. If
        false, only pixels whose center is within the polygon or that
        are selected by Bresenham's line algorithm will be burned in.
    merge_alg : MergeAlg, required
        Merge algorithm to use.  One of:
            MergeAlg.replace (default):
                the new value will overwrite the existing value.
            MergeAlg.add:
                the new value will be added to the existing raster.

    Returns
    -------
    None

    Notes
    -----
    This function uses significant memory resources.
    GDALRasterizeGeometries does the bulk of the work and requires a
    working buffer. The size of this buffer is the smaller of the
    `image` data or GDAL's maximum cache size.  That latter value is 5%
    of a computer's physical RAM unless otherwise specified using the
    GDAL_CACHEMAX configuration option. Additionally, this function uses
    a temporary in-memory dataset containing a copy of the input `image`
    data and an array of OGRGeometryH structs. The size of that array is
    approximately equal to the size of all objects in `shapes`. Note
    that the `shapes` iterator is also materialized to a list within
    this function.

    If the working buffer is smaller than the `image` data, the array of
    shapes will be iterated multiple times. Performance is thus a linear
    function of the buffer size. For maximum speed, ensure that
    GDAL_CACHEMAX is larger than the size of the input `image`.

    The minimum memory requirement of this function is approximately
    equal to 2x the `image` data size plus 2x the smaller of the the
    `image` data and GDAL max cache size, plus 2x the size of the
    objects in `shapes`.

    """
    cdef int retval
    cdef int i
    cdef size_t num_geoms = 0
    cdef OGRGeometryH *geoms = NULL
    cdef char **options = NULL
    cdef double *pixel_values = NULL
    cdef MemoryDataset mem = None
    cdef int *band_ids = NULL

    try:
        if all_touched:
            options = CSLSetNameValue(options, "ALL_TOUCHED", "TRUE")
        merge_algorithm = merge_alg.value.encode('utf-8')
        options = CSLSetNameValue(options, "MERGE_ALG", merge_algorithm)

        # GDAL needs an array of geometries.
        # For now, we'll build a Python list on the way to building that
        # C array. TODO: make this more efficient.
        all_shapes = list(shapes)
        num_geoms = len(all_shapes)

        geoms = <OGRGeometryH *>CPLMalloc(
            num_geoms * sizeof(OGRGeometryH))
        pixel_values = <double *>CPLMalloc(num_geoms * sizeof(double))

        # initialize all geoms to NULL
        for i in range(<int>num_geoms):
            geoms[i] = NULL

        for i, (geometry, value) in enumerate(all_shapes):
            try:
                geoms[i] = OGRGeomBuilder().build(geometry)
                pixel_values[i] = <double>value
            except Exception as error:
                log.error(
                    "Geometry %r at index %d with value %d skipped due to error: %r",
                    geometry, i, value, error
                )

        if isinstance(image, DatasetWriterBase):
            band_ids = <int *>CPLMalloc(<int>image.count*sizeof(int))
            for i in range(<int>image.count):
                band_ids[i] = i + 1
            exc_wrap_int(
                GDALRasterizeGeometries(
                    <GDALDatasetH>((<DatasetWriterBase>image)._hds), 1, band_ids, num_geoms, geoms, NULL,
                    NULL, pixel_values, options, NULL, NULL))
        else:
            # TODO: is a vsimem file more memory efficient?
            with MemoryDataset(image, transform=transform) as mem:
                band_ids = <int *>CPLMalloc(<int>mem.count*sizeof(int))
                for i in range(<int>mem.count):
                    band_ids[i] = i + 1
                exc_wrap_int(
                    GDALRasterizeGeometries(
                        mem.handle(), 1, band_ids, num_geoms, geoms, NULL,
                        NULL, pixel_values, options, NULL, NULL))

    finally:
        if geoms != NULL:
            for i in range(<int>num_geoms):
                _deleteOgrGeom(geoms[i])
        CPLFree(geoms)
        CPLFree(pixel_values)
        CPLFree(band_ids)
        CSLDestroy(options)


def _explode(coords):
    """Explode a GeoJSON geometry's coordinates object and yield
    coordinate tuples. As long as the input is conforming, the type of
    the geometry doesn't matter.  From Fiona 1.4.8"""
    for e in coords:
        if isinstance(e, (float, int)):
            yield coords
            break
        else:
            for f in _explode(e):
                yield f


def _bounds(geometry, north_up=True, transform=None):
    """Bounding box of a GeoJSON geometry, GeometryCollection, or FeatureCollection.

    left, bottom, right, top
    *not* xmin, ymin, xmax, ymax

    If not north_up, y will be switched to guarantee the above.

    From Fiona 1.4.8 with updates here to handle feature collections.
    TODO: add to Fiona.
    """

    if 'features' in geometry and geometry["features"]:
        xmins, ymins, xmaxs, ymaxs = zip(
            *[_bounds(feat["geometry"]) for feat in geometry["features"]]
        )
        if north_up:
            return min(xmins), min(ymins), max(xmaxs), max(ymaxs)
        else:
            return min(xmins), max(ymaxs), max(xmaxs), min(ymins)

    elif 'geometries' in geometry and geometry['geometries']:
        xmins, ymins, xmaxs, ymaxs = zip(*[_bounds(geom) for geom in geometry["geometries"]])
        if north_up:
            return min(xmins), min(ymins), max(xmaxs), max(ymaxs)
        else:
            return min(xmins), max(ymaxs), max(xmaxs), min(ymins)

    elif 'coordinates' in geometry:
        # Input is a singular geometry object
        if transform is not None:
            xyz = list(_explode(geometry['coordinates']))
            # Because the affine transform matrix only applies in 2D we
            # must slice away any possible Z coordinate from a point.
            xyz_px = [transform * point[:2] for point in xyz]
            xyz = tuple(zip(*xyz_px))
            return min(xyz[0]), max(xyz[1]), max(xyz[0]), min(xyz[1])
        else:
            xyz = tuple(zip(*list(_explode(geometry['coordinates']))))
            if north_up:
                return min(xyz[0]), min(xyz[1]), max(xyz[0]), max(xyz[1])
            else:
                return min(xyz[0]), max(xyz[1]), max(xyz[0]), min(xyz[1])

    # all valid inputs returned above, so whatever falls through is an error
    raise ValueError(
            "geometry must be a GeoJSON-like geometry, GeometryCollection, "
            "or FeatureCollection"
        )

# Mapping of OGR integer geometry types to GeoJSON type names.
GEOMETRY_TYPES = {
    0: 'Unknown',
    1: 'Point',
    2: 'LineString',
    3: 'Polygon',
    4: 'MultiPoint',
    5: 'MultiLineString',
    6: 'MultiPolygon',
    7: 'GeometryCollection',
    100: 'None',
    101: 'LinearRing',
    0x80000001: '3D Point',
    0x80000002: '3D LineString',
    0x80000003: '3D Polygon',
    0x80000004: '3D MultiPoint',
    0x80000005: '3D MultiLineString',
    0x80000006: '3D MultiPolygon',
    0x80000007: '3D GeometryCollection'
}

# Mapping of GeoJSON type names to OGR integer geometry types
GEOJSON2OGR_GEOMETRY_TYPES = dict(
    (v, k) for k, v in GEOMETRY_TYPES.iteritems()
)


# Geometry related functions and classes follow.


cdef _deleteOgrGeom(OGRGeometryH geom):
    """Delete an OGR geometry"""
    if geom != NULL:
        OGR_G_DestroyGeometry(geom)
    geom = NULL


cdef class GeomBuilder:
    """Builds a GeoJSON (Fiona-style) geometry from an OGR geometry."""

    cdef _buildCoords(self, OGRGeometryH geom):
        # Build a coordinate sequence
        cdef int i
        cdef list coords = []
        if geom == NULL:
            raise ValueError("Null geom")
        npoints = OGR_G_GetPointCount(geom)

        if self.ndims == 2:
            for i in range(npoints):
                coords.append((OGR_G_GetX(geom, i), OGR_G_GetY(geom, i)))
        else:
            for i in range(npoints):
                coords.append((OGR_G_GetX(geom, i), OGR_G_GetY(geom, i), OGR_G_GetZ(geom, i)))
        return coords

    cpdef _buildPoint(self):
        return {
            'type': 'Point',
            'coordinates': self._buildCoords(self.geom)[0]}

    cpdef _buildLineString(self):
        return {
            'type': 'LineString',
            'coordinates': self._buildCoords(self.geom)}

    cpdef _buildLinearRing(self):
        return {
            'type': 'LinearRing',
            'coordinates': self._buildCoords(self.geom)}

    cdef _buildParts(self, OGRGeometryH geom):
        cdef int j
        cdef OGRGeometryH part
        if geom == NULL:
            raise ValueError("Null geom")
        parts = []
        for j in range(OGR_G_GetGeometryCount(geom)):
            part = OGR_G_GetGeometryRef(geom, j)
            parts.append(GeomBuilder().build(part))
        return parts

    cpdef _buildPolygon(self):
        coordinates = [p['coordinates'] for p in self._buildParts(self.geom)]
        return {'type': 'Polygon', 'coordinates': coordinates}

    cpdef _buildMultiPoint(self):
        coordinates = [p['coordinates'] for p in self._buildParts(self.geom)]
        return {'type': 'MultiPoint', 'coordinates': coordinates}

    cpdef _buildMultiLineString(self):
        coordinates = [p['coordinates'] for p in self._buildParts(self.geom)]
        return {'type': 'MultiLineString', 'coordinates': coordinates}

    cpdef _buildMultiPolygon(self):
        coordinates = [p['coordinates'] for p in self._buildParts(self.geom)]
        return {'type': 'MultiPolygon', 'coordinates': coordinates}

    cpdef _buildGeometryCollection(self):
        geometries = [geom for geom in self._buildParts(self.geom)]
        return {'type': 'GeometryCollection', 'geometries': geometries}

    cdef build(self, OGRGeometryH geom):
        """Builds a GeoJSON object from an OGR geometry object."""
        if geom == NULL:
            raise ValueError("Null geom")
        cdef unsigned int etype = OGR_G_GetGeometryType(geom)
        self.code = etype
        self.geomtypename = GEOMETRY_TYPES[self.code & (~0x80000000)]
        self.ndims = OGR_G_GetCoordinateDimension(geom)
        self.geom = geom

        try:
            return getattr(self, '_build' + self.geomtypename)()
        except AttributeError:
            raise ValueError(f"Unsupported geometry type {self.geomtypename}")


cdef class OGRGeomBuilder:
    """
    Builds an OGR geometry from GeoJSON geometry.
    From Fiona: https://github.com/Toblerity/Fiona/blob/master/fiona/ogrext.pyx
    """

    cdef OGRGeometryH _createOgrGeometry(self, int geom_type) except NULL:
        cdef OGRGeometryH geom = OGR_G_CreateGeometry(geom_type)
        if geom is NULL:
            raise Exception(
                "Could not create OGR Geometry of type: %i" % geom_type)
        return geom

    cdef _addPointToGeometry(self, OGRGeometryH geom, object coordinate):
        if len(coordinate) == 2:
            x, y = coordinate
            OGR_G_AddPoint_2D(geom, x, y)
        else:
            x, y, z = coordinate[:3]
            OGR_G_AddPoint(geom, x, y, z)

    cdef OGRGeometryH _buildPoint(self, object coordinates) except NULL:
        cdef OGRGeometryH geom = self._createOgrGeometry(
            GEOJSON2OGR_GEOMETRY_TYPES['Point'])
        self._addPointToGeometry(geom, coordinates)
        return geom

    cdef OGRGeometryH _buildLineString(self, object coordinates) except NULL:
        cdef OGRGeometryH geom = self._createOgrGeometry(
            GEOJSON2OGR_GEOMETRY_TYPES['LineString'])
        for coordinate in coordinates:
            self._addPointToGeometry(geom, coordinate)
        return geom

    cdef OGRGeometryH _buildLinearRing(self, object coordinates) except NULL:
        cdef OGRGeometryH geom = self._createOgrGeometry(
            GEOJSON2OGR_GEOMETRY_TYPES['LinearRing'])
        for coordinate in coordinates:
            self._addPointToGeometry(geom, coordinate)
        OGR_G_CloseRings(geom)
        return geom

    cdef OGRGeometryH _buildPolygon(self, object coordinates) except NULL:
        cdef OGRGeometryH ring = NULL
        cdef OGRGeometryH geom = self._createOgrGeometry(
            GEOJSON2OGR_GEOMETRY_TYPES['Polygon'])
        for r in coordinates:
            ring = self._buildLinearRing(r)
            OGR_G_AddGeometryDirectly(geom, ring)
        return geom

    cdef OGRGeometryH _buildMultiPoint(self, object coordinates) except NULL:
        cdef OGRGeometryH part = NULL
        cdef OGRGeometryH geom = self._createOgrGeometry(
            GEOJSON2OGR_GEOMETRY_TYPES['MultiPoint'])
        for coordinate in coordinates:
            part = self._buildPoint(coordinate)
            OGR_G_AddGeometryDirectly(geom, part)
        return geom

    cdef OGRGeometryH _buildMultiLineString(
            self, object coordinates) except NULL:
        cdef OGRGeometryH part = NULL
        cdef OGRGeometryH geom = self._createOgrGeometry(
            GEOJSON2OGR_GEOMETRY_TYPES['MultiLineString'])
        for line in coordinates:
            part = self._buildLineString(line)
            OGR_G_AddGeometryDirectly(geom, part)
        return geom

    cdef OGRGeometryH _buildMultiPolygon(self, object coordinates) except NULL:
        cdef OGRGeometryH part = NULL
        cdef OGRGeometryH geom = self._createOgrGeometry(
            GEOJSON2OGR_GEOMETRY_TYPES['MultiPolygon'])
        for poly in coordinates:
            part = self._buildPolygon(poly)
            OGR_G_AddGeometryDirectly(geom, part)
        return geom

    cdef OGRGeometryH _buildGeometryCollection(self, object geoms) except NULL:
        cdef OGRGeometryH part = NULL
        cdef OGRGeometryH ogr_geom = self._createOgrGeometry(
            GEOJSON2OGR_GEOMETRY_TYPES['GeometryCollection'])
        for g in geoms:
            part = OGRGeomBuilder().build(g)
            OGR_G_AddGeometryDirectly(ogr_geom, part)
        return ogr_geom

    cdef OGRGeometryH build(self, object geometry) except NULL:
        """Builds an OGR geometry from GeoJSON geometry.
        Assumes that geometry has been validated prior to calling this; this
        only does basic checks for validity.
        """
        geometry = getattr(geometry, "__geo_interface__", geometry)
        cdef object typename = geometry['type']
        cdef object coordinates
        cdef object geometries

        valid_types = {'Point', 'MultiPoint', 'LineString', 'LinearRing',
                       'MultiLineString', 'Polygon', 'MultiPolygon'}

        if typename in valid_types:
            coordinates = geometry.get('coordinates')
            if not (coordinates and len(coordinates) > 0):
                raise ValueError("Input is not a valid geometry object")

            if typename == 'Point':
                return self._buildPoint(coordinates)
            elif typename == 'LineString':
                return self._buildLineString(coordinates)
            elif typename == 'LinearRing':
                return self._buildLinearRing(coordinates)
            elif typename == 'Polygon':
                return self._buildPolygon(coordinates)
            elif typename == 'MultiPoint':
                return self._buildMultiPoint(coordinates)
            elif typename == 'MultiLineString':
                return self._buildMultiLineString(coordinates)
            elif typename == 'MultiPolygon':
                return self._buildMultiPolygon(coordinates)

        elif typename == 'GeometryCollection':
            geometries = geometry.get('geometries')
            if not (geometries and len(geometries) > 0):
                raise ValueError("Input is not a valid geometry object")

            return self._buildGeometryCollection(geometries)

        else:
            raise ValueError("Unsupported geometry type %s" % typename)


# Feature extension classes and functions follow.

cdef _deleteOgrFeature(OGRFeatureH feat):
    """Delete an OGR feature"""
    if feat != NULL:
        OGR_F_Destroy(feat)
    feat = NULL


cdef class ShapeIterator:
    """Provides an iterator over shapes in an OGR feature layer."""

    def __iter__(self):
        OGR_L_ResetReading(self.layer)
        return self

    def __next__(self):
        cdef OGRFeatureH feat = NULL
        cdef OGRGeometryH geom = NULL

        try:
            feat = OGR_L_GetNextFeature(self.layer)

            if feat == NULL:
                raise StopIteration

            if self.fieldtype == 0:
                image_value = OGR_F_GetFieldAsInteger(feat, 0)
            else:
                image_value = OGR_F_GetFieldAsDouble(feat, 0)
            geom = OGR_F_GetGeometryRef(feat)
            if geom != NULL:
                shape = GeomBuilder().build(geom)
            else:
                shape = None
            return shape, image_value

        finally:
            _deleteOgrFeature(feat)
