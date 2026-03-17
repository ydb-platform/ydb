# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Implements RasterSource classes which can retrieve imagery from web services
such as WMS and WMTS.

The matplotlib interface can make use of RasterSources via the
:meth:`cartopy.mpl.geoaxes.GeoAxes.add_raster` method,
with additional specific methods which make use of this for WMS and WMTS
(:meth:`~cartopy.mpl.geoaxes.GeoAxes.add_wms` and
:meth:`~cartopy.mpl.geoaxes.GeoAxes.add_wmts`). An example of using WMTS in
this way can be found at :ref:`sphx_glr_gallery_web_services_wmts.py`.

"""

import collections
import io
import math
from pathlib import Path
from urllib.parse import urlparse
import warnings
import weakref
from xml.etree import ElementTree

import numpy as np
from PIL import Image
import shapely.geometry as sgeom

import cartopy


try:
    import owslib.util
    from owslib.wfs import WebFeatureService
    from owslib.wms import WebMapService
    import owslib.wmts

    _OWSLIB_AVAILABLE = True
except ImportError:
    WebMapService = None
    WebFeatureService = None
    _OWSLIB_AVAILABLE = False

import cartopy.crs as ccrs
from cartopy.img_transform import warp_array
from cartopy.io import LocatedImage, RasterSource


_OWSLIB_REQUIRED = 'OWSLib is required to use OGC web services.'

# Hardcode some known EPSG codes for now.
# The order given here determines the preferred SRS for WMS retrievals.
_CRS_TO_OGC_SRS = collections.OrderedDict(
    [(ccrs.PlateCarree(), ['EPSG:4326']),
     (ccrs.Mercator.GOOGLE, ['EPSG:3857', 'EPSG:900913']),
     (ccrs.OSGB(approx=True), ['EPSG:27700'])
     ])

# Standard pixel size of 0.28 mm as defined by WMTS.
METERS_PER_PIXEL = 0.28e-3

_WGS84_METERS_PER_UNIT = 2 * math.pi * 6378137 / 360

METERS_PER_UNIT = {
    'urn:ogc:def:crs:EPSG::27700': 1,
    'urn:ogc:def:crs:EPSG::900913': 1,
    'urn:ogc:def:crs:OGC:1.3:CRS84': _WGS84_METERS_PER_UNIT,
    'urn:ogc:def:crs:EPSG::3031': 1,
    'urn:ogc:def:crs:EPSG::3413': 1,
    'urn:ogc:def:crs:EPSG::3857': 1,
    'urn:ogc:def:crs:EPSG:6.18.3:3857': 1
}

_URN_TO_CRS = collections.OrderedDict(
    [('urn:ogc:def:crs:OGC:1.3:CRS84', ccrs.PlateCarree()),
     ('urn:ogc:def:crs:EPSG::4326', ccrs.PlateCarree()),
     ('urn:ogc:def:crs:EPSG::900913', ccrs.Mercator.GOOGLE),
     ('urn:ogc:def:crs:EPSG::27700', ccrs.OSGB(approx=True)),
     ('urn:ogc:def:crs:EPSG::3031', ccrs.Stereographic(
         central_latitude=-90,
         true_scale_latitude=-71)),
     ('urn:ogc:def:crs:EPSG::3413', ccrs.Stereographic(
         central_longitude=-45,
         central_latitude=90,
         true_scale_latitude=70)),
     ('urn:ogc:def:crs:EPSG::3857', ccrs.Mercator.GOOGLE),
     ('urn:ogc:def:crs:EPSG:6.18.3:3857', ccrs.Mercator.GOOGLE)
     ])

# XML namespace definitions
_MAP_SERVER_NS = '{http://mapserver.gis.umn.edu/mapserver}'
_GML_NS = '{http://www.opengis.net/gml}'


def _warped_located_image(image, source_projection, source_extent,
                          output_projection, output_extent, target_resolution):
    """
    Reproject an Image from one source-projection and extent to another.

    Returns
    -------
    LocatedImage
        A reprojected LocatedImage, the extent of which is >= the requested
        'output_extent'.

    """
    if source_projection == output_projection:
        extent = output_extent
    else:
        # Convert Image to numpy array (flipping so that origin
        # is 'lower').
        # Convert to RGBA to keep the color palette in the regrid process
        # if any
        img, extent = warp_array(np.asanyarray(image.convert('RGBA'))[::-1],
                                 source_proj=source_projection,
                                 source_extent=source_extent,
                                 target_proj=output_projection,
                                 target_res=np.asarray(target_resolution,
                                                       dtype=int),
                                 target_extent=output_extent,
                                 mask_extrapolated=True)

        # Convert arrays with masked RGB(A) values to non-masked RGBA
        # arrays, setting the alpha channel to zero for masked values.
        # This avoids unsightly grey boundaries appearing when the
        # extent is limited (i.e. not global).
        if np.ma.is_masked(img):
            img[:, :, 3] = np.where(np.any(img.mask, axis=2), 0,
                                    img[:, :, 3])
            img = img.data

        # Convert warped image array back to an Image, undoing the
        # earlier flip.
        image = Image.fromarray(img[::-1])

    return LocatedImage(image, extent)


def _target_extents(extent, requested_projection, available_projection):
    """
    Translate the requested extent in the display projection into a list of
    extents in the projection available from the service (multiple if it
    crosses seams).

    The extents are represented as (min_x, max_x, min_y, max_y).

    """
    # Start with the requested area.
    min_x, max_x, min_y, max_y = extent
    target_box = sgeom.box(min_x, min_y, max_x, max_y)

    # If the requested area (i.e. target_box) is bigger (or nearly bigger) than
    # the entire output requested_projection domain, then we erode the request
    # area to avoid re-projection instabilities near the projection boundary.
    buffered_target_box = target_box.buffer(requested_projection.threshold,
                                            resolution=1)
    fudge_mode = buffered_target_box.contains(requested_projection.domain)
    if fudge_mode:
        target_box = requested_projection.domain.buffer(
            -requested_projection.threshold)

    # Translate the requested area into the server projection.
    polys = available_projection.project_geometry(target_box,
                                                  requested_projection)

    # Return the polygons' rectangular bounds as extent tuples.
    target_extents = []
    for poly in polys.geoms:
        min_x, min_y, max_x, max_y = poly.bounds
        if fudge_mode:
            # If we shrunk the request area before, then here we
            # need to re-inflate.
            radius = min(max_x - min_x, max_y - min_y) / 5.0
            radius = min(radius, available_projection.threshold * 15)
            poly = poly.buffer(radius, resolution=1)
            # Prevent the expanded request going beyond the
            # limits of the requested_projection.
            poly = available_projection.domain.intersection(poly)
            min_x, min_y, max_x, max_y = poly.bounds
        target_extents.append((min_x, max_x, min_y, max_y))

    return target_extents


class WMSRasterSource(RasterSource):
    """
    A WMS imagery retriever which can be added to a map.

    Note
    ----
        Requires owslib and Pillow to work.

    No caching of retrieved maps is done with this WMSRasterSource.

    To reduce load on the WMS server it is encouraged to tile
    map requests and subsequently stitch them together to recreate
    a single raster, thus allowing for a more aggressive caching scheme,
    but this WMSRasterSource does not currently implement WMS tile
    fetching.

    Whilst not the same service, there is also a WMTSRasterSource which
    makes use of tiles and comes with built-in caching for fast repeated
    map retrievals.

    """

    def __init__(self, service, layers, getmap_extra_kwargs=None):
        """
        Parameters
        ----------
        service: string or WebMapService instance
            The WebMapService instance, or URL of a WMS service,
            from whence to retrieve the image.
        layers: string or list of strings
            The name(s) of layers to use from the WMS service.
        getmap_extra_kwargs: dict, optional
            Extra keywords to pass through to the service's getmap method.
            If None, a dictionary with ``{'transparent': True}`` will be
            defined.

        """
        if WebMapService is None:
            raise ImportError(_OWSLIB_REQUIRED)

        if isinstance(service, str):
            service = WebMapService(service)

        if isinstance(layers, str):
            layers = [layers]

        if getmap_extra_kwargs is None:
            getmap_extra_kwargs = {'transparent': True}

        if len(layers) == 0:
            raise ValueError('One or more layers must be defined.')
        for layer in layers:
            if layer not in service.contents:
                raise ValueError(f'The {layer!r} layer does not exist in '
                                 'this service.')

        #: The OWSLib WebMapService instance.
        self.service = service

        #: The names of the layers to fetch.
        self.layers = layers

        #: Extra kwargs passed through to the service's getmap request.
        self.getmap_extra_kwargs = getmap_extra_kwargs

    def _native_srs(self, projection):
        # Return a list of all SRS identifiers that correspond to the given
        # projection when known, otherwise return None.
        native_srs_list = _CRS_TO_OGC_SRS.get(projection, None)

        # If the native_srs could not be identified, return None
        if native_srs_list is None:
            return None
        else:
            # If the native_srs was identified, check if it is provided
            # by the service. If not return None to continue checking
            # for available fallback srs
            contents = self.service.contents

            for native_srs in native_srs_list:
                native_OK = all(
                    native_srs.lower() in map(
                        str.lower, contents[layer].crsOptions)
                    for layer in self.layers
                )
                if native_OK:
                    return native_srs

            return None

    def _fallback_proj_and_srs(self):
        """
        Return a :class:`cartopy.crs.Projection` and corresponding
        SRS string in which the WMS service can supply the requested
        layers.

        """
        contents = self.service.contents
        for proj, srs_list in _CRS_TO_OGC_SRS.items():
            for srs in srs_list:
                srs_OK = all(
                    srs.lower() in map(str.lower, contents[layer].crsOptions)
                    for layer in self.layers)
                if srs_OK:
                    return proj, srs

        raise ValueError('The requested layers are not available in a '
                         'known SRS.')

    def validate_projection(self, projection):
        if self._native_srs(projection) is None:
            self._fallback_proj_and_srs()

    def _image_and_extent(self, wms_proj, wms_srs, wms_extent, output_proj,
                          output_extent, target_resolution):
        min_x, max_x, min_y, max_y = wms_extent
        wms_image = self.service.getmap(layers=self.layers,
                                        srs=wms_srs,
                                        bbox=(min_x, min_y, max_x, max_y),
                                        size=target_resolution,
                                        format='image/png',
                                        **self.getmap_extra_kwargs)
        wms_image = Image.open(io.BytesIO(wms_image.read()))

        return _warped_located_image(wms_image, wms_proj, wms_extent,
                                     output_proj, output_extent,
                                     target_resolution)

    def fetch_raster(self, projection, extent, target_resolution):
        target_resolution = [math.ceil(val) for val in target_resolution]
        wms_srs = self._native_srs(projection)
        if wms_srs is not None:
            wms_proj = projection
            wms_extents = [extent]
        else:
            # The SRS for the requested projection is not known, so
            # attempt to use the fallback and perform the necessary
            # transformations.
            wms_proj, wms_srs = self._fallback_proj_and_srs()

            # Calculate the bounding box(es) in WMS projection.
            wms_extents = _target_extents(extent, projection, wms_proj)

        located_images = []
        for wms_extent in wms_extents:
            located_images.append(self._image_and_extent(wms_proj, wms_srs,
                                                         wms_extent,
                                                         projection, extent,
                                                         target_resolution))
        return located_images


class WMTSRasterSource(RasterSource):
    """
    A WMTS imagery retriever which can be added to a map.

    Uses tile caching for fast repeated map retrievals.

    Note
    ----
        Requires owslib and Pillow to work.

    """

    _shared_image_cache = weakref.WeakKeyDictionary()
    """
    A nested mapping from WMTS, layer name, tile matrix name, tile row
    and tile column to the resulting PIL image::

        {wmts: {(layer_name, tile_matrix_name): {(row, column): Image}}}

    This provides a significant boost when producing multiple maps of the
    same projection or with an interactive figure.

    """

    def __init__(self, wmts, layer_name, gettile_extra_kwargs=None, cache=False):
        """
        Parameters
        ----------
        wmts
            The URL of the WMTS, or an owslib.wmts.WebMapTileService instance.
        layer_name
            The name of the layer to use.
        gettile_extra_kwargs: dict, optional
            Extra keywords (e.g. time) to pass through to the
            service's gettile method.
        cache : bool or str, optional
            If True, the default cache directory is used. If False, no cache is
            used. If a string, the string is used as the path to the cache.

        """
        if WebMapService is None:
            raise ImportError(_OWSLIB_REQUIRED)

        if not (hasattr(wmts, 'tilematrixsets') and
                hasattr(wmts, 'contents') and
                hasattr(wmts, 'gettile')):
            wmts = owslib.wmts.WebMapTileService(wmts)

        try:
            layer = wmts.contents[layer_name]
        except KeyError:
            raise ValueError(
                f'Invalid layer name {layer_name!r} for WMTS at {wmts.url!r}')

        #: The OWSLib WebMapTileService instance.
        self.wmts = wmts

        #: The layer to fetch.
        self.layer = layer

        #: Extra kwargs passed through to the service's gettile request.
        if gettile_extra_kwargs is None:
            gettile_extra_kwargs = {}
        self.gettile_extra_kwargs = gettile_extra_kwargs

        self._matrix_set_name_map = {}

        # Enable a cache mechanism when cache is equal to True or to a path.
        self._default_cache = False
        if cache is True:
            self._default_cache = True
            self.cache_path = Path(cartopy.config["cache_dir"])
        elif cache is False:
            self.cache_path = None
        else:
            self.cache_path = Path(cache)
        self.cache = set({})
        self._load_cache()

    def _matrix_set_name(self, target_projection):
        key = id(target_projection)
        matrix_set_name = self._matrix_set_name_map.get(key)
        if matrix_set_name is None:
            if hasattr(self.layer, 'tilematrixsetlinks'):
                matrix_set_names = self.layer.tilematrixsetlinks.keys()
            else:
                matrix_set_names = self.layer.tilematrixsets

            def find_projection(match_projection):
                result = None
                for tile_matrix_set_name in matrix_set_names:
                    matrix_sets = self.wmts.tilematrixsets
                    tile_matrix_set = matrix_sets[tile_matrix_set_name]
                    crs_urn = tile_matrix_set.crs
                    tms_crs = None
                    if crs_urn in _URN_TO_CRS:
                        tms_crs = _URN_TO_CRS.get(crs_urn)
                    elif ':EPSG:' in crs_urn:
                        epsg_num = crs_urn.split(':')[-1]
                        tms_crs = ccrs.epsg(int(epsg_num))
                    if tms_crs == match_projection:
                        result = tile_matrix_set_name
                        break
                return result

            # First search for a matrix set in the target projection.
            matrix_set_name = find_projection(target_projection)
            if matrix_set_name is None:
                # Search instead for a set in _any_ projection we can use.
                # Nothing to do for EPSG
                for possible_projection in _URN_TO_CRS.values():
                    # Look for supported projections (in a preferred order).
                    matrix_set_name = find_projection(possible_projection)
                    if matrix_set_name is not None:
                        break
                if matrix_set_name is None:
                    # Fail completely.
                    available_urns = sorted({
                        self.wmts.tilematrixsets[name].crs
                        for name in matrix_set_names})
                    msg = 'Unable to find tile matrix for projection.'
                    msg += f'\n    Projection: {target_projection}'
                    msg += '\n    Available tile CRS URNs:'
                    msg += '\n        ' + '\n        '.join(available_urns)
                    raise ValueError(msg)
            self._matrix_set_name_map[key] = matrix_set_name
        return matrix_set_name

    def validate_projection(self, projection):
        self._matrix_set_name(projection)

    def fetch_raster(self, projection, extent, target_resolution):
        matrix_set_name = self._matrix_set_name(projection)
        crs_urn = self.wmts.tilematrixsets[matrix_set_name].crs
        if crs_urn in _URN_TO_CRS:
            wmts_projection = _URN_TO_CRS[crs_urn]
        elif ':EPSG:' in crs_urn:
            epsg_num = crs_urn.split(':')[-1]
            wmts_projection = ccrs.epsg(int(epsg_num))
        else:
            raise ValueError(f'Unknown coordinate reference system string:'
                             f' {crs_urn}')

        if wmts_projection == projection:
            wmts_extents = [extent]
        else:
            # Calculate (possibly multiple) extents in the given projection.
            wmts_extents = _target_extents(extent, projection, wmts_projection)
            # Bump resolution by a small factor, as a weak alternative to
            # delivering a minimum projected resolution.
            # Generally, the desired area is smaller than the enclosing extent
            # in projection space and may have varying scaling, so the ideal
            # solution is a hard problem !
            resolution_factor = 1.4
            target_resolution = np.array(target_resolution) * resolution_factor

        width, height = target_resolution
        located_images = []
        for wmts_desired_extent in wmts_extents:
            # Calculate target resolution for the actual polygon.  Note that
            # this gives *every* polygon enough pixels for the whole result,
            # which is potentially excessive!
            min_x, max_x, min_y, max_y = wmts_desired_extent
            if wmts_projection == projection:
                max_pixel_span = min((max_x - min_x) / width,
                                     (max_y - min_y) / height)
            else:
                # X/Y orientation is arbitrary, so use a worst-case guess.
                max_pixel_span = (min(max_x - min_x, max_y - min_y) /
                                  max(width, height))

            # Fetch a suitable image and its actual extent.
            wmts_image, wmts_actual_extent = self._wmts_images(
                self.wmts, self.layer, matrix_set_name,
                extent=wmts_desired_extent,
                max_pixel_span=max_pixel_span)

            # Return each (image, extent) as a LocatedImage.
            if wmts_projection == projection:
                located_image = LocatedImage(wmts_image, wmts_actual_extent)
            else:
                # Reproject the image to the desired projection.
                located_image = _warped_located_image(
                    wmts_image,
                    wmts_projection, wmts_actual_extent,
                    output_projection=projection, output_extent=extent,
                    target_resolution=target_resolution)

            located_images.append(located_image)

        return located_images

    @property
    def _cache_dir(self):
        """Return the name of the cache directory"""
        return self.cache_path / self.__class__.__name__

    def _load_cache(self):
        """Load the cache"""
        if self.cache_path is not None:
            cache_dir = self._cache_dir
            if not cache_dir.exists():
                cache_dir.mkdir(parents=True, exist_ok=True)
                if self._default_cache:
                    warnings.warn(
                        'Cartopy created the following directory to cache '
                        f'WMTSRasterSource tiles: {cache_dir}')
            self.cache = self.cache.union(set(cache_dir.iterdir()))

    def _choose_matrix(self, tile_matrices, meters_per_unit, max_pixel_span):
        # Get the tile matrices in order of increasing resolution.
        tile_matrices = sorted(tile_matrices,
                               key=lambda tm: tm.scaledenominator,
                               reverse=True)

        # Find which tile matrix has the appropriate resolution.
        max_scale = max_pixel_span * meters_per_unit / METERS_PER_PIXEL
        for tm in tile_matrices:
            if tm.scaledenominator <= max_scale:
                return tm
        return tile_matrices[-1]

    def _tile_span(self, tile_matrix, meters_per_unit):
        pixel_span = (tile_matrix.scaledenominator *
                      (METERS_PER_PIXEL / meters_per_unit))
        tile_span_x = tile_matrix.tilewidth * pixel_span
        tile_span_y = tile_matrix.tileheight * pixel_span
        return tile_span_x, tile_span_y

    def _select_tiles(self, tile_matrix, tile_matrix_limits,
                      tile_span_x, tile_span_y, extent):
        # Convert the requested extent from CRS coordinates to tile
        # indices. See annex H of the WMTS v1.0.0 spec.
        # NB. The epsilons get rid of any tiles which only just
        # (i.e. one part in a million) intrude into the requested
        # extent. Since these wouldn't be visible anyway there's nothing
        # to be gained by spending the time downloading them.
        min_x, max_x, min_y, max_y = extent
        matrix_min_x, matrix_max_y = tile_matrix.topleftcorner
        epsilon = 1e-6
        min_col = int((min_x - matrix_min_x) / tile_span_x + epsilon)
        max_col = int((max_x - matrix_min_x) / tile_span_x - epsilon)
        min_row = int((matrix_max_y - max_y) / tile_span_y + epsilon)
        max_row = int((matrix_max_y - min_y) / tile_span_y - epsilon)
        # Clamp to the limits of the tile matrix.
        min_col = max(min_col, 0)
        max_col = min(max_col, tile_matrix.matrixwidth - 1)
        min_row = max(min_row, 0)
        max_row = min(max_row, tile_matrix.matrixheight - 1)
        # Clamp to any layer-specific limits on the tile matrix.
        if tile_matrix_limits:
            min_col = max(min_col, tile_matrix_limits.mintilecol)
            max_col = min(max_col, tile_matrix_limits.maxtilecol)
            min_row = max(min_row, tile_matrix_limits.mintilerow)
            max_row = min(max_row, tile_matrix_limits.maxtilerow)
        return min_col, max_col, min_row, max_row

    def _wmts_images(self, wmts, layer, matrix_set_name, extent,
                     max_pixel_span):
        """
        Add images from the specified WMTS layer and matrix set to cover
        the specified extent at an appropriate resolution.

        The zoom level (aka. tile matrix) is chosen to give the lowest
        possible resolution which still provides the requested quality.
        If insufficient resolution is available, the highest available
        resolution is used.

        Parameters
        ----------
        wmts
            The owslib.wmts.WebMapTileService providing the tiles.
        layer
            The owslib.wmts.ContentMetadata (aka. layer) to draw.
        matrix_set_name
            The name of the matrix set to use.
        extent
            Tuple of (left, right, bottom, top) in Axes coordinates.
        max_pixel_span
            Preferred maximum pixel width or height in Axes coordinates.

        """

        # Find which tile matrix has the appropriate resolution.
        tile_matrix_set = wmts.tilematrixsets[matrix_set_name]
        tile_matrices = tile_matrix_set.tilematrix.values()
        if tile_matrix_set.crs in METERS_PER_UNIT:
            meters_per_unit = METERS_PER_UNIT[tile_matrix_set.crs]
        elif ':EPSG:' in tile_matrix_set.crs:
            epsg_num = tile_matrix_set.crs.split(':')[-1]
            tms_crs = ccrs.epsg(int(epsg_num))
            # catch UserWarning from .to_dict(), because the resulting
            # dictionary does not contain all information for all projections;
            # need only 'units' here
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                crs_dict = tms_crs.to_dict()
                crs_units = crs_dict.get('units', '')
                if crs_units != 'm':
                    raise ValueError('Only unit "m" implemented for'
                                     ' EPSG projections.')
                meters_per_unit = 1
        else:
            raise ValueError(f'Unknown coordinate reference system string:'
                             f' {tile_matrix_set.crs}')
        tile_matrix = self._choose_matrix(tile_matrices, meters_per_unit,
                                          max_pixel_span)

        # Determine which tiles are required to cover the requested extent.
        tile_span_x, tile_span_y = self._tile_span(tile_matrix,
                                                   meters_per_unit)
        tile_matrix_set_links = getattr(layer, 'tilematrixsetlinks', None)
        if tile_matrix_set_links is None:
            tile_matrix_limits = None
        else:
            tile_matrix_set_link = tile_matrix_set_links[matrix_set_name]
            tile_matrix_limits = tile_matrix_set_link.tilematrixlimits.get(
                tile_matrix.identifier)
        min_col, max_col, min_row, max_row = self._select_tiles(
            tile_matrix, tile_matrix_limits, tile_span_x, tile_span_y, extent)

        # Find the relevant section of the image cache.
        tile_matrix_id = tile_matrix.identifier
        cache_by_wmts = WMTSRasterSource._shared_image_cache
        cache_by_layer_matrix = cache_by_wmts.setdefault(wmts, {})
        image_cache = cache_by_layer_matrix.setdefault((layer.id,
                                                        tile_matrix_id), {})

        # To avoid nasty seams between the individual tiles, we
        # accumulate the tile images into a single image.
        big_img = None
        n_rows = 1 + max_row - min_row
        n_cols = 1 + max_col - min_col
        # Ignore out-of-range errors if the current version of OWSLib
        # doesn't provide the regional information.
        ignore_out_of_range = tile_matrix_set_links is None
        for row in range(min_row, max_row + 1):
            for col in range(min_col, max_col + 1):
                # Get the tile's Image from the cache if possible.
                img_key = (row, col)
                img = image_cache.get(img_key)

                if img is None:
                    # Try it from disk cache
                    if self.cache_path is not None:
                        filename = f"{img_key[0]}_{img_key[1]}.npy"
                        cached_file = self._cache_dir / filename
                    else:
                        filename = None
                        cached_file = None

                    if cached_file in self.cache:
                        img = Image.fromarray(np.load(cached_file, allow_pickle=False))
                    else:
                        try:
                            tile = wmts.gettile(
                                layer=layer.id,
                                tilematrixset=matrix_set_name,
                                tilematrix=str(tile_matrix_id),
                                row=str(row), column=str(col),
                                **self.gettile_extra_kwargs)
                        except owslib.util.ServiceException as exception:
                            if ('TileOutOfRange' in exception.message and
                                    ignore_out_of_range):
                                continue
                            raise exception
                        img = Image.open(io.BytesIO(tile.read()))
                        image_cache[img_key] = img
                        # save image to local cache
                        if self.cache_path is not None:
                            np.save(cached_file, img, allow_pickle=False)
                            self.cache.add(filename)

                if big_img is None:
                    size = (img.size[0] * n_cols, img.size[1] * n_rows)
                    big_img = Image.new('RGBA', size, (255, 255, 255, 255))
                top = (row - min_row) * tile_matrix.tileheight
                left = (col - min_col) * tile_matrix.tilewidth
                big_img.paste(img, (left, top))

        if big_img is None:
            img_extent = None
        else:
            matrix_min_x, matrix_max_y = tile_matrix.topleftcorner
            min_img_x = matrix_min_x + tile_span_x * min_col
            max_img_y = matrix_max_y - tile_span_y * min_row
            img_extent = (min_img_x, min_img_x + n_cols * tile_span_x,
                          max_img_y - n_rows * tile_span_y, max_img_y)
        return big_img, img_extent


class WFSGeometrySource:
    """Web Feature Service (WFS) retrieval for Cartopy."""

    def __init__(self, service, features, getfeature_extra_kwargs=None):
        """
        Parameters
        ----------
        service
            The URL of a WFS, or an instance of
            :class:`owslib.wfs.WebFeatureService`.
        features
            The typename(s) of the features from the WFS that
            will be retrieved and made available as geometries.
        getfeature_extra_kwargs: optional
            Extra keyword args to pass to the service's `getfeature` call.
            Defaults to None

        """
        if WebFeatureService is None:
            raise ImportError(_OWSLIB_REQUIRED)

        if isinstance(service, str):
            # host name such as mapserver.gis.umn.edu or
            # agroenvgeo.data.inra.fr from full address
            # http://mapserver.gis.umn.edu/mapserver
            # or https://agroenvgeo.data.inra.fr:443/geoserver/wfs
            self.url = urlparse(service).hostname
            # WebFeatureService of owslib
            service = WebFeatureService(service)
        else:
            self.url = ''

        if isinstance(features, str):
            features = [features]

        if getfeature_extra_kwargs is None:
            getfeature_extra_kwargs = {}

        if not features:
            raise ValueError('One or more features must be specified.')
        for feature in features:
            if feature not in service.contents:
                raise ValueError(
                    f'The {feature!r} feature does not exist in this service.')

        self.service = service
        self.features = features
        self.getfeature_extra_kwargs = getfeature_extra_kwargs

        self._default_urn = None

    def default_projection(self):
        """
        Return a :class:`cartopy.crs.Projection` in which the WFS
        service can supply the requested features.

        """
        # Using first element in crsOptions (default).
        if self._default_urn is None:
            default_urn = {self.service.contents[feature].crsOptions[0] for
                           feature in self.features}
            if len(default_urn) != 1:
                ValueError('Failed to find a single common default SRS '
                           'across all features (typenames).')
            else:
                default_urn = default_urn.pop()

            if (str(default_urn) not in _URN_TO_CRS) and (
                    ":EPSG:" not in str(default_urn)
            ):
                raise ValueError(
                    f"Unknown mapping from SRS/CRS_URN {default_urn!r} to "
                    "cartopy projection.")
            self._default_urn = default_urn

        if str(self._default_urn) in _URN_TO_CRS:
            return _URN_TO_CRS[str(self._default_urn)]
        elif ':EPSG:' in str(self._default_urn):
            epsg_num = str(self._default_urn).split(':')[-1]
            return ccrs.epsg(int(epsg_num))
        else:
            raise ValueError(f'Unknown coordinate reference system:'
                             f' {str(self._default_urn)}')

    def fetch_geometries(self, projection, extent):
        """
        Return any Point, Linestring or LinearRing geometries available
        from the WFS that lie within the specified extent.

        Parameters
        ----------
        projection: :class:`cartopy.crs.Projection`
            The projection in which the extent is specified and in
            which the geometries should be returned. Only the default
            (native) projection is supported.
        extent: four element tuple
            (min_x, max_x, min_y, max_y) tuple defining the geographic extent
            of the geometries to obtain.

        Returns
        -------
        geoms
            A list of Shapely geometries.

        """
        if self.default_projection() != projection:
            raise ValueError('Geometries are only available in projection '
                             f'{self.default_projection()!r}.')

        min_x, max_x, min_y, max_y = extent
        response = self.service.getfeature(typename=self.features,
                                           bbox=(min_x, min_y, max_x, max_y),
                                           **self.getfeature_extra_kwargs)
        geoms_by_srs = self._to_shapely_geoms(response)
        if not geoms_by_srs:
            geoms = []
        elif len(geoms_by_srs) > 1:
            raise ValueError('Unexpected response from the WFS server. The '
                             'geometries are in multiple SRSs, when only one '
                             'was expected.')
        else:
            srs, geoms = list(geoms_by_srs.items())[0]
            # Attempt to verify the SRS associated with the geometries (if any)
            # matches the specified projection.
            if srs is not None:
                if srs in _URN_TO_CRS:
                    geom_proj = _URN_TO_CRS[srs]
                    if geom_proj != projection:
                        raise ValueError(
                            f'The geometries are not in expected projection. '
                            f'Expected {projection!r}, got {geom_proj!r}.')
                elif ':EPSG:' in srs:
                    epsg_num = srs.split(':')[-1]
                    geom_proj = ccrs.epsg(int(epsg_num))
                    if geom_proj != projection:
                        raise ValueError(
                            f'The EPSG geometries are not in expected '
                            f' projection. Expected {projection!r}, '
                            f' got {geom_proj!r}.')
                else:
                    warnings.warn(
                        f'Unable to verify matching projections due to '
                        f'incomplete mappings from SRS identifiers to cartopy '
                        f'projections. The geometries have an SRS of {srs!r}.')
        return geoms

    def _to_shapely_geoms(self, response):
        """
        Convert polygon coordinate strings in WFS response XML to Shapely
        geometries.

        Parameters
        ----------
        response: (file-like object)
            WFS response XML data.

        Returns
        -------
        geoms_by_srs
            A dictionary containing geometries, with key-value pairs of
            the form {srsname: [geoms]}.

        """
        linear_rings_data = []
        linestrings_data = []
        points_data = []
        tree = ElementTree.parse(response)

        # Get geometries from http://mapserver.gis.umn.edu/mapserver
        # and other servers
        for node in tree.iter():
            snode = str(node)
            if ((_MAP_SERVER_NS in snode) or
                (self.url and (self.url in snode)
                 )):
                s1 = snode.split()[1]
                tag = s1[s1.find('}') + 1:-1]
                if ('geom' in tag) or ('Geom' in tag):
                    # Find LinearRing geometries in our msGeometry node.
                    find_str = f'.//{_GML_NS}LinearRing'
                    if self._node_has_child(node, find_str):
                        data = self._find_polygon_coords(node, find_str)
                        linear_rings_data.extend(data)

                    # Find LineString geometries in our msGeometry node.
                    find_str = f'.//{_GML_NS}LineString'
                    if self._node_has_child(node, find_str):
                        data = self._find_polygon_coords(node, find_str)
                        linestrings_data.extend(data)

                    # Find Point geometries in our msGeometry node.
                    find_str = f'.//{_GML_NS}Point'
                    if self._node_has_child(node, find_str):
                        data = self._find_polygon_coords(node, find_str)
                        points_data.extend(data)

        geoms_by_srs = {}
        for srs, x, y in linear_rings_data:
            geoms_by_srs.setdefault(srs, []).append(
                sgeom.LinearRing(zip(x, y)))
        for srs, x, y in linestrings_data:
            geoms_by_srs.setdefault(srs, []).append(
                sgeom.LineString(zip(x, y)))
        for srs, x, y in points_data:
            geoms_by_srs.setdefault(srs, []).append(
                sgeom.Point(zip(x, y)))
        return geoms_by_srs

    def _find_polygon_coords(self, node, find_str):
        """
        Return the x, y coordinate values for all the geometries in
        a given`node`.

        Parameters
        ----------
        node: :class:`xml.etree.ElementTree.Element`
            Node of the parsed XML response.
        find_str: string
            A search string used to match subelements that contain
            the coordinates of interest, for example:
            './/{http://www.opengis.net/gml}LineString'

        Returns
        -------
        data
            A list of (srsName, x_vals, y_vals) tuples.

        """

        data = []
        for polygon in node.findall(find_str):
            feature_srs = polygon.attrib.get('srsName')
            x, y = [], []

            # We can have nodes called `coordinates` or `coord`.
            coordinates_find_str = f'{_GML_NS}coordinates'
            coords_find_str = f'{_GML_NS}coord'

            if self._node_has_child(polygon, coordinates_find_str):
                points = polygon.findtext(coordinates_find_str)
                coords = points.strip().split(' ')
                for coord in coords:
                    x_val, y_val = coord.split(',')
                    x.append(float(x_val))
                    y.append(float(y_val))
            elif self._node_has_child(polygon, coords_find_str):
                for coord in polygon.findall(coords_find_str):
                    x.append(float(coord.findtext(f'{_GML_NS}X')))
                    y.append(float(coord.findtext(f'{_GML_NS}Y')))
            else:
                raise ValueError('Unable to find or parse coordinate values '
                                 'from the XML.')

            data.append((feature_srs, x, y))
        return data

    @staticmethod
    def _node_has_child(node, find_str):
        """
        Return whether `node` contains (at any sub-level), a node with name
        equal to `find_str`.

        """
        element = node.find(find_str)
        return element is not None
