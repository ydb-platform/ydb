# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from unittest import mock
from xml.etree.ElementTree import ParseError

import numpy as np


try:
    from owslib.wfs import WebFeatureService
    from owslib.wms import WebMapService
    from owslib.wmts import ContentMetadata, WebMapTileService
except ImportError:
    WebMapService = None
    ContentMetadata = None
    WebMapTileService = None
import pytest

import cartopy.crs as ccrs
from cartopy.tests.conftest import _HAS_PYKDTREE_OR_SCIPY


if not _HAS_PYKDTREE_OR_SCIPY:
    pytest.skip('pykdtree or scipy is required', allow_module_level=True)


import cartopy.io.ogc_clients as ogc
from cartopy.io.ogc_clients import _OWSLIB_AVAILABLE


RESOLUTION = (30, 30)


@pytest.mark.network
@pytest.mark.xfail(reason='URL no longer valid')
@pytest.mark.skipif(not _OWSLIB_AVAILABLE, reason='OWSLib is unavailable.')
class TestWMSRasterSource:
    URI = 'http://vmap0.tiles.osgeo.org/wms/vmap0'
    layer = 'basic'
    layers = ['basic', 'ocean']
    projection = ccrs.PlateCarree()

    def test_string_service(self):
        source = ogc.WMSRasterSource(self.URI, self.layer)
        from owslib.map.wms111 import WebMapService_1_1_1
        from owslib.map.wms130 import WebMapService_1_3_0
        assert isinstance(source.service,
                          (WebMapService_1_1_1, WebMapService_1_3_0))
        assert isinstance(source.layers, list)
        assert source.layers == [self.layer]

    def test_wms_service_instance(self):
        service = WebMapService(self.URI)
        source = ogc.WMSRasterSource(service, self.layer)
        assert source.service is service

    def test_multiple_layers(self):
        source = ogc.WMSRasterSource(self.URI, self.layers)
        assert source.layers == self.layers

    def test_no_layers(self):
        match = r'One or more layers must be defined\.'
        with pytest.raises(ValueError, match=match):
            ogc.WMSRasterSource(self.URI, [])

    def test_extra_kwargs_empty(self):
        source = ogc.WMSRasterSource(self.URI, self.layer,
                                     getmap_extra_kwargs={})
        assert source.getmap_extra_kwargs == {}

    def test_extra_kwargs_None(self):
        source = ogc.WMSRasterSource(self.URI, self.layer,
                                     getmap_extra_kwargs=None)
        assert source.getmap_extra_kwargs == {'transparent': True}

    def test_extra_kwargs_non_empty(self):
        kwargs = {'another': 'kwarg'}
        source = ogc.WMSRasterSource(self.URI, self.layer,
                                     getmap_extra_kwargs=kwargs)
        assert source.getmap_extra_kwargs == kwargs

    def test_supported_projection(self):
        source = ogc.WMSRasterSource(self.URI, self.layer)
        source.validate_projection(self.projection)

    def test_unsupported_projection(self):
        source = ogc.WMSRasterSource(self.URI, self.layer)
        # Patch dict of known Proj->SRS mappings so that it does
        # not include any of the available SRSs from the WMS.
        with mock.patch.dict('cartopy.io.ogc_clients._CRS_TO_OGC_SRS',
                             {ccrs.OSNI(approx=True): 'EPSG:29901'},
                             clear=True):
            msg = 'not available'
            with pytest.raises(ValueError, match=msg):
                source.validate_projection(ccrs.Miller())

    def test_fetch_img(self):
        source = ogc.WMSRasterSource(self.URI, self.layer)
        extent = [-10, 10, 40, 60]
        located_image, = source.fetch_raster(self.projection, extent,
                                             RESOLUTION)
        img = np.array(located_image.image)
        assert img.shape == RESOLUTION + (4,)
        # No transparency in this image.
        assert img[:, :, 3].min() == 255
        assert extent == located_image.extent

    def test_fetch_img_different_projection(self):
        source = ogc.WMSRasterSource(self.URI, self.layer)
        extent = [-570000, 5100000, 870000, 3500000]
        located_image, = source.fetch_raster(ccrs.Orthographic(), extent,
                                             RESOLUTION)
        img = np.array(located_image.image)
        assert img.shape == RESOLUTION + (4,)

    def test_multi_image_result(self):
        source = ogc.WMSRasterSource(self.URI, self.layer)
        crs = ccrs.PlateCarree(central_longitude=180)
        extent = [-15, 25, 45, 85]
        located_images = source.fetch_raster(crs, extent, RESOLUTION)
        assert len(located_images) == 2

    def test_float_resolution(self):
        # The resolution (in pixels) should be cast to ints.
        source = ogc.WMSRasterSource(self.URI, self.layer)
        extent = [-570000, 5100000, 870000, 3500000]
        located_image, = source.fetch_raster(self.projection, extent,
                                             [19.5, 39.1])
        img = np.array(located_image.image)
        assert img.shape == (40, 20, 4)


@pytest.mark.filterwarnings("ignore:TileMatrixLimits")
@pytest.mark.network
@pytest.mark.skipif(not _OWSLIB_AVAILABLE, reason='OWSLib is unavailable.')
@pytest.mark.xfail(reason='NASA servers are returning bad content metadata')
class TestWMTSRasterSource:
    URI = 'https://map1c.vis.earthdata.nasa.gov/wmts-geo/wmts.cgi'
    layer_name = 'VIIRS_CityLights_2012'
    projection = ccrs.PlateCarree()

    def test_string_service(self):
        source = ogc.WMTSRasterSource(self.URI, self.layer_name)
        assert isinstance(source.wmts, WebMapTileService)
        assert isinstance(source.layer, ContentMetadata)
        assert source.layer.name == self.layer_name

    def test_wmts_service_instance(self):
        service = WebMapTileService(self.URI)
        source = ogc.WMTSRasterSource(service, self.layer_name)
        assert source.wmts is service

    def test_native_projection(self):
        source = ogc.WMTSRasterSource(self.URI, self.layer_name)
        source.validate_projection(self.projection)

    def test_non_native_projection(self):
        source = ogc.WMTSRasterSource(self.URI, self.layer_name)
        source.validate_projection(ccrs.Miller())

    def test_unsupported_projection(self):
        source = ogc.WMTSRasterSource(self.URI, self.layer_name)
        with mock.patch('cartopy.io.ogc_clients._URN_TO_CRS', {}):
            match = r'Unable to find tile matrix for projection\.'
            with pytest.raises(ValueError, match=match):
                source.validate_projection(ccrs.Miller())

    def test_fetch_img(self):
        source = ogc.WMTSRasterSource(self.URI, self.layer_name)
        extent = [-10, 10, 40, 60]
        located_image, = source.fetch_raster(self.projection, extent,
                                             RESOLUTION)
        img = np.array(located_image.image)
        assert img.shape == (512, 512, 4)
        # No transparency in this image.
        assert img[:, :, 3].min() == 255
        assert located_image.extent == (-180.0, 107.99999999999994,
                                        -197.99999999999994, 90.0)

    def test_fetch_img_reprojected(self):
        source = ogc.WMTSRasterSource(self.URI, self.layer_name)
        extent = [-20, -1, 48, 50]
        # NB single result in this case.
        located_image, = source.fetch_raster(ccrs.NorthPolarStereo(), extent,
                                             (30, 30))

        # Check image array is as expected (more or less).
        img = np.array(located_image.image)
        assert img.shape == (42, 42, 4)
        # When reprojected, extent is exactly what you asked for.
        assert located_image.extent == extent

    def test_fetch_img_reprojected_twoparts(self):
        source = ogc.WMTSRasterSource(self.URI, self.layer_name)
        extent = [-10, 12, 48, 50]
        images = source.fetch_raster(ccrs.NorthPolarStereo(), extent, (30, 30))

        # Check for 2 results in this case.
        assert len(images) == 2
        im1, im2 = images
        # Check image arrays is as expected (more or less).
        assert np.array(im1.image).shape == (42, 42, 4)
        assert np.array(im2.image).shape == (42, 42, 4)
        # When reprojected, extent is exactly what you asked for.
        assert im1.extent == extent
        assert im2.extent == extent


@pytest.mark.network
@pytest.mark.skipif(not _OWSLIB_AVAILABLE, reason='OWSLib is unavailable.')
class TestWFSGeometrySource:
    URI = 'https://nsidc.org/cgi-bin/atlas_south?service=WFS'
    typename = 'land_excluding_antarctica'
    native_projection = ccrs.Stereographic(central_latitude=-90,
                                           true_scale_latitude=-71)

    def test_string_service(self):
        service = WebFeatureService(self.URI)
        source = ogc.WFSGeometrySource(self.URI, self.typename)
        assert isinstance(source.service, type(service))
        assert source.features == [self.typename]

    def test_wfs_service_instance(self):
        service = WebFeatureService(self.URI)
        source = ogc.WFSGeometrySource(service, self.typename)
        assert source.service is service
        assert source.features == [self.typename]

    def test_default_projection(self):
        source = ogc.WFSGeometrySource(self.URI, self.typename)
        assert source.default_projection() == self.native_projection

    def test_unsupported_projection(self):
        source = ogc.WFSGeometrySource(self.URI, self.typename)
        msg = 'Geometries are only available in projection'
        with pytest.raises(ValueError, match=msg):
            source.fetch_geometries(ccrs.PlateCarree(), [-180, 180, -90, 90])

    @pytest.mark.xfail(raises=ParseError,
                       reason="Bad XML returned from the URL")
    def test_fetch_geometries(self):
        source = ogc.WFSGeometrySource(self.URI, self.typename)
        # Extent covering New Zealand.
        extent = (-99012, 1523166, -6740315, -4589165)
        geoms = source.fetch_geometries(self.native_projection, extent)
        assert len(geoms) == 23
