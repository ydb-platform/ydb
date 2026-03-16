from types import FunctionType

import numpy as np
import param

from ..core.dimension import Dimension
from ..core.element import Element2D
from ..util.transform import easting_northing_to_lon_lat, lon_lat_to_easting_northing


class Tiles(Element2D):
    """The Tiles element represents tile sources, specified as URL
    containing different template variables or xyzservices.TileProvider.
    These variables correspond to three different formats for specifying the spatial
    location and zoom level of the requested tiles:

      * Web mapping tiles sources containing {x}, {y}, and {z} variables

      * Bounding box tile sources containing {XMIN}, {XMAX}, {YMIN}, {YMAX} variables

      * Quadkey tile sources containing a {Q} variable

    Tiles are defined in a pseudo-Mercator projection (EPSG:3857)
    defined as eastings and northings. Any data overlaid on a tile
    source therefore has to be defined in those coordinates or be
    projected (e.g. using GeoViews).

    """

    kdims = param.List(default=[Dimension('x'), Dimension('y')],
                       bounds=(2, 2), constant=True, doc="""
        The key dimensions of a geometry represent the x- and y-
        coordinates in a 2D space.""")

    group = param.String(default='Tiles', constant=True)

    def __init__(self, data=None, kdims=None, vdims=None, **params):
        from bokeh.models import MercatorTileSource
        if isinstance(data, MercatorTileSource):
            data = data.url
        elif data is not None and not isinstance(data, (str, dict)):
            raise TypeError(f'{type(self).__name__} data should be a tile service URL or '
                            f'xyzservices.TileProvider not a {type(data).__name__} type.' )
        super().__init__(data, kdims=kdims, vdims=vdims, **params)

    def range(self, dim, data_range=True, dimension_range=True):
        return np.nan, np.nan

    def dimension_values(self, dimension, expanded=True, flat=True):
        return np.array([])

    @staticmethod
    def lon_lat_to_easting_northing(longitude, latitude):
        """Projects the given longitude, latitude values into Web Mercator
        (aka Pseudo-Mercator or EPSG:3857) coordinates.

        See docstring for holoviews.util.transform.lon_lat_to_easting_northing
        for more information

        """
        return lon_lat_to_easting_northing(longitude, latitude)

    @staticmethod
    def easting_northing_to_lon_lat(easting, northing):
        """Projects the given easting, northing values into
        longitude, latitude coordinates.

        See docstring for holoviews.util.transform.easting_northing_to_lon_lat
        for more information

        """
        return easting_northing_to_lon_lat(easting, northing)


# Mapping between patterns to match specified as tuples and tuples containing attributions
_ATTRIBUTIONS = {
    ('openstreetmap',) : (
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    ),
    ('cartodb',) : (
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, '
        '&copy; <a href="https://cartodb.com/attributions">CartoDB</a>'
    ),
    ('cartocdn',) : (
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, '
        '&copy; <a href="https://cartodb.com/attributions">CartoDB</a>'
    ),
    ('stamen', 'png') : ( # to match both 'toner' and 'terrain'
        'Map tiles by <a href="https://stamen.com">Stamen Design</a> / <a href="https://stadiamaps.com">Stadia Maps</a>, '
        'under <a href="https://creativecommons.org/licenses/by/3.0">CC BY 3.0</a>. '
        'Data by <a href="https://openstreetmap.org">OpenStreetMap</a>, '
        'under <a href="https://www.openstreetmap.org/copyright">ODbL</a>.'
    ),
    ('stamen', 'jpg') : (  # watercolor
        'Map tiles by <a href="https://stamen.com">Stamen Design</a> / <a href="https://stadiamaps.com">Stadia Maps</a>, '
        'under <a href="https://creativecommons.org/licenses/by/3.0">CC BY 3.0</a>. '
        'Data by <a href="https://openstreetmap.org">OpenStreetMap</a>, '
        'under <a href="https://creativecommons.org/licenses/by-sa/3.0">CC BY SA</a>.'
    ),
    ('wikimedia',) : (
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    ),
    ('arcgis','Terrain') : (
        '&copy; <a href="http://downloads.esri.com/ArcGISOnline/docs/tou_summary.pdf">Esri</a>, '
        'USGS, NOAA'
    ),
    ('arcgis','Reference') : (
        '&copy; <a href="http://downloads.esri.com/ArcGISOnline/docs/tou_summary.pdf">Esri</a>, '
        'Garmin, USGS, NPS'
    ),
    ('arcgis','Imagery') : (
        '&copy; <a href="http://downloads.esri.com/ArcGISOnline/docs/tou_summary.pdf">Esri</a>, '
        'Earthstar Geographics'
    ),
    ('arcgis','NatGeo') : (
        '&copy; <a href="http://downloads.esri.com/ArcGISOnline/docs/tou_summary.pdf">Esri</a>, '
        'NatGeo, Garmin, HERE, UNEP-WCMC, USGS, NASA, ESA, METI, NRCAN, GEBCO, NOAA, Increment P'
    ),
    ('arcgis','USA_Topo') : (
        '&copy; <a href="http://downloads.esri.com/ArcGISOnline/docs/tou_summary.pdf">Esri</a>, '
        'NatGeo, i-cubed'
    ),
    ('arcgis', 'World_Street_Map') : (
        '&copy; Esri &mdash; Source: Esri, DeLorme, NAVTEQ, USGS, Intermap, iPC, NRCAN, Esri Japan, METI, Esri China (Hong Kong), Esri (Thailand), TomTom, 2012'
    )
}


# CartoDB basemaps
CartoDark = lambda: Tiles('https://cartodb-basemaps-4.global.ssl.fastly.net/dark_all/{Z}/{X}/{Y}.png', name="CartoDark")
CartoLight = lambda: Tiles('https://cartodb-basemaps-4.global.ssl.fastly.net/light_all/{Z}/{X}/{Y}.png', name="CartoLight")


# Stamen basemaps
StamenTerrain = lambda: Tiles('https://tiles.stadiamaps.com/tiles/stamen_terrain/{Z}/{X}/{Y}.png', name="StamenTerrain")
StamenTerrainRetina = lambda: Tiles('https://tiles.stadiamaps.com/tiles/stamen_terrain/{Z}/{X}/{Y}@2x.png', name="StamenTerrainRetina")
StamenWatercolor = lambda: Tiles('https://tiles.stadiamaps.com/tiles/stamen_watercolor/{Z}/{X}/{Y}.jpg', name="StamenWatercolor")
StamenToner = lambda: Tiles('https://tiles.stadiamaps.com/tiles/stamen_toner/{Z}/{X}/{Y}.png', name="StamenToner")
StamenTonerRetina = lambda: Tiles('https://tiles.stadiamaps.com/tiles/stamen_toner/{Z}/{X}/{Y}@2x.png', name="StamenTonerRetina")
StamenTonerBackground = lambda: Tiles('https://tiles.stadiamaps.com/tiles/stamen_toner_background/{Z}/{X}/{Y}.png', name="StamenTonerBackground")
StamenTonerBackgroundRetina = lambda: Tiles('https://tiles.stadiamaps.com/tiles/stamen_toner_background/{Z}/{X}/{Y}@2x.png', name="StamenTonerBackgroundRetina")
StamenLabels = lambda: Tiles('https://tiles.stadiamaps.com/tiles/stamen_toner_labels/{Z}/{X}/{Y}.png', name="StamenLabels")
StamenLabelsRetina = lambda: Tiles('https://tiles.stadiamaps.com/tiles/stamen_toner_labels/{Z}/{X}/{Y}@2x.png', name="StamenLabelsRetina")

# Esri maps (see https://server.arcgisonline.com/arcgis/rest/services for the full list)
EsriImagery = lambda: Tiles('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{Z}/{Y}/{X}.jpg', name="EsriImagery")
EsriNatGeo = lambda: Tiles('https://server.arcgisonline.com/ArcGIS/rest/services/NatGeo_World_Map/MapServer/tile/{Z}/{Y}/{X}', name="EsriNatGeo")
EsriUSATopo = lambda: Tiles('https://server.arcgisonline.com/ArcGIS/rest/services/USA_Topo_Maps/MapServer/tile/{Z}/{Y}/{X}', name="EsriUSATopo")
EsriTerrain = lambda: Tiles('https://server.arcgisonline.com/ArcGIS/rest/services/World_Terrain_Base/MapServer/tile/{Z}/{Y}/{X}', name="EsriTerrain")
EsriStreet = lambda: Tiles('https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{Z}/{Y}/{X}')
EsriReference = lambda: Tiles('https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Reference_Overlay/MapServer/tile/{Z}/{Y}/{X}', name="EsriReference")
ESRI = EsriImagery # For backwards compatibility with gv 1.5


# Miscellaneous
OSM = lambda: Tiles('https://c.tile.openstreetmap.org/{Z}/{X}/{Y}.png', name="OSM")
OpenTopoMap = lambda: Tiles('https://a.tile.opentopomap.org/{Z}/{X}/{Y}.png', name="OpenTopoMap")

_all_tile_sources = {k: v for k, v in locals().items() if isinstance(v, FunctionType) and k not in
                ['ESRI', 'lon_lat_to_easting_northing', 'easting_northing_to_lon_lat',
                 'deprecation_warning', 'wikimedia_replacement']}

tile_sources = {k: v for k, v in _all_tile_sources.items() if "Stamen" not in k}
stamen_sources = {k: v for k, v in _all_tile_sources.items() if "Stamen" in k}
