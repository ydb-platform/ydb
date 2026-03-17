# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

"""
This module defines a :class:`Feature` interface, which can be used and
extended to add various "features" to geoaxes using ax.add_feature(), such as
Shapely objects and Natural Earth Imagery.

The default zorder for Cartopy features is defined in defined in
:class:`~cartopy.mpl.feature_artist.FeatureArtist` as 1.5, which puts them
above images and patches, but below lines and text.

"""

from abc import ABCMeta, abstractmethod

import numpy as np
import shapely.geometry as sgeom

import cartopy.crs
import cartopy.io.shapereader as shapereader


COLORS = {'land': np.array((240, 240, 220)) / 256.,
          'land_alt1': np.array((220, 220, 220)) / 256.,
          'water': np.array((152, 183, 226)) / 256.}
"""
A dictionary of colors useful for drawing Features.

The named keys in this dictionary represent the "type" of
feature being plotted.

"""

_NATURAL_EARTH_GEOM_CACHE = {}
"""
Caches a mapping between (name, category, scale) and a tuple of the
resulting geometries.

Provides a significant performance benefit (when combined with object id
caching in GeoAxes.add_geometries) when producing multiple maps of the
same projection.

"""


class Feature(metaclass=ABCMeta):
    """
    Represents a collection of points, lines and polygons with convenience
    methods for common drawing and filtering operations.

    Parameters
    ----------
    crs
        The coordinate reference system of this Feature


    Other Parameters
    ----------------
    **kwargs
        Keyword arguments to be used when drawing this feature.


    .. seealso::

        To add features to the current Matplotlib axes, see
        :func:`GeoAxes <cartopy.mpl.geoaxes.GeoAxes.add_feature>`.

    """

    def __init__(self, crs, **kwargs):
        self._crs = crs
        self._kwargs = dict(kwargs)

    @property
    def crs(self):
        """The cartopy CRS for the geometries in this feature."""
        return self._crs

    @property
    def kwargs(self):
        """
        The read-only dictionary of keyword arguments that are used when
        creating the Matplotlib artists for this feature.

        """
        return dict(self._kwargs)

    @abstractmethod
    def geometries(self):
        """
        Return an iterator of (shapely) geometries for this feature.

        """
        pass

    def intersecting_geometries(self, extent):
        """
        Return an iterator of shapely geometries that intersect with
        the given extent. The extent is assumed to be in the CRS of
        the feature. If extent is None, the method returns all
        geometries for this dataset.

        """
        # shapely 2.0 returns tuple of NaNs instead of None for empty geometry
        # -> check for both
        if extent is not None and not np.isnan(extent[0]):
            extent_geom = sgeom.box(extent[0], extent[2],
                                    extent[1], extent[3])
            return (geom for geom in self.geometries() if
                    geom is not None and extent_geom.intersects(geom))
        else:
            return self.geometries()


class Scaler:
    """
    General object for handling the scale of the geometries used in a Feature.
    """

    def __init__(self, scale):
        self._scale = scale

    @property
    def scale(self):
        return self._scale

    def scale_from_extent(self, extent):
        """
        Given an extent, update the scale.

        Parameters
        ----------
        extent
            The boundaries of the plotted area of a projection. The
            coordinate system of the extent should be constant, and at the
            same scale as the scales argument in the constructor.

        """
        # Note: Implementation does nothing. For subclasses to specialise.
        return self._scale


class AdaptiveScaler(Scaler):
    """
    Automatically select scale of geometries based on extent of plotted axes.
    """

    def __init__(self, default_scale, limits):
        """
        Parameters
        ----------
        default_scale
            Coarsest scale used as default when plot is at maximum extent.

        limits
            Scale-extent pairs at which scale of geometries change. Must be a
            tuple of tuples ordered from coarsest to finest scales. Limit
            values are the upper bounds for their corresponding scale.

        Example
        -------

        >>> s = AdaptiveScaler('coarse',
        ...           (('intermediate', 30), ('fine', 10)))
        >>> s.scale_from_extent([-180, 180, -90, 90])
        'coarse'
        >>> s.scale_from_extent([-5, 6, 45, 56])
        'intermediate'
        >>> s.scale_from_extent([-5, 5, 45, 56])
        'fine'

        """
        super().__init__(default_scale)
        self._default_scale = default_scale
        # Upper limit on extent in degrees.
        self._limits = limits

    def scale_from_extent(self, extent):
        scale = self._default_scale

        if extent is not None:
            width = abs(extent[1] - extent[0])
            height = abs(extent[3] - extent[2])
            min_extent = min(width, height)

            if min_extent != 0:
                for scale_candidate, upper_bound in self._limits:
                    if min_extent <= upper_bound:
                        # It is a valid scale, so track it.
                        scale = scale_candidate
                    else:
                        # This scale is not valid and we can stop looking.
                        # We use the last (valid) scale that we saw.
                        break

        self._scale = scale
        return self._scale


class ShapelyFeature(Feature):
    """
    A class capable of drawing a collection of
    shapely geometries.

    """

    def __init__(self, geometries, crs, **kwargs):
        """
        Parameters
        ----------
        geometries
            A collection of shapely geometries.
        crs
            The cartopy CRS in which the provided geometries are defined.

        Other Parameters
        ----------------
        **kwargs
            Keyword arguments to be used when drawing this feature.

        """
        super().__init__(crs, **kwargs)
        if isinstance(geometries, sgeom.base.BaseGeometry):
            geometries = [geometries]
        self._geoms = tuple(geometries)

    def geometries(self):
        return iter(self._geoms)


class NaturalEarthFeature(Feature):
    """
    A simple interface to Natural Earth shapefiles.

    See https://www.naturalearthdata.com/

    """

    def __init__(self, category, name, scale, **kwargs):
        """
        Parameters
        ----------
        category
            The category of the dataset, i.e. either 'cultural' or 'physical'.
        name
            The name of the dataset, e.g. 'admin_0_boundary_lines_land'.
        scale
            The dataset scale, i.e. one of '10m', '50m', or '110m',
            or Scaler object. Dataset scales correspond to 1:10,000,000,
            1:50,000,000, and 1:110,000,000 respectively.

        Other Parameters
        ----------------
        **kwargs
            Keyword arguments to be used when drawing this feature.

        """
        super().__init__(cartopy.crs.PlateCarree(), **kwargs)
        self.category = category
        self.name = name

        # Cast the given scale to a (constant) Scaler if a string is passed.
        if isinstance(scale, str):
            scale = Scaler(scale)

        self.scaler = scale
        # Make sure this is a valid resolution
        self._validate_scale()

    @property
    def scale(self):
        return self.scaler.scale

    def _validate_scale(self):
        if self.scale not in ('110m', '50m', '10m'):
            raise ValueError(
                f'{self.scale!r} is not a valid Natural Earth scale. '
                'Valid scales are "110m", "50m", and "10m".'
            )

    def geometries(self):
        """
        Returns an iterator of (shapely) geometries for this feature.

        """
        key = (self.name, self.category, self.scale)
        if key not in _NATURAL_EARTH_GEOM_CACHE:
            path = shapereader.natural_earth(resolution=self.scale,
                                             category=self.category,
                                             name=self.name)
            reader = shapereader.Reader(path)
            if reader.crs is not None:
                self._crs = reader.crs
            geometries = tuple(reader.geometries())
            _NATURAL_EARTH_GEOM_CACHE[key] = geometries
        else:
            geometries = _NATURAL_EARTH_GEOM_CACHE[key]

        return iter(geometries)

    def intersecting_geometries(self, extent):
        """
        Returns an iterator of shapely geometries that intersect with
        the given extent.
        The extent is assumed to be in the CRS of the feature.
        If extent is None, the method returns all geometries for this dataset.
        """
        self.scaler.scale_from_extent(extent)
        return super().intersecting_geometries(extent)

    def with_scale(self, new_scale):
        """
        Return a copy of the feature with a new scale.

        Parameters
        ----------
        new_scale
            The new dataset scale, i.e. one of '10m', '50m', or '110m'.
            Corresponding to 1:10,000,000, 1:50,000,000, and 1:110,000,000
            respectively.

        """
        return NaturalEarthFeature(self.category, self.name, new_scale,
                                   **self.kwargs)


class GSHHSFeature(Feature):
    """
    An interface to the GSHHS dataset.

    See https://www.ngdc.noaa.gov/mgg/shorelines/gshhs.html

    Parameters
    ----------
    scale
        The dataset scale. One of 'auto', 'coarse', 'low', 'intermediate',
        'high, or 'full' (default is 'auto').
    levels
        A list of integers 1-6 corresponding to the desired GSHHS feature
        levels to draw (default is [1] which corresponds to coastlines).

    Other Parameters
    ----------------
    **kwargs
        Keyword arguments to be used when drawing the feature. Defaults
        are edgecolor='black' and facecolor='none'.

    """

    _geometries_cache = {}
    """
    A mapping from scale and level to GSHHS shapely geometry::

        {(scale, level): geom}

    This provides a performance boost when plotting in interactive mode or
    instantiating multiple GSHHS artists, by reducing repeated file IO.

    """

    def __init__(self, scale='auto', levels=None, **kwargs):
        super().__init__(cartopy.crs.PlateCarree(), **kwargs)

        if scale not in ('auto', 'a', 'coarse', 'c', 'low', 'l',
                         'intermediate', 'i', 'high', 'h', 'full', 'f'):
            raise ValueError(f"Unknown GSHHS scale {scale!r}.")
        self._scale = scale

        if levels is None:
            levels = [1]
        self._levels = set(levels)
        unknown_levels = self._levels.difference([1, 2, 3, 4, 5, 6])
        if unknown_levels:
            raise ValueError(f"Unknown GSHHS levels {unknown_levels!r}.")

        # Default kwargs
        self._kwargs.setdefault('edgecolor', 'black')
        self._kwargs.setdefault('facecolor', 'none')

    def _scale_from_extent(self, extent):
        """
        Return the appropriate scale (e.g. 'i') for the given extent
        expressed in PlateCarree CRS.

        """
        # Default to coarse scale
        scale = 'c'

        if extent is not None:
            # Upper limit on extent in degrees.
            scale_limits = (('c', 20.0),
                            ('l', 10.0),
                            ('i', 2.0),
                            ('h', 0.5),
                            ('f', 0.1))

            width = abs(extent[1] - extent[0])
            height = abs(extent[3] - extent[2])
            min_extent = min(width, height)
            if min_extent != 0:
                for scale, limit in scale_limits:
                    if min_extent > limit:
                        break

        return scale

    def geometries(self):
        return self.intersecting_geometries(extent=None)

    def intersecting_geometries(self, extent):
        if self._scale == 'auto':
            scale = self._scale_from_extent(extent)
        else:
            scale = self._scale[0]

        if extent is not None:
            extent_geom = sgeom.box(extent[0], extent[2],
                                    extent[1], extent[3])
        for level in self._levels:
            geoms = GSHHSFeature._geometries_cache.get((scale, level))
            if geoms is None:
                # Load GSHHS geometries from appropriate shape file.
                # TODO selective load based on bbox of each geom in file.
                path = shapereader.gshhs(scale, level)
                reader = shapereader.Reader(path)
                if reader.crs is not None:
                    self._crs = reader.crs
                geoms = tuple(reader.geometries())
                GSHHSFeature._geometries_cache[(scale, level)] = geoms
            for geom in geoms:
                if extent is None or extent_geom.intersects(geom):
                    yield geom


class WFSFeature(Feature):
    """
    A class capable of drawing a collection of geometries
    obtained from an OGC Web Feature Service (WFS).

    This feature requires additional dependencies. If installed via pip,
    try ``pip install cartopy[ows]``.
    """

    def __init__(self, wfs, features, **kwargs):
        """
        Parameters
        ----------
        wfs: string or :class:`owslib.wfs.WebFeatureService` instance
            The WebFeatureService instance, or URL of a WFS service, from which
            to retrieve the geometries.
        features: string or list of strings
            The typename(s) of features available from the web service that
            will be retrieved. Somewhat analogous to layers in WMS/WMTS.

        Other Parameters
        ----------------
        **kwargs
            Keyword arguments to be used when drawing this feature.

        """
        try:
            from cartopy.io.ogc_clients import WFSGeometrySource
        except ImportError as e:
            raise ImportError(
                'WFSFeature requires additional dependencies. If installed '
                'via pip, try `pip install cartopy[ows]`.\n') from e

        self.source = WFSGeometrySource(wfs, features)
        crs = self.source.default_projection()
        super().__init__(crs, **kwargs)
        # Default kwargs
        self._kwargs.setdefault('edgecolor', 'black')
        self._kwargs.setdefault('facecolor', 'none')

    def geometries(self):
        min_x, min_y, max_x, max_y = self.crs.boundary.bounds
        geoms = self.source.fetch_geometries(self.crs,
                                             extent=(min_x, max_x,
                                                     min_y, max_y))
        return iter(geoms)

    def intersecting_geometries(self, extent):
        geoms = self.source.fetch_geometries(self.crs, extent)
        return iter(geoms)


auto_scaler = AdaptiveScaler('110m', (('50m', 50), ('10m', 15)))
"""AdaptiveScaler for NaturalEarthFeature. Default scale is '110m'.
'110m' is used when both latitudes and longitudes span more than 50 degrees,
'50m' for 50-15 degrees and '10m' below 15 degrees."""


BORDERS = NaturalEarthFeature(
    'cultural', 'admin_0_boundary_lines_land',
    auto_scaler, edgecolor='black', facecolor='never')
"""Automatically scaled country boundaries."""


STATES = NaturalEarthFeature(
    'cultural', 'admin_1_states_provinces_lakes',
    auto_scaler, edgecolor='black', facecolor='none')
"""Automatically scaled state and province boundaries."""


COASTLINE = NaturalEarthFeature(
    'physical', 'coastline', auto_scaler,
    edgecolor='black', facecolor='never')
"""Automatically scaled coastline, including major islands."""


LAKES = NaturalEarthFeature(
    'physical', 'lakes', auto_scaler,
    edgecolor='none', facecolor=COLORS['water'])
"""Automatically scaled natural and artificial lakes."""


LAND = NaturalEarthFeature(
    'physical', 'land', auto_scaler,
    edgecolor='none', facecolor=COLORS['land'], zorder=-1)
"""Automatically scaled land polygons, including major islands."""


OCEAN = NaturalEarthFeature(
    'physical', 'ocean', auto_scaler,
    edgecolor='none', facecolor=COLORS['water'], zorder=-1)
"""Automatically scaled ocean polygons."""


RIVERS = NaturalEarthFeature(
    'physical', 'rivers_lake_centerlines', auto_scaler,
    edgecolor=COLORS['water'], facecolor='never')
"""Automatically scaled single-line drainages, including lake centerlines."""
