# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

"""
This module defines the :class:`FeatureArtist` class, for drawing
:class:`Feature` instances through an extension of the Matplotlib Artist interfaces.

"""

import warnings
import weakref

import matplotlib.artist
import matplotlib.collections
import numpy as np

import cartopy.feature as cfeature
from cartopy.mpl import _MPL_38
import cartopy.mpl.path as cpath


class _GeomKey:
    """
    Provide id() based equality and hashing for geometries.

    Instances of this class must be treated as immutable for the caching
    to operate correctly.

    A workaround for Shapely polygons no longer being hashable as of 1.5.13.

    """

    def __init__(self, geom):
        self._id = id(geom)

    def __eq__(self, other):
        return self._id == other._id

    def __hash__(self):
        return hash(self._id)


def _freeze(obj):
    """
    Recursively freeze the given object so that it might be suitable for
    use as a hashable.

    """
    if isinstance(obj, dict):
        obj = frozenset(((k, _freeze(v)) for k, v in obj.items()))
    elif isinstance(obj, list):
        obj = tuple(_freeze(item) for item in obj)
    elif isinstance(obj, np.ndarray):
        obj = tuple(obj)
    return obj


class FeatureArtist(matplotlib.collections.Collection):
    """
    A subclass of :class:`~matplotlib.collections.Collection` capable of
    drawing a :class:`cartopy.feature.Feature`.

    """

    _geom_key_to_geometry_cache = weakref.WeakValueDictionary()
    """
    A mapping from _GeomKey to geometry to assist with the caching of
    transformed Matplotlib paths.

    """
    _geom_key_to_path_cache = weakref.WeakKeyDictionary()
    """
    A nested mapping from geometry (converted to a _GeomKey) and target
    projection to the resulting transformed Matplotlib paths::

        {geom: {target_projection: list_of_paths}}

    This provides a significant boost when producing multiple maps of the
    same projection.

    """

    def __init__(self, feature, **kwargs):
        """
        Parameters
        ----------
        feature
            An instance of :class:`cartopy.feature.Feature` to draw.
        styler
            A callable that given a geometry, returns matplotlib styling
            parameters.

        Other Parameters
        ----------------
        **kwargs
            Keyword arguments to be used when drawing the feature. These
            will override those shared with the feature.

        """
        super().__init__()

        self._styler = kwargs.pop('styler', None)
        self._kwargs = dict(kwargs)

        if 'color' in self._kwargs:
            # We want the user to be able to override both face and edge
            # colours if the original feature already supplied it.
            color = self._kwargs.pop('color')
            self._kwargs['facecolor'] = self._kwargs['edgecolor'] = color

        # Paths are worked out at draw, but add_collection fails if paths is
        # left to the default of None.
        self.set_paths([])

        # Set default zorder so that features are drawn under
        # lines e.g. contours but over images and filled patches.
        # Note that the zorder of Patch, PatchCollection and PathCollection
        # are all 1 by default. Assuming default zorder, drawing takes place in
        # the following order: collections, patches, FeatureArtist, lines,
        # text.
        self.set_zorder(1.5)

        # Update drawing styles from the feature and **kwargs.
        self.set(**feature.kwargs)
        self.set(**self._kwargs)

        self._feature = feature

    def set_facecolor(self, c):
        """
        Set the facecolor(s) of the `.FeatureArtist`.  If set to 'never' then
        subsequent calls will have no effect.  Otherwise works the same as
        `matplotlib.collections.Collection.set_facecolor`.
        """
        if isinstance(c, str) and c == 'never':
            self._never_fc = True
            super().set_facecolor('none')

        elif (getattr(self, '_never_fc', False) and
                (not isinstance(c, str) or c != 'none')):
            warnings.warn('facecolor will have no effect as it has been '
                          'defined as "never".')
        else:
            super().set_facecolor(c)

    if not _MPL_38:
        # set_paths does not yet exist on Collection.
        def set_paths(self, paths):
            self._paths = paths

    @matplotlib.artist.allow_rasterization
    def draw(self, renderer):
        """
        Draw the geometries of the feature that intersect with the extent of
        the :class:`cartopy.mpl.geoaxes.GeoAxes` instance to which this
        object has been added.

        """
        if not self.get_visible():
            return

        ax = self.axes
        feature_crs = self._feature.crs

        # Get geometries that we need to draw.
        extent = None
        try:
            extent = ax.get_extent(feature_crs)
        except ValueError:
            warnings.warn('Unable to determine extent. Defaulting to global.')

        if isinstance(self._feature, cfeature.ShapelyFeature):
            # User passed a specific list of geometries.  If they also passed
            # `array` or a list of facecolors then we should keep the colours
            # consistent after pan/zoom.  Do this by creating a Path for every
            # geometry regardless of whether they are currently in view.
            geoms = self._feature.geometries()
        else:
            # For efficiency on local maps with high resolution features (e.g
            # from Natural Earth), only create paths for geometries that are
            # in view.
            geoms = self._feature.intersecting_geometries(extent)

        stylised_paths = {}
        # Make an empty placeholder style dictionary for when styler is not
        # used.  Freeze it so that we can use it as a dict key.  We will need
        # to unfreeze all style dicts with dict(frozen) before passing to mpl.
        no_style = _freeze({})

        # Project (if necessary) and convert geometries to matplotlib paths.
        key = ax.projection
        for geom in geoms:
            # As Shapely geometries cannot be relied upon to be
            # hashable, we have to use a WeakValueDictionary to manage
            # their weak references. The key can then be a simple,
            # "disposable", hashable geom-key object that just uses the
            # id() of a geometry to determine equality and hash value.
            # The only persistent, strong reference to the geom-key is
            # in the WeakValueDictionary, so when the geometry is
            # garbage collected so is the geom-key.
            # The geom-key is also used to access the WeakKeyDictionary
            # cache of transformed geometries. So when the geom-key is
            # garbage collected so are the transformed geometries.
            geom_key = _GeomKey(geom)
            FeatureArtist._geom_key_to_geometry_cache.setdefault(
                geom_key, geom)
            mapping = FeatureArtist._geom_key_to_path_cache.setdefault(
                geom_key, {})
            geom_path = mapping.get(key)
            if geom_path is None:
                if ax.projection != feature_crs:
                    projected_geom = ax.projection.project_geometry(
                        geom, feature_crs)
                else:
                    projected_geom = geom

                geom_path = cpath.shapely_to_path(projected_geom)
                mapping[key] = geom_path

            if self._styler is None:
                stylised_paths.setdefault(no_style, []).append(geom_path)
            else:
                style = _freeze(self._styler(geom))
                stylised_paths.setdefault(style, []).append(geom_path)

        self.set_clip_path(ax.patch)

        # Draw each style individually.  Note that there will only be multiple
        # styles if styler was used.
        for style, paths in stylised_paths.items():
            style = dict(style)

            # Temporarily replace properties.
            orig_style = {k: getattr(self, f"get_{k}")() for k in style}
            self.set(paths=paths, **style)

            super().draw(renderer)

            self.set(paths=[], **orig_style)
