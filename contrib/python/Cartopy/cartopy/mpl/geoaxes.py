# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

"""
This module defines the :class:`cartopy.mpl.geoaxes.GeoAxes` class, an extension of
matplotlib which adds a `transform` keyword argument to many plotting methods to enable
geographic projections and boundary wrapping to occur on the axes.

When a Matplotlib figure contains a GeoAxes the plotting commands can transform
plot results from source coordinates to the GeoAxes' target projection.

"""

import collections
import contextlib
import functools
import json
import os
from pathlib import Path
import warnings
import weakref

import matplotlib as mpl
import matplotlib.artist
import matplotlib.axes
import matplotlib.contour
from matplotlib.image import imread
import matplotlib.patches as mpatches
import matplotlib.path as mpath
import matplotlib.spines as mspines
import matplotlib.transforms as mtransforms
import numpy as np
import numpy.ma as ma
import shapely.geometry as sgeom

from cartopy import config
import cartopy.crs as ccrs
import cartopy.feature
from cartopy.mpl import _MPL_38
import cartopy.mpl.contour
import cartopy.mpl.feature_artist as feature_artist
import cartopy.mpl.geocollection
import cartopy.mpl.path as cpath
from cartopy.mpl.slippy_image_artist import SlippyImageArtist


# A nested mapping from path, source CRS, and target projection to the
# resulting transformed paths:
#     {path: {(source_crs, target_projection): list_of_paths}}
# Provides a significant performance boost for contours which, at
# matplotlib 1.2.0 called transform_path_non_affine twice unnecessarily.
_PATH_TRANSFORM_CACHE = weakref.WeakKeyDictionary()

# A dictionary of pre-loaded images for large background images, kept as a
# dictionary so that large images are loaded only once.
_BACKG_IMG_CACHE = {}

# A dictionary of background images in the directory specified by the
# CARTOPY_USER_BACKGROUNDS environment variable.
_USER_BG_IMGS = {}

# XXX call this InterCRSTransform
class InterProjectionTransform(mtransforms.Transform):
    """
    Transform coordinates from the source_projection to
    the ``target_projection``.

    """
    input_dims = 2
    output_dims = 2
    is_separable = False
    has_inverse = True

    def __init__(self, source_projection, target_projection):
        """
        Create the transform object from the given projections.

        Parameters
        ----------
        source_projection
            A :class:`~cartopy.crs.CRS`.
        target_projection
            A :class:`~cartopy.crs.CRS`.

        """
        # assert target_projection is cartopy.crs.Projection
        # assert source_projection is cartopy.crs.CRS
        self.source_projection = source_projection
        self.target_projection = target_projection
        mtransforms.Transform.__init__(self)

    def __repr__(self):
        return (f'< {self.__class__.__name__!s} {self.source_projection!s} '
                f'-> {self.target_projection!s} >')

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            result = NotImplemented
        else:
            result = (self.source_projection == other.source_projection and
                      self.target_projection == other.target_projection)
        return result

    def __ne__(self, other):
        return not self == other

    def transform_non_affine(self, xy):
        """
        Transform from source to target coordinates.

        Parameters
        ----------
        xy
            An (n,2) array of points in source coordinates.

        Returns
        -------
        x, y
            An (n,2) array of transformed points in target coordinates.

        """
        prj = self.target_projection
        if isinstance(xy, np.ndarray):
            return prj.transform_points(self.source_projection,
                                        xy[:, 0], xy[:, 1])[:, 0:2]
        else:
            x, y = xy
            x, y = prj.transform_point(x, y, self.source_projection)
            return x, y

    def transform_path_non_affine(self, src_path):
        """
        Transform from source to target coordinates.

        Cache results, so subsequent calls with the same *src_path* argument
        (and the same source and target projections) are faster.

        Parameters
        ----------
        src_path
            A Matplotlib :class:`~matplotlib.path.Path` object
            with vertices in source coordinates.

        Returns
        -------
        result
            A Matplotlib :class:`~matplotlib.path.Path` with vertices
            in target coordinates.

        """
        mapping = _PATH_TRANSFORM_CACHE.get(src_path)
        if mapping is not None:
            key = (self.source_projection, self.target_projection)
            result = mapping.get(key)
            if result is not None:
                return result

        # Allow the vertices to be quickly transformed, if
        # quick_vertices_transform allows it.
        new_vertices = self.target_projection.quick_vertices_transform(
            src_path.vertices, self.source_projection)
        if new_vertices is not None:
            if new_vertices is src_path.vertices:
                return src_path
            else:
                return mpath.Path(new_vertices, src_path.codes)

        if src_path.vertices.shape == (1, 2):
            return mpath.Path(self.transform(src_path.vertices))

        geom = cpath.path_to_shapely(src_path)
        transformed_geom = self.target_projection.project_geometry(
            geom, self.source_projection)

        result = cpath.shapely_to_path(transformed_geom)

        # store the result in the cache for future performance boosts
        key = (self.source_projection, self.target_projection)
        if mapping is None:
            _PATH_TRANSFORM_CACHE[src_path] = {key: result}
        else:
            mapping[key] = result

        return result

    def inverted(self):
        """
        Returns
        -------
        InterProjectionTransform
            A Matplotlib :class:`~matplotlib.transforms.Transform`
            from target to source coordinates.

        """
        return InterProjectionTransform(self.target_projection,
                                        self.source_projection)


class _ViewClippedPathPatch(mpatches.PathPatch):
    def __init__(self, axes, **kwargs):
        self._original_path = mpath.Path(np.empty((0, 2)))
        super().__init__(self._original_path, **kwargs)
        self._axes = axes

        # We need to use a TransformWrapper as our transform so that we can
        # update the transform without breaking others' references to this one.
        self._trans_wrap = mtransforms.TransformWrapper(self.get_transform())

    def set_transform(self, transform):
        self._trans_wrap.set(transform)
        super().set_transform(self._trans_wrap)

    def set_boundary(self, path, transform):
        self._original_path = cpath._ensure_path_closed(path)
        self.set_transform(transform)
        self.stale = True

    def _adjust_location(self):
        if self.stale:
            self.set_path(
                cpath._ensure_path_closed(
                    self._original_path.clip_to_bbox(self.axes.viewLim)))
            # Some places in matplotlib's transform stack cache the actual
            # path so we trigger an update by invalidating the transform.
            self._trans_wrap.invalidate()

    @matplotlib.artist.allow_rasterization
    def draw(self, renderer, *args, **kwargs):
        self._adjust_location()
        super().draw(renderer, *args, **kwargs)


class GeoSpine(mspines.Spine):
    def __init__(self, axes, **kwargs):
        self._original_path = mpath.Path(np.empty((0, 2)))
        kwargs.setdefault('clip_on', False)
        super().__init__(axes, 'geo', self._original_path, **kwargs)

    def set_boundary(self, path, transform):
        # Make sure path is closed (required by "Path.clip_to_bbox")
        self._original_path = cpath._ensure_path_closed(path)
        self.set_transform(transform)
        self.stale = True

    def _adjust_location(self):
        if self.stale:
            self._path = cpath._ensure_path_closed(
                self._original_path.clip_to_bbox(self.axes.viewLim)
                )

    def get_window_extent(self, renderer=None):
        # make sure the location is updated so that transforms etc are
        # correct:
        self._adjust_location()
        return super().get_window_extent(renderer=renderer)

    @matplotlib.artist.allow_rasterization
    def draw(self, renderer):
        self._adjust_location()
        ret = super().draw(renderer)
        self.stale = False
        return ret

    def set_position(self, position):
        """GeoSpine does not support changing its position."""
        raise NotImplementedError(
            'GeoSpine does not support changing its position.')


def _add_transform(func):
    """A decorator that adds and validates the transform keyword argument."""
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        transform = kwargs.get('transform', None)
        if transform is None:
            transform = self.projection
        # Raise an error if any of these functions try to use
        # a spherical source CRS.
        non_spherical_funcs = ['contour', 'contourf', 'pcolormesh', 'pcolor',
                               'quiver', 'barbs', 'streamplot']
        if (func.__name__ in non_spherical_funcs and
                isinstance(transform, ccrs.CRS) and
                not isinstance(transform, ccrs.Projection)):
            raise ValueError(f'Invalid transform: Spherical {func.__name__} '
                             'is not supported - consider using '
                             'PlateCarree/RotatedPole.')

        kwargs['transform'] = transform
        return func(self, *args, **kwargs)
    return wrapper


def _add_transform_first(func):
    """
    A decorator that adds and validates the transform_first keyword argument.

    This handles a fast-path optimization that projects the points before
    creating any patches or lines. This means that the lines/patches will be
    calculated in projected-space, not data-space. It requires the first
    three arguments to be x, y, and z and all must be two-dimensional to use
    the fast-path option.

    This should be added after the _add_transform wrapper so that a transform
    is guaranteed to be present.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if kwargs.pop('transform_first', False):
            if len(args) < 3:
                # For the fast-path we need X and Y input points
                raise ValueError("The X and Y arguments must be provided to "
                                 "use the transform_first=True fast-path.")
            x, y, z = (np.array(i) for i in args[:3])
            if not (x.ndim == y.ndim == 2):
                raise ValueError("The X and Y arguments must be gridded "
                                 "2-dimensional arrays")

            # Remove the transform from the keyword arguments
            t = kwargs.pop('transform')
            # Transform all of the x and y points
            pts = self.projection.transform_points(t, x, y)
            x = pts[..., 0].reshape(x.shape)
            y = pts[..., 1].reshape(y.shape)
            # The x coordinates could be wrapped, but matplotlib expects
            # them to be sorted, so we will reorganize the arrays based on x
            ind = np.argsort(x, axis=1)
            x = np.take_along_axis(x, ind, axis=1)
            y = np.take_along_axis(y, ind, axis=1)
            z = np.take_along_axis(z, ind, axis=1)

            # Use the new points as the input arguments
            args = (x, y, z) + args[3:]
        return func(self, *args, **kwargs)
    return wrapper


class GeoAxes(matplotlib.axes.Axes):
    """
    A subclass of :class:`matplotlib.axes.Axes` which represents a
    map :class:`~cartopy.crs.Projection`.

    This class replaces the Matplotlib :class:`~matplotlib.axes.Axes` class
    when created with the *projection* keyword. For example::

        # Set up a standard map for latlon data.
        geo_axes = plt.axes(projection=cartopy.crs.PlateCarree())

        # Set up a standard map for latlon data for multiple subplots
        fig, geo_axes = plt.subplots(nrows=2, ncols=2,
                            subplot_kw={'projection': ccrs.PlateCarree()})

        # Set up an OSGB map.
        geo_axes = plt.subplot(2, 2, 1, projection=cartopy.crs.OSGB())

    When a source projection is provided to one of it's plotting methods,
    using the *transform* keyword, the standard Matplotlib plot result is
    transformed from source coordinates to the target projection. For example::

        # Plot latlon data on an OSGB map.
        plt.axes(projection=cartopy.crs.OSGB())
        plt.contourf(x, y, data, transform=cartopy.crs.PlateCarree())

    """
    name = 'cartopy.geoaxes'

    def __init__(self, *args, **kwargs):
        """
        Create a GeoAxes object using standard matplotlib
        :class:`~matplotlib.axes.Axes` args and kwargs.

        Parameters
        ----------
        projection : cartopy.crs.Projection
            The target projection of this Axes.
        """
        if "map_projection" in kwargs:
            warnings.warn("The `map_projection` keyword argument is "
                          "deprecated, use `projection` to instantiate a "
                          "GeoAxes instead.")
            projection = kwargs.pop("map_projection")
        else:
            projection = kwargs.pop("projection")

        # The :class:`cartopy.crs.Projection` of this GeoAxes.
        if not isinstance(projection, ccrs.Projection):
            raise ValueError("A GeoAxes can only be created with a "
                             "projection of type cartopy.crs.Projection")
        self.projection = projection

        super().__init__(*args, **kwargs)
        self.img_factories = []
        self._done_img_factory = False

    def add_image(self, factory, *args, **kwargs):
        """
        Add an image "factory" to the Axes.

        Any image "factory" added will be asked to retrieve an image
        with associated metadata for a given bounding box at draw time.
        The advantage of this approach is that the limits of the map
        do not need to be known when adding the image factory, but can
        be deferred until everything which can effect the limits has been
        added.

        Parameters
        ----------
        factory
            Currently an image "factory" is just an object with
            an ``image_for_domain`` method. Examples of image factories
            are :class:`cartopy.io.img_nest.NestedImageCollection` and
            :class:`cartopy.io.img_tiles.GoogleTiles`.

        """
        if hasattr(factory, 'image_for_domain'):
            # XXX TODO: Needs deprecating.
            self.img_factories.append([factory, args, kwargs])
        else:
            # Args and kwargs not allowed.
            assert not bool(args) and not bool(kwargs)
            image = factory
            super().add_image(image)
            return image

    @contextlib.contextmanager
    def hold_limits(self, hold=True):
        """
        Keep track of the original view and data limits for the life of this
        context manager, optionally reverting any changes back to the original
        values after the manager exits.

        Parameters
        ----------
        hold: bool, optional
            Whether to revert the data and view limits after the
            context manager exits.  Defaults to True.

        """
        with contextlib.ExitStack() as stack:
            if hold:
                stack.callback(self.dataLim.set_points,
                               self.dataLim.frozen().get_points())
                stack.callback(self.viewLim.set_points,
                               self.viewLim.frozen().get_points())
                stack.callback(setattr, self, 'ignore_existing_data_limits',
                               self.ignore_existing_data_limits)
                stack.callback(self.set_autoscalex_on,
                               self.get_autoscalex_on())
                stack.callback(self.set_autoscaley_on,
                               self.get_autoscaley_on())
            yield

    def _draw_preprocess(self):
        """
        Perform pre-processing steps shared between :func:`GeoAxes.draw`
        and :func:`GeoAxes.get_tightbbox`.
        """
        # If data has been added (i.e. autoscale hasn't been turned off)
        # then we should autoscale the view.
        if self.get_autoscale_on() and self.ignore_existing_data_limits:
            self.autoscale_view()

        # apply_aspect may change the x or y data limits, so must be called
        # before the patch is updated.
        self.apply_aspect()

        # Adjust location of background patch so that new gridlines generated
        # by `draw` or `get_tightbbox` are positioned and clipped correctly.
        self.patch._adjust_location()

    def get_tightbbox(self, renderer=None, *args, **kwargs):
        """
        Extend the standard behaviour of
        :func:`matplotlib.axes.Axes.get_tightbbox`.

        Adjust the axes aspect ratio and background patch location before
        calculating the tight bounding box.
        """
        # Shared processing steps
        self._draw_preprocess()

        return super().get_tightbbox(renderer, *args, **kwargs)

    @matplotlib.artist.allow_rasterization
    def draw(self, renderer=None, **kwargs):
        """
        Extend the standard behaviour of :func:`matplotlib.axes.Axes.draw`.

        Draw image factory results before invoking standard Matplotlib drawing.
        A global range is used if no limits have yet been set.
        """
        # Shared processing steps
        self._draw_preprocess()

        # XXX This interface needs a tidy up:
        #       image drawing on pan/zoom;
        #       caching the resulting image;
        #       buffering the result by 10%...;
        if not self._done_img_factory:
            for factory, factory_args, factory_kwargs in self.img_factories:
                img, extent, origin = factory.image_for_domain(
                    self._get_extent_geom(factory.crs), factory_args[0])
                self.imshow(img, extent=extent, origin=origin,
                            transform=factory.crs, *factory_args[1:],
                            **factory_kwargs)
        self._done_img_factory = True

        return super().draw(renderer=renderer, **kwargs)

    def _update_title_position(self, renderer):
        super()._update_title_position(renderer)

        if self._autotitlepos is not None and not self._autotitlepos:
            return

        from cartopy.mpl.gridliner import Gridliner
        gridliners = [a for a in self.artists if isinstance(a, Gridliner)]
        if not gridliners:
            return

        # Get the max ymax of all top labels
        top = -1
        for gl in gridliners:
            # Both top and geo labels can appear at the top of the axes
            if gl.top_labels or gl.geo_labels:
                # Make sure Gridliner is populated and up-to-date
                gl._draw_gridliner(renderer=renderer)
                for label in (gl.top_label_artists +
                              gl.geo_label_artists):
                    bb = label.get_tightbbox(renderer)
                    top = max(top, bb.ymax)
        if top < 0:
            # nothing to do if no label found
            return
        yn = self.transAxes.inverted().transform((0., top))[1]
        if yn <= 1:
            # nothing to do if the upper bounds of labels is below
            # the top of the axes
            return

        # Loop on titles to adjust
        titles = (self.title, self._left_title, self._right_title)
        for title in titles:
            x, y0 = title.get_position()
            y = max(1.0, yn)
            title.set_position((x, y))

    def __str__(self):
        return '< GeoAxes: %s >' % self.projection

    def __clear(self):
        """Clear the current axes and add boundary lines."""
        self.xaxis.set_visible(False)
        self.yaxis.set_visible(False)
        # Enable tight autoscaling.
        self._tight = True
        self.set_aspect('equal')

        self._boundary()

        # XXX consider a margin - but only when the map is not global...
        # self._xmargin = 0.15
        # self._ymargin = 0.15

        self.dataLim.intervalx = self.projection.x_limits
        self.dataLim.intervaly = self.projection.y_limits

    def clear(self):
        """Clear the current Axes and add boundary lines."""
        result = super().clear()
        self.__clear()
        return result

    def format_coord(self, x, y):
        """
        Returns
        -------
        A string formatted for the Matplotlib GUI status bar.

        """
        lon, lat = self.projection.as_geodetic().transform_point(
            x, y, self.projection,
        )

        ns = 'N' if lat >= 0.0 else 'S'
        ew = 'E' if lon >= 0.0 else 'W'

        return (
            f'{x:.4g}, {y:.4g} '
            f'({abs(lat):f}°{ns}, {abs(lon):f}°{ew})'
        )

    def coastlines(self, resolution='auto', color='black', **kwargs):
        """
        Add coastal **outlines** to the current axes from the Natural Earth
        "coastline" shapefile collection.

        Parameters
        ----------
        resolution : str or :class:`cartopy.feature.Scaler`, optional
            A named resolution to use from the Natural Earth
            dataset. Currently can be one of "auto" (default), "110m", "50m",
            and "10m", or a Scaler object.  If "auto" is selected, the
            resolution is defined by `~cartopy.feature.auto_scaler`.

        """
        kwargs['edgecolor'] = color
        kwargs['facecolor'] = 'none'
        feature = cartopy.feature.COASTLINE

        # The coastline feature is automatically scaled by default, but for
        # anything else, including custom scaler instances, create a new
        # feature which derives from the default one.
        if resolution != 'auto':
            feature = feature.with_scale(resolution)

        return self.add_feature(feature, **kwargs)

    def tissot(self, rad_km=500, lons=None, lats=None, n_samples=80, **kwargs):
        """
        Add Tissot's indicatrices to the axes.

        Parameters
        ----------
        rad_km
            The radius in km of the circles to be drawn.
        lons
            A numpy.ndarray, list or tuple of longitude values that
            locate the centre of each circle. Specifying more than one
            dimension allows individual points to be drawn whereas a
            1D array produces a grid of points.
        lats
            A numpy.ndarray, list or tuple of latitude values that
            that locate the centre of each circle. See lons.
        n_samples
            Integer number of points sampled around the circumference of
            each circle.


        ``**kwargs`` are passed through to
        :class:`cartopy.feature.ShapelyFeature`.

        """
        from cartopy import geodesic

        geod = geodesic.Geodesic()
        geoms = []

        if lons is None:
            lons = np.linspace(-180, 180, 6, endpoint=False)
        else:
            lons = np.asarray(lons)
        if lats is None:
            lats = np.linspace(-80, 80, 6)
        else:
            lats = np.asarray(lats)

        if lons.ndim == 1 or lats.ndim == 1:
            lons, lats = np.meshgrid(lons, lats)
        lons, lats = lons.flatten(), lats.flatten()

        if lons.shape != lats.shape:
            raise ValueError('lons and lats must have the same shape.')

        for lon, lat in zip(lons, lats):
            circle = geod.circle(lon, lat, rad_km * 1e3, n_samples=n_samples)
            geoms.append(sgeom.Polygon(circle))

        feature = cartopy.feature.ShapelyFeature(geoms, ccrs.Geodetic(),
                                                 **kwargs)
        return self.add_feature(feature)

    def add_feature(self, feature, **kwargs):
        """
        Add the given :class:`~cartopy.feature.Feature` instance to the axes.

        Parameters
        ----------
        feature
            An instance of :class:`~cartopy.feature.Feature`.

        Returns
        -------
        A :class:`cartopy.mpl.feature_artist.FeatureArtist` instance
            The instance responsible for drawing the feature.

        Note
        ----
            Matplotlib keyword arguments can be used when drawing the feature.
            This allows standard Matplotlib control over aspects such as
            'facecolor', 'alpha', etc.

        """
        # Instantiate an artist to draw the feature and add it to the axes.
        artist = feature_artist.FeatureArtist(feature, **kwargs)
        return self.add_collection(artist)

    def add_geometries(self, geoms, crs, **kwargs):
        """
        Add the given shapely geometries (in the given crs) to the axes.

        Parameters
        ----------
        geoms
            A collection of shapely geometries.
        crs
            The cartopy CRS in which the provided geometries are defined.
        styler
            A callable that returns matplotlib patch styling given a geometry.

        Returns
        -------
        A :class:`cartopy.mpl.feature_artist.FeatureArtist` instance
            The instance responsible for drawing the feature.

        Note
        ----
            Matplotlib keyword arguments can be used when drawing the feature.
            This allows standard Matplotlib control over aspects such as
            'facecolor', 'alpha', etc.


        """
        styler = kwargs.pop('styler', None)
        feature = cartopy.feature.ShapelyFeature(geoms, crs, **kwargs)
        return self.add_feature(feature, styler=styler)

    def get_extent(self, crs=None):
        """
        Get the extent (x0, x1, y0, y1) of the map in the given coordinate
        system.

        If no crs is given, the returned extents' coordinate system will be
        the CRS of this Axes.

        """
        p = self._get_extent_geom(crs)
        r = p.bounds
        x1, y1, x2, y2 = r
        return x1, x2, y1, y2

    def _get_extent_geom(self, crs=None):
        # Perform the calculations for get_extent(), which just repackages it.
        with self.hold_limits():
            if self.get_autoscale_on():
                self.autoscale_view()
            [x1, y1], [x2, y2] = self.viewLim.get_points()

        domain_in_src_proj = sgeom.Polygon([[x1, y1], [x2, y1],
                                            [x2, y2], [x1, y2],
                                            [x1, y1]])

        # Determine target projection based on requested CRS.
        if crs is None:
            proj = self.projection
        elif isinstance(crs, ccrs.Projection):
            proj = crs
        else:
            # Attempt to select suitable projection for
            # non-projection CRS.
            if isinstance(crs, ccrs.RotatedGeodetic):
                proj = ccrs.RotatedPole(crs.proj4_params['lon_0'] - 180,
                                        crs.proj4_params['o_lat_p'])
                warnings.warn(f'Approximating coordinate system {crs!r} with '
                              'a RotatedPole projection.')
            elif hasattr(crs, 'is_geodetic') and crs.is_geodetic():
                proj = ccrs.PlateCarree(globe=crs.globe)
                warnings.warn(f'Approximating coordinate system {crs!r} with '
                              'the PlateCarree projection.')
            else:
                raise ValueError('Cannot determine extent in'
                                 f' coordinate system {crs!r}')

        # Calculate intersection with boundary and project if necessary.
        boundary_poly = sgeom.Polygon(self.projection.boundary)
        if proj != self.projection:
            # Erode boundary by threshold to avoid transform issues.
            # This is a workaround for numerical issues at the boundary.
            eroded_boundary = boundary_poly.buffer(-self.projection.threshold)
            geom_in_src_proj = eroded_boundary.intersection(
                domain_in_src_proj)
            geom_in_crs = proj.project_geometry(geom_in_src_proj,
                                                self.projection)
        else:
            geom_in_crs = boundary_poly.intersection(domain_in_src_proj)

        return geom_in_crs

    def set_extent(self, extents, crs=None):
        """
        Set the extent (x0, x1, y0, y1) of the map in the given
        coordinate system.

        If no crs is given, the extents' coordinate system will be assumed
        to be the Geodetic version of this axes' projection.

        Parameters
        ----------
        extents
            Tuple of floats representing the required extent (x0, x1, y0, y1).
        """
        # TODO: Implement the same semantics as plt.xlim and
        # plt.ylim - allowing users to set None for a minimum and/or
        # maximum value
        x1, x2, y1, y2 = extents
        domain_in_crs = sgeom.polygon.LineString([[x1, y1], [x2, y1],
                                                  [x2, y2], [x1, y2],
                                                  [x1, y1]])

        projected = None

        # Sometimes numerical issues cause the projected vertices of the
        # requested extents to appear outside the projection domain.
        # This results in an empty geometry, which has an empty `bounds`
        # tuple, which causes an unpack error.
        # This workaround avoids using the projection when the requested
        # extents are obviously the same as the projection domain.
        try_workaround = ((crs is None and
                           isinstance(self.projection, ccrs.PlateCarree)) or
                          crs == self.projection)
        if try_workaround:
            boundary = self.projection.boundary
            if boundary.equals(domain_in_crs):
                projected = boundary

        if projected is None:
            projected = self.projection.project_geometry(domain_in_crs, crs)
        try:
            # This might fail with an unhelpful error message ('need more
            # than 0 values to unpack') if the specified extents fall outside
            # the projection extents, so try and give a better error message.
            x1, y1, x2, y2 = projected.bounds
        except ValueError:
            raise ValueError(
                'Failed to determine the required bounds in projection '
                'coordinates. Check that the values provided are within the '
                f'valid range (x_limits={self.projection.x_limits}, '
                f'y_limits={self.projection.y_limits}).')

        self.set_xlim([x1, x2])
        self.set_ylim([y1, y2])

    def set_global(self):
        """
        Set the extent of the Axes to the limits of the projection.

        Note
        ----
            In some cases where the projection has a limited sensible range
            the ``set_global`` method does not actually make the whole globe
            visible. Instead, the most appropriate extents will be used (e.g.
            Ordnance Survey UK will set the extents to be around the British
            Isles.

        """
        self.set_xlim(self.projection.x_limits)
        self.set_ylim(self.projection.y_limits)

    def autoscale_view(self, tight=None, scalex=True, scaley=True):
        """
        Autoscale the view limits using the data limits, taking into
        account the projection of the geoaxes.

        See :meth:`~matplotlib.axes.Axes.imshow()` for more details.
        """
        super().autoscale_view(tight=tight, scalex=scalex, scaley=scaley)
        # Limit the resulting bounds to valid area.
        if scalex and self.get_autoscalex_on():
            bounds = self.get_xbound()
            self.set_xbound(max(bounds[0], self.projection.x_limits[0]),
                            min(bounds[1], self.projection.x_limits[1]))
        if scaley and self.get_autoscaley_on():
            bounds = self.get_ybound()
            self.set_ybound(max(bounds[0], self.projection.y_limits[0]),
                            min(bounds[1], self.projection.y_limits[1]))

    def set_xticks(self, ticks, minor=False, crs=None):
        """
        Set the x ticks.

        Parameters
        ----------
        ticks
            List of floats denoting the desired position of x ticks.
        minor: optional
            flag indicating whether the ticks should be minor
            ticks i.e. small and unlabelled (defaults to False).
        crs: optional
            An instance of :class:`~cartopy.crs.CRS` indicating the
            coordinate system of the provided tick values. If no
            coordinate system is specified then the values are assumed
            to be in the coordinate system of the projection.
            Only transformations from one rectangular coordinate system
            to another rectangular coordinate system are supported (defaults
            to None).

        Note
        ----
            This interface is subject to change whilst functionality is added
            to support other map projections.

        """
        # Project ticks if crs differs from axes' projection
        if crs is not None and crs != self.projection:
            if not isinstance(crs, (ccrs._RectangularProjection,
                                    ccrs.Mercator)) or \
                    not isinstance(self.projection,
                                   (ccrs._RectangularProjection,
                                    ccrs.Mercator)):
                raise RuntimeError('Cannot handle non-rectangular coordinate '
                                   'systems.')
            proj_xyz = self.projection.transform_points(crs,
                                                        np.asarray(ticks),
                                                        np.zeros(len(ticks)))
            xticks = proj_xyz[..., 0]
        else:
            xticks = ticks

        # Switch on drawing of x axis
        self.xaxis.set_visible(True)

        return super().set_xticks(xticks, minor=minor)

    def set_yticks(self, ticks, minor=False, crs=None):
        """
        Set the y ticks.

        Parameters
        ----------
        ticks
            List of floats denoting the desired position of y ticks.
        minor: optional
            flag indicating whether the ticks should be minor
            ticks i.e. small and unlabelled (defaults to False).
        crs: optional
            An instance of :class:`~cartopy.crs.CRS` indicating the
            coordinate system of the provided tick values. If no
            coordinate system is specified then the values are assumed
            to be in the coordinate system of the projection.
            Only transformations from one rectangular coordinate system
            to another rectangular coordinate system are supported (defaults
            to None).

        Note
        ----
            This interface is subject to change whilst functionality is added
            to support other map projections.

        """
        # Project ticks if crs differs from axes' projection
        if crs is not None and crs != self.projection:
            if not isinstance(crs, (ccrs._RectangularProjection,
                                    ccrs.Mercator)) or \
                    not isinstance(self.projection,
                                   (ccrs._RectangularProjection,
                                    ccrs.Mercator)):
                raise RuntimeError('Cannot handle non-rectangular coordinate '
                                   'systems.')
            proj_xyz = self.projection.transform_points(crs,
                                                        np.zeros(len(ticks)),
                                                        np.asarray(ticks))
            yticks = proj_xyz[..., 1]
        else:
            yticks = ticks

        # Switch on drawing of y axis
        self.yaxis.set_visible(True)

        return super().set_yticks(yticks, minor=minor)

    def stock_img(self, name='ne_shaded', **kwargs):
        """
        Add a standard image to the map.

        Currently, the only (and default) option for image is a downsampled
        version of the Natural Earth shaded relief raster. Other options
        (e.g., alpha) will be passed to :func:`GeoAxes.imshow`.

        """
        if name == 'ne_shaded':
            source_proj = ccrs.PlateCarree()
            fname = (config["repo_data_dir"] / 'raster' / 'natural_earth'
                     / '50-natural-earth-1-downsampled.png')

            return self.imshow(imread(fname), origin='upper',
                               transform=source_proj,
                               extent=[-180, 180, -90, 90],
                               **kwargs)
        else:
            raise ValueError('Unknown stock image %r.' % name)

    def background_img(self, name='ne_shaded', resolution='low', extent=None,
                       cache=False):
        """
        Add a background image to the map, from a selection of pre-prepared
        images held in a directory specified by the CARTOPY_USER_BACKGROUNDS
        environment variable. That directory is checked with
        func:`self.read_user_background_images` and needs to contain a JSON
        file which defines for the image metadata.

        Parameters
        ----------
        name: optional
            The name of the image to read according to the contents
            of the JSON file. A typical file might have, for instance:
            'ne_shaded' : Natural Earth Shaded Relief
            'ne_grey' : Natural Earth Grey Earth.
        resolution: optional
            The resolution of the image to read, according to
            the contents of the JSON file. A typical file might
            have the following for each name of the image:
            'low', 'med', 'high', 'vhigh', 'full'.
        extent: optional
            Using a high resolution background image zoomed into
            a small area will take a very long time to render as
            the image is prepared globally, even though only a small
            area is used. Adding the extent will only render a
            particular geographic region. Specified as
            [longitude start, longitude end,
            latitude start, latitude end].

                  e.g. [-11, 3, 48, 60] for the UK
                  or [167.0, 193.0, 47.0, 68.0] to cross the date line.

        cache: optional
            Logical flag as to whether or not to cache the loaded
            images into memory. The images are stored before the
            extent is used.

        """
        # read in the user's background image directory:
        if len(_USER_BG_IMGS) == 0:
            self.read_user_background_images()
        bgdir = Path(os.getenv(
            'CARTOPY_USER_BACKGROUNDS',
            config["repo_data_dir"] / 'raster' / 'natural_earth'))
        # now get the filename we want to use:
        try:
            fname = _USER_BG_IMGS[name][resolution]
        except KeyError:
            raise ValueError(
                f'Image {name!r} and resolution {resolution!r} are not '
                f'present in the user background image metadata in directory '
                f'{bgdir!r}')
        # Now obtain the image data from file or cache:
        fpath = bgdir / fname
        if cache:
            if fname in _BACKG_IMG_CACHE:
                img = _BACKG_IMG_CACHE[fname]
            else:
                img = imread(fpath)
                _BACKG_IMG_CACHE[fname] = img
        else:
            img = imread(fpath)
        if len(img.shape) == 2:
            # greyscale images are only 2-dimensional, so need replicating
            # to 3 colour channels:
            img = np.repeat(img[:, :, np.newaxis], 3, axis=2)
        # now get the projection from the metadata:
        if _USER_BG_IMGS[name]['__projection__'] == 'PlateCarree':
            # currently only PlateCarree is defined:
            source_proj = ccrs.PlateCarree()
        else:
            raise NotImplementedError('Background image projection undefined')

        if extent is None:
            # not specifying an extent, so return all of it:
            return self.imshow(img, origin='upper',
                               transform=source_proj,
                               extent=[-180, 180, -90, 90])
        else:
            # return only a subset of the image:
            # set up coordinate arrays:
            d_lat = 180 / img.shape[0]
            d_lon = 360 / img.shape[1]
            # latitude starts at 90N for this image:
            lat_pts = (np.arange(img.shape[0]) * -d_lat - (d_lat / 2)) + 90
            lon_pts = (np.arange(img.shape[1]) * d_lon + (d_lon / 2)) - 180

            # which points are in range:
            lat_in_range = np.logical_and(lat_pts >= extent[2],
                                          lat_pts <= extent[3])
            if extent[0] < 180 and extent[1] > 180:
                # we have a region crossing the dateline
                # this is the westerly side of the input image:
                lon_in_range1 = np.logical_and(lon_pts >= extent[0],
                                               lon_pts <= 180.0)
                img_subset1 = img[lat_in_range, :, :][:, lon_in_range1, :]
                # and the eastward half:
                lon_in_range2 = lon_pts + 360. <= extent[1]
                img_subset2 = img[lat_in_range, :, :][:, lon_in_range2, :]
                # now join them up:
                img_subset = np.concatenate((img_subset1, img_subset2), axis=1)
                # now define the extent for output that matches those points:
                ret_extent = [lon_pts[lon_in_range1][0] - d_lon / 2,
                              lon_pts[lon_in_range2][-1] + d_lon / 2 + 360,
                              lat_pts[lat_in_range][-1] - d_lat / 2,
                              lat_pts[lat_in_range][0] + d_lat / 2]
            else:
                # not crossing the dateline, so just find the region:
                lon_in_range = np.logical_and(lon_pts >= extent[0],
                                              lon_pts <= extent[1])
                img_subset = img[lat_in_range, :, :][:, lon_in_range, :]
                # now define the extent for output that matches those points:
                ret_extent = [lon_pts[lon_in_range][0] - d_lon / 2.0,
                              lon_pts[lon_in_range][-1] + d_lon / 2.0,
                              lat_pts[lat_in_range][-1] - d_lat / 2.0,
                              lat_pts[lat_in_range][0] + d_lat / 2.0]

            return self.imshow(img_subset, origin='upper',
                               transform=source_proj,
                               extent=ret_extent)

    def read_user_background_images(self, verify=True):
        """
        Read the metadata in the specified CARTOPY_USER_BACKGROUNDS
        environment variable to populate the dictionaries for background_img.

        If CARTOPY_USER_BACKGROUNDS is not set then by default the image in
        lib/cartopy/data/raster/natural_earth/ will be made available.

        The metadata should be a standard JSON file which specifies a two
        level dictionary. The first level is the image type.
        For each image type there must be the fields:
        __comment__, __source__ and __projection__
        and then an element giving the filename for each resolution.

        An example JSON file can be found at:
        lib/cartopy/data/raster/natural_earth/images.json

        """
        bgdir = Path(os.getenv(
            'CARTOPY_USER_BACKGROUNDS',
            config["repo_data_dir"] / 'raster' / 'natural_earth'))
        json_file = bgdir / 'images.json'

        with open(json_file) as js_obj:
            dict_in = json.load(js_obj)
        for img_type in dict_in:
            _USER_BG_IMGS[img_type] = dict_in[img_type]

        if verify:
            required_info = ['__comment__', '__source__', '__projection__']
            for img_type in _USER_BG_IMGS:
                if img_type == '__comment__':
                    # the top level comment doesn't need verifying:
                    pass
                else:
                    # check that this image type has the required info:
                    for required in required_info:
                        if required not in _USER_BG_IMGS[img_type]:
                            raise ValueError(
                                f'User background metadata file {json_file!r},'
                                f' image type {img_type!r}, does not specify'
                                f' metadata item {required!r}')
                    for resln in _USER_BG_IMGS[img_type]:
                        # the required_info items are not resolutions:
                        if resln not in required_info:
                            img_it_r = _USER_BG_IMGS[img_type][resln]
                            test_file = bgdir / img_it_r
                            if not test_file.is_file():
                                raise ValueError(
                                    f'File "{test_file}" not found')

    def add_raster(self, raster_source, **slippy_image_kwargs):
        """
        Add the given raster source to the GeoAxes.

        Parameters
        ----------
        raster_source:
            :class:`cartopy.io.RasterSource` like instance
             ``raster_source`` may be any object which
             implements the RasterSource interface, including
             instances of objects such as
             :class:`~cartopy.io.ogc_clients.WMSRasterSource`
             and
             :class:`~cartopy.io.ogc_clients.WMTSRasterSource`.
             Note that image retrievals are done at draw time,
             not at creation time.

        """
        # Allow a fail-fast error if the raster source cannot provide
        # images in the current projection.
        raster_source.validate_projection(self.projection)
        img = SlippyImageArtist(self, raster_source, **slippy_image_kwargs)
        with self.hold_limits():
            self.add_image(img)
        return img

    def _regrid_shape_aspect(self, regrid_shape, target_extent):
        """
        Helper for setting regridding shape which is used in several
        plotting methods.

        """
        if not isinstance(regrid_shape, collections.abc.Sequence):
            target_size = int(regrid_shape)
            x_range, y_range = np.diff(target_extent)[::2]
            desired_aspect = x_range / y_range
            if x_range >= y_range:
                regrid_shape = (int(target_size * desired_aspect), target_size)
            else:
                regrid_shape = (target_size, int(target_size / desired_aspect))
        return regrid_shape

    @_add_transform
    def imshow(self, img, *args, **kwargs):
        """
        Add the "transform" keyword to :func:`~matplotlib.pyplot.imshow`.

        Parameters
        ----------
        img
            The image to be displayed.

        Other Parameters
        ----------------
        transform: :class:`~cartopy.crs.Projection` or matplotlib transform
            The coordinate system in which the given image is
            rectangular.
        regrid_shape: int or pair of ints
            The shape of the desired image if it needs to be
            transformed.  If a single integer is given then
            that will be used as the minimum length dimension,
            while the other dimension will be scaled up
            according to the target extent's aspect ratio.
            The default is for the minimum dimension of a
            transformed image to have length 750, so for an
            image being transformed into a global PlateCarree
            projection the resulting transformed image would
            have a shape of ``(750, 1500)``.
        extent: tuple
            The corner coordinates of the image in the form
            ``(left, right, bottom, top)``. The coordinates should
            be in the coordinate system passed to the transform
            keyword.
        origin: {'lower', 'upper'}
            The origin of the vertical pixels. See
            :func:`matplotlib.pyplot.imshow` for further details.
            Default is ``'upper'``. Prior to 0.18, it was ``'lower'``.

        """
        if 'update_datalim' in kwargs:
            raise ValueError('The update_datalim keyword has been removed in '
                             'imshow. To hold the data and view limits see '
                             'GeoAxes.hold_limits.')

        transform = kwargs.pop('transform')
        extent = kwargs.get('extent', None)
        kwargs.setdefault('origin', 'upper')

        same_projection = (isinstance(transform, ccrs.Projection) and
                           self.projection == transform)

        # Only take the shortcut path if the image is within the current
        # bounds (+/- threshold) of the projection
        x0, x1 = self.projection.x_limits
        y0, y1 = self.projection.y_limits
        eps = self.projection.threshold
        inside_bounds = (extent is None or
                         (x0 - eps <= extent[0] <= x1 + eps and
                          x0 - eps <= extent[1] <= x1 + eps and
                          y0 - eps <= extent[2] <= y1 + eps and
                          y0 - eps <= extent[3] <= y1 + eps))

        if (transform is None or transform == self.transData or
                same_projection and inside_bounds):
            if "regrid_shape" in kwargs:
                warnings.warn("ignoring regrid_shape because it doesn't do anything "
                              "when working in the same projection. To avoid this "
                              "warning, remove the 'regrid_shape' keyword argument.")
                kwargs.pop("regrid_shape")
            result = super().imshow(img, *args, **kwargs)
        else:
            extent = kwargs.pop('extent', None)
            img = np.asanyarray(img)
            if kwargs['origin'] == 'upper':
                # It is implicitly assumed by the regridding operation that the
                # origin of the image is 'lower', so simply adjust for that
                # here.
                img = img[::-1]
                kwargs['origin'] = 'lower'

            if not isinstance(transform, ccrs.Projection):
                raise ValueError('Expected a projection subclass. Cannot '
                                 'handle a %s in imshow.' % type(transform))

            target_extent = self.get_extent(self.projection)
            regrid_shape = kwargs.pop('regrid_shape', 750)
            regrid_shape = self._regrid_shape_aspect(regrid_shape,
                                                     target_extent)
            # Lazy import because scipy/pykdtree in img_transform are only
            # optional dependencies
            from cartopy.img_transform import warp_array
            original_extent = extent
            img, extent = warp_array(img,
                                     source_proj=transform,
                                     source_extent=original_extent,
                                     target_proj=self.projection,
                                     target_res=regrid_shape,
                                     target_extent=target_extent,
                                     mask_extrapolated=True,
                                     )
            alpha = kwargs.pop('alpha', None)
            if np.array(alpha).ndim == 2:
                alpha, _ = warp_array(alpha,
                                      source_proj=transform,
                                      source_extent=original_extent,
                                      target_proj=self.projection,
                                      target_res=regrid_shape,
                                      target_extent=target_extent,
                                      mask_extrapolated=True,
                                      )
            kwargs['alpha'] = alpha

            # As a workaround to a matplotlib limitation, turn any images
            # which are masked array RGB(A) into RGBA images

            if np.ma.is_masked(img) and len(img.shape) > 2:

                # transform RGB(A) into RGBA
                old_img = img
                img = np.ones(old_img.shape[:2] + (4, ),
                              dtype=old_img.dtype)
                img[:, :, :3] = old_img[:, :, :3]

                # if img is RGBA, save alpha channel
                if old_img.shape[-1] == 4:
                    img[:, :, 3] = old_img[:, :, 3]
                elif img.dtype.kind == 'u':
                    img[:, :, 3] *= 255

                # apply the mask to the A channel
                img[np.any(old_img[:, :, :3].mask, axis=2), 3] = 0

            result = super().imshow(img, *args, extent=extent, **kwargs)

        return result

    def gridlines(self, crs=None, draw_labels=False,
                  xlocs=None, ylocs=None, dms=False,
                  x_inline=None, y_inline=None, auto_inline=True,
                  xformatter=None, yformatter=None, xlim=None, ylim=None,
                  rotate_labels=None, xlabel_style=None, ylabel_style=None,
                  labels_bbox_style=None, xpadding=5, ypadding=5,
                  offset_angle=25, auto_update=None, formatter_kwargs=None,
                  **kwargs):
        """
        Automatically add gridlines to the axes, in the given coordinate
        system, at draw time.

        Parameters
        ----------
        crs: optional
            The :class:`cartopy._crs.CRS` defining the coordinate system in
            which gridlines are drawn.
            Defaults to :class:`cartopy.crs.PlateCarree`.
        draw_labels: optional
            Toggle whether to draw labels. For finer control, attributes of
            :class:`Gridliner` may be modified individually. Defaults to False.

            - string: "x" or "y" to only draw labels of the respective
              coordinate in the CRS.
            - list: Can contain the side identifiers and/or coordinate
              types to select which ones to draw.
              For all labels one would use
              `["x", "y", "top", "bottom", "left", "right", "geo"]`.
            - dict: The keys are the side identifiers
              ("top", "bottom", "left", "right") and the values are the
              coordinates ("x", "y"); this way you can precisely
              decide what kind of label to draw and where.
              For x labels on the bottom and y labels on the right you
              could pass in `{"bottom": "x", "left": "y"}`.

            Note that, by default, x and y labels are not drawn on left/right
            and top/bottom edges respectively unless explicitly requested.

        xlocs: optional
            An iterable of gridline locations or a
            :class:`matplotlib.ticker.Locator` instance which will be
            used to determine the locations of the gridlines in the
            x-coordinate of the given CRS. Defaults to None, which
            implies automatic locating of the gridlines.
        ylocs: optional
            An iterable of gridline locations or a
            :class:`matplotlib.ticker.Locator` instance which will be
            used to determine the locations of the gridlines in the
            y-coordinate of the given CRS. Defaults to None, which
            implies automatic locating of the gridlines.
        dms: bool
            When default longitude and latitude locators and formatters are
            used, ticks are able to stop on minutes and seconds if minutes is
            set to True, and not fraction of degrees. This keyword is passed
            to :class:`~cartopy.mpl.gridliner.Gridliner` and has no effect
            if xlocs and ylocs are explicitly set.
        x_inline: optional
            Toggle whether the x labels drawn should be inline.
        y_inline: optional
            Toggle whether the y labels drawn should be inline.
        auto_inline: optional
            Set x_inline and y_inline automatically based on projection
        xformatter: optional
            A :class:`matplotlib.ticker.Formatter` instance to format labels
            for x-coordinate gridlines. It defaults to None, which implies the
            use of a :class:`cartopy.mpl.ticker.LongitudeFormatter` initiated
            with the ``dms`` argument, if the crs is of
            :class:`~cartopy.crs.PlateCarree` type.
        yformatter: optional
            A :class:`matplotlib.ticker.Formatter` instance to format labels
            for y-coordinate gridlines. It defaults to None, which implies the
            use of a :class:`cartopy.mpl.ticker.LatitudeFormatter` initiated
            with the ``dms`` argument, if the crs is of
            :class:`~cartopy.crs.PlateCarree` type.
        xlim: optional
            Set a limit for the gridlines so that they do not go all the
            way to the edge of the boundary. xlim can be a single number or
            a (min, max) tuple. If a single number, the limits will be
            (-xlim, +xlim).
        ylim: optional
            Set a limit for the gridlines so that they do not go all the
            way to the edge of the boundary. ylim can be a single number or
            a (min, max) tuple. If a single number, the limits will be
            (-ylim, +ylim).
        rotate_labels: optional, bool, str
            Allow the rotation of non-inline labels.

            - False: Do not rotate the labels.
            - True: Rotate the labels parallel to the gridlines.
            - None: no rotation except for some projections (default).
            - A float: Rotate labels by this value in degrees.

        xlabel_style: dict
            A dictionary passed through to ``ax.text`` on x label creation
            for styling of the text labels.
        ylabel_style: dict
            A dictionary passed through to ``ax.text`` on y label creation
            for styling of the text labels.
        labels_bbox_style: dict
            bbox style for all text labels.
        xpadding: float
            Padding for x labels. If negative, the labels are
            drawn inside the map.
        ypadding: float
            Padding for y labels. If negative, the labels are
            drawn inside the map.
        offset_angle: float
            Difference of angle in degrees from 90 to define when
            a label must be flipped to be more readable.
            For example, a value of 10 makes a vertical top label to be
            flipped only at 100 degrees.
        auto_update: bool, default=True
            Whether to update the gridlines and labels when the plot is
            refreshed.

            .. deprecated:: 0.23
               In future the gridlines and labels will always be updated.

        formatter_kwargs: dict, optional
            Options passed to the default formatters.
            See :class:`~cartopy.mpl.ticker.LongitudeFormatter` and
            :class:`~cartopy.mpl.ticker.LatitudeFormatter`

        Keyword Parameters
        ------------------
        **kwargs:
            All other keywords control line properties.  These are passed
            through to :class:`matplotlib.collections.Collection`.

        Returns
        -------
        gridliner
            A :class:`cartopy.mpl.gridliner.Gridliner` instance.

        Notes
        -----
        The "x" and "y" for locations and inline settings do not necessarily
        correspond to X and Y, but to the first and second coordinates of the
        specified CRS. For the common case of PlateCarree gridlines, these
        correspond to longitudes and latitudes. Depending on the projection
        used for the map, meridians and parallels can cross both the X axis and
        the Y axis.
        """
        if crs is None:
            crs = ccrs.PlateCarree(globe=self.projection.globe)
        from cartopy.mpl.gridliner import Gridliner
        gl = Gridliner(
            self, crs=crs, draw_labels=draw_labels, xlocator=xlocs,
            ylocator=ylocs, collection_kwargs=kwargs, dms=dms,
            x_inline=x_inline, y_inline=y_inline, auto_inline=auto_inline,
            xformatter=xformatter, yformatter=yformatter,
            xlim=xlim, ylim=ylim, rotate_labels=rotate_labels,
            xlabel_style=xlabel_style, ylabel_style=ylabel_style,
            labels_bbox_style=labels_bbox_style,
            xpadding=xpadding, ypadding=ypadding, offset_angle=offset_angle,
            auto_update=auto_update, formatter_kwargs=formatter_kwargs)
        self.add_artist(gl)
        return gl

    def _gen_axes_patch(self):
        return _ViewClippedPathPatch(self)

    def _gen_axes_spines(self, locations=None, offset=0.0, units='inches'):
        # generate some axes spines, as some Axes super class machinery
        # requires them. Just make them invisible
        spines = super()._gen_axes_spines(locations=locations,
                                          offset=offset,
                                          units=units)
        for spine in spines.values():
            spine.set_visible(False)

        spines['geo'] = GeoSpine(self)
        return spines

    def _boundary(self):
        """
        Add the map's boundary to this GeoAxes.

        The :data:`.patch` and :data:`.spines['geo']` are updated to match.

        """
        path = cpath.shapely_to_path(self.projection.boundary)

        # Get the outline path in terms of self.transData
        proj_to_data = self.projection._as_mpl_transform(self) - self.transData
        trans_path = proj_to_data.transform_path(path)

        # Set the boundary - we can make use of the rectangular clipping.
        self.set_boundary(trans_path)

        # Attach callback events for when the xlim or ylim are changed. This
        # is what triggers the patches to be re-clipped at draw time.
        self.callbacks.connect('xlim_changed', _trigger_patch_reclip)
        self.callbacks.connect('ylim_changed', _trigger_patch_reclip)

    def set_boundary(self, path, transform=None):
        """
        Given a path, update :data:`.spines['geo']` and :data:`.patch`.

        Parameters
        ----------
        path: :class:`matplotlib.path.Path`
            The path of the desired boundary.
        transform: None or :class:`matplotlib.transforms.Transform`, optional
            The coordinate system of the given path. Currently
            this must be convertible to data coordinates, and
            therefore cannot extend beyond the limits of the
            axes' projection.

        """
        if transform is None:
            transform = self.transData

        if isinstance(transform, cartopy.crs.CRS):
            transform = transform._as_mpl_transform(self)

        # Attach the original path to the patches. This will be used each time
        # a new clipped path is calculated.
        self.patch.set_boundary(path, transform)
        self.spines['geo'].set_boundary(path, transform)

    @_add_transform
    @_add_transform_first
    def contour(self, *args, **kwargs):
        """
        Add the "transform" keyword to :func:`~matplotlib.pyplot.contour`.

        Other Parameters
        ----------------
        transform
            A :class:`~cartopy.crs.Projection`.

        transform_first : bool, optional
            If True, this will transform the input arguments into
            projection-space before computing the contours, which is much
            faster than computing the contours in data-space and projecting
            the filled polygons. Using this method does not handle wrapped
            coordinates as well and can produce misleading contours in the
            middle of the domain. To use the projection-space method the input
            arguments X and Y must be provided and be 2-dimensional.
            The default is False, to compute the contours in data-space.

        """
        result = super().contour(*args, **kwargs)

        if not _MPL_38:
            # We need to compute the dataLim correctly for contours.
            bboxes = [col.get_datalim(self.transData)
                      for col in result.collections
                      if col.get_paths()]
            if bboxes:
                extent = mtransforms.Bbox.union(bboxes)
                self.update_datalim(extent.get_points())
        else:
            # We need to compute the dataLim correctly for contours and set the
            # artist's sticky edges to match.
            datalim = result.get_datalim(self.transData)
            self.update_datalim(datalim)
            result.sticky_edges.x[:] = datalim.xmin, datalim.xmax
            result.sticky_edges.y[:] = datalim.ymin, datalim.ymax

        self.autoscale_view()

        # Re-cast the contour as a GeoContourSet.
        if isinstance(result, matplotlib.contour.QuadContourSet):
            result.__class__ = cartopy.mpl.contour.GeoContourSet
        return result

    @_add_transform
    @_add_transform_first
    def contourf(self, *args, **kwargs):
        """
        Add the "transform" keyword to :func:`~matplotlib.pyplot.contourf`.

        Other Parameters
        ----------------
        transform
            A :class:`~cartopy.crs.Projection`.

        transform_first : bool, optional
            If True, this will transform the input arguments into
            projection-space before computing the contours, which is much
            faster than computing the contours in data-space and projecting
            the filled polygons. Using this method does not handle wrapped
            coordinates as well and can produce misleading contours in the
            middle of the domain. To use the projection-space method the input
            arguments X and Y must be provided and be 2-dimensional.
            The default is False, to compute the contours in data-space.
        """
        result = super().contourf(*args, **kwargs)

        if not _MPL_38:
            # We need to compute the dataLim correctly for contours.
            bboxes = [col.get_datalim(self.transData)
                      for col in result.collections
                      if col.get_paths()]
            if bboxes:
                extent = mtransforms.Bbox.union(bboxes)
                self.update_datalim(extent.get_points())
        else:
            # We need to compute the dataLim correctly for contours and set the
            # artist's sticky edges to match.
            datalim = result.get_datalim(self.transData)
            self.update_datalim(datalim)
            result.sticky_edges.x[:] = datalim.xmin, datalim.xmax
            result.sticky_edges.y[:] = datalim.ymin, datalim.ymax

        self.autoscale_view()

        # Re-cast the contour as a GeoContourSet.
        if isinstance(result, matplotlib.contour.QuadContourSet):
            result.__class__ = cartopy.mpl.contour.GeoContourSet

        return result

    @_add_transform
    def scatter(self, *args, **kwargs):
        """
        Add the "transform" keyword to :func:`~matplotlib.pyplot.scatter`.

        Other Parameters
        ----------------
        transform
            A :class:`~cartopy.crs.Projection`.

        """
        # exclude Geodetic as a valid source CS
        if (isinstance(kwargs['transform'],
                       InterProjectionTransform) and
                kwargs['transform'].source_projection.is_geodetic()):
            raise ValueError('Cartopy cannot currently do spherical '
                             'scatter. The source CRS cannot be a '
                             'geodetic, consider using the cylindrical form '
                             '(PlateCarree or RotatedPole).')

        result = super().scatter(*args, **kwargs)
        self.autoscale_view()
        return result

    @_add_transform
    def annotate(self, text, xy, xytext=None, xycoords='data', textcoords=None,
                 *args, **kwargs):
        """
        Add the "transform" keyword to :func:`~matplotlib.pyplot.annotate`.

        Other Parameters
        ----------------
        transform
            A :class:`~cartopy.crs.Projection`.

        """
        transform = kwargs.pop('transform', None)
        is_transform_crs = isinstance(transform, ccrs.CRS)

        # convert CRS to mpl transform for default 'data' setup
        if is_transform_crs and xycoords == 'data':
            xycoords = transform._as_mpl_transform(self)

        # textcoords = xycoords by default but complains if xytext is empty
        if textcoords is None and xytext is not None:
            textcoords = xycoords

        # use transform if textcoords is data and xytext is provided
        if is_transform_crs and xytext is not None and textcoords == 'data':
            textcoords = transform._as_mpl_transform(self)

        # convert to mpl_transform if CRS passed to xycoords
        if isinstance(xycoords, ccrs.CRS):
            xycoords = xycoords._as_mpl_transform(self)

        # convert to mpl_transform if CRS passed to textcoords
        if isinstance(textcoords, ccrs.CRS):
            textcoords = textcoords._as_mpl_transform(self)

        result = super().annotate(text, xy, xytext, xycoords=xycoords,
                                  textcoords=textcoords, *args, **kwargs)
        self.autoscale_view()
        return result

    @_add_transform
    def hexbin(self, x, y, *args, **kwargs):
        """
        Add the "transform" keyword to :func:`~matplotlib.pyplot.hexbin`.

        The points are first transformed into the projection of the axes and
        then the hexbin algorithm is computed using the data in the axes
        projection.

        Other Parameters
        ----------------
        transform
            A :class:`~cartopy.crs.Projection`.
        """
        t = kwargs.pop('transform')
        pairs = self.projection.transform_points(
            t,
            np.asarray(x),
            np.asarray(y),
        )
        x = pairs[:, 0]
        y = pairs[:, 1]

        result = super().hexbin(x, y, *args, **kwargs)
        self.autoscale_view()
        return result

    @_add_transform
    def pcolormesh(self, *args, **kwargs):
        """
        Add the "transform" keyword to :func:`~matplotlib.pyplot.pcolormesh`.

        Other Parameters
        ----------------
        transform
            A :class:`~cartopy.crs.Projection`.

        """
        # Add in an argument checker to handle Matplotlib's potential
        # interpolation when coordinate wraps are involved
        args, kwargs = self._wrap_args(*args, **kwargs)
        result = super().pcolormesh(*args, **kwargs)
        # Wrap the quadrilaterals if necessary
        result = self._wrap_quadmesh(result, **kwargs)
        # Re-cast the QuadMesh as a GeoQuadMesh to enable future wrapping
        # updates to the collection as well.
        result.__class__ = cartopy.mpl.geocollection.GeoQuadMesh

        self.autoscale_view()
        return result

    def _wrap_args(self, *args, **kwargs):
        """
        Handle the interpolation when a wrap could be involved with
        the data coordinates before passing on to Matplotlib.
        """
        default_shading = mpl.rcParams.get('pcolor.shading')
        shading = kwargs.get('shading') or default_shading
        if not (shading in ('nearest', 'auto') and len(args) == 3 and
                getattr(kwargs.get('transform'), '_wrappable', False)):
            return args, kwargs

        # We have changed the shading from nearest/auto to flat
        # due to the addition of an extra coordinate
        kwargs['shading'] = 'flat'
        X = np.asanyarray(args[0])
        Y = np.asanyarray(args[1])
        nrows, ncols = np.asanyarray(args[2]).shape[:2]
        Nx = X.shape[-1]
        Ny = Y.shape[0]
        if X.ndim != 2 or X.shape[0] == 1:
            X = X.reshape(1, Nx).repeat(Ny, axis=0)
        if Y.ndim != 2 or Y.shape[1] == 1:
            Y = Y.reshape(Ny, 1).repeat(Nx, axis=1)

        def _interp_grid(X, wrap=0):
            # helper for below
            if np.shape(X)[1] > 1:
                dX = np.diff(X, axis=1)
                # account for the wrap
                if wrap:
                    dX = (dX + wrap / 2) % wrap - wrap / 2
                dX = dX / 2
                X = np.hstack((X[:, [0]] - dX[:, [0]],
                               X[:, :-1] + dX,
                               X[:, [-1]] + dX[:, [-1]]))
            else:
                # This is just degenerate, but we can't reliably guess
                # a dX if there is just one value.
                X = np.hstack((X, X))
            return X
        t = kwargs.get('transform')
        xwrap = abs(t.x_limits[1] - t.x_limits[0])
        if ncols == Nx:
            X = _interp_grid(X, wrap=xwrap)
            Y = _interp_grid(Y)
        if nrows == Ny:
            X = _interp_grid(X.T, wrap=xwrap).T
            Y = _interp_grid(Y.T).T

        return (X, Y, args[2]), kwargs

    def _wrap_quadmesh(self, collection, **kwargs):
        """
        Handles the Quadmesh collection when any of the quadrilaterals
        cross the boundary of the projection.
        """
        t = kwargs.get('transform', None)

        # Get the quadmesh data coordinates
        coords = collection._coordinates
        Ny, Nx, _ = coords.shape
        if kwargs.get('shading') == 'gouraud':
            # Gouraud shading has the same shape for coords and data
            data_shape = Ny, Nx, -1
        else:
            data_shape = Ny - 1, Nx - 1, -1
        # data array
        C = collection.get_array().reshape(data_shape)
        if C.shape[-1] == 1:
            C = C.squeeze(axis=-1)
        transformed_pts = self.projection.transform_points(
            t, coords[..., 0], coords[..., 1])

        # Compute the length of diagonals in transformed coordinates
        # and create a mask where the wrapped cells are of shape (Ny-1, Nx-1)
        with np.errstate(invalid='ignore'):
            xs, ys = transformed_pts[..., 0], transformed_pts[..., 1]
            diagonal0_lengths = np.hypot(xs[1:, 1:] - xs[:-1, :-1],
                                         ys[1:, 1:] - ys[:-1, :-1])
            diagonal1_lengths = np.hypot(xs[1:, :-1] - xs[:-1, 1:],
                                         ys[1:, :-1] - ys[:-1, 1:])
            # The maximum size of the diagonal of any cell, defined to
            # be the projection width divided by 2*sqrt(2)
            # TODO: Make this dependent on the boundary of the
            #       projection which will help with curved boundaries
            size_limit = (abs(self.projection.x_limits[1] -
                              self.projection.x_limits[0]) /
                          (2 * np.sqrt(2)))
            mask = (np.isnan(diagonal0_lengths) |
                    (diagonal0_lengths > size_limit) |
                    np.isnan(diagonal1_lengths) |
                    (diagonal1_lengths > size_limit))

        # Update the data limits based on the corners of the mesh
        # in transformed coordinates, ignoring nan values
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', 'All-NaN slice encountered')
            # If we have all nans, that is OK and will be handled by the
            # Bbox calculations later, so suppress that warning from the user
            corners = ((np.nanmin(xs), np.nanmin(ys)),
                       (np.nanmax(xs), np.nanmax(ys)))
        collection._corners = mtransforms.Bbox(corners)
        self.update_datalim(collection._corners)

        # We need to keep the transform/projection check after
        # update_datalim to make sure we are getting the proper
        # datalims on the returned collection
        if (not (getattr(t, '_wrappable', False) and
                 getattr(self.projection, '_wrappable', False)) or
                not np.any(mask)):
            # If both projections are unwrappable
            # or if there aren't any points to wrap
            return collection

        # Wrapping with gouraud shading is error-prone. We will do our best,
        # but pcolor does not handle gouraud shading, so there needs to be
        # another way to handle the wrapped cells.
        if kwargs.get('shading') == 'gouraud':
            warnings.warn("Handling wrapped coordinates with gouraud "
                          "shading is likely to introduce artifacts. "
                          "It is recommended to remove the wrap manually "
                          "before calling pcolormesh.")
            # With gouraud shading, we actually want an (Ny, Nx) shaped mask
            gmask = np.zeros((data_shape[0], data_shape[1]), dtype=bool)
            # If any of the cells were wrapped, apply it to all 4 corners
            gmask[:-1, :-1] |= mask
            gmask[1:, :-1] |= mask
            gmask[1:, 1:] |= mask
            gmask[:-1, 1:] |= mask
            mask = gmask

        # We have quadrilaterals that cross the wrap boundary
        # Now, we need to update the original collection with
        # a mask over those cells and use pcolor to draw those
        # cells instead, which will properly handle the wrap.

        if collection.get_cmap()._rgba_bad[3] != 0.0:
            warnings.warn("The colormap's 'bad' has been set, but "
                          "in order to wrap pcolormesh across the "
                          "map it must be fully transparent.",
                          stacklevel=3)

        # Get hold of masked versions of the array to be passed to set_array
        # methods of QuadMesh and PolyQuadMesh
        pcolormesh_data, pcolor_data, pcolor_mask = \
            cartopy.mpl.geocollection._split_wrapped_mesh_data(C, mask)

        collection.set_array(pcolormesh_data)

        # plot with slightly lower zorder to avoid odd issue
        # where the main plot is obscured
        zorder = collection.zorder - .1
        kwargs.pop('zorder', None)
        kwargs.pop('shading', None)
        kwargs.setdefault('snap', False)
        vmin = kwargs.pop('vmin', None)
        vmax = kwargs.pop('vmax', None)
        norm = kwargs.pop('norm', None)
        cmap = kwargs.pop('cmap', None)
        # Plot all of the wrapped cells.
        # `pcolor` only draws polygons where the data is not
        # masked, so this will only draw a limited subset of
        # polygons that were actually wrapped.

        if not _MPL_38:
            # We will add the original data mask in later to
            # make sure that set_array can work in future
            # calls on the proper sized array inputs.
            # NOTE: we don't use C.data here because C.data could
            #       contain nan's which would be masked in the
            #       pcolor routines, which we don't want. We will
            #       fill in the proper data later with set_array()
            #       calls.
            pcolor_zeros = np.ma.array(np.zeros(C.shape), mask=pcolor_mask)
            pcolor_col = self.pcolor(coords[..., 0], coords[..., 1],
                                     pcolor_zeros, zorder=zorder,
                                     **kwargs)

            # The pcolor_col is now possibly shorter than the
            # actual collection, so grab the masked cells
            pcolor_col.set_array(pcolor_data[mask].ravel())
        else:
            pcolor_col = self.pcolor(coords[..., 0], coords[..., 1],
                                     pcolor_data, zorder=zorder,
                                     **kwargs)
            # Currently pcolor_col.get_array() will return a compressed array
            # and warn unless we explicitly set the 2D array.  This should be
            # unnecessary with future matplotlib versions.
            pcolor_col.set_array(pcolor_data)

        pcolor_col.set_cmap(cmap)
        pcolor_col.set_norm(norm)
        pcolor_col.set_clim(vmin, vmax)
        # scale the data according to the *original* data
        pcolor_col.norm.autoscale_None(C)

        # put the pcolor_col and mask on the pcolormesh
        # collection so that users can do things post
        # this method
        collection._wrapped_mask = mask
        collection._wrapped_collection_fix = pcolor_col

        return collection

    @_add_transform
    def pcolor(self, *args, **kwargs):
        """
        Add the "transform" keyword to :func:`~matplotlib.pyplot.pcolor`.

        Other Parameters
        ----------------
        transform
            A :class:`~cartopy.crs.Projection`.

        """
        # Add in an argument checker to handle Matplotlib's potential
        # interpolation when coordinate wraps are involved
        args, kwargs = self._wrap_args(*args, **kwargs)
        result = super().pcolor(*args, **kwargs)

        # Update the datalim for this pcolor.
        limits = result.get_datalim(self.transData)
        self.update_datalim(limits)

        self.autoscale_view()
        return result

    @_add_transform
    def quiver(self, x, y, u, v, *args, **kwargs):
        """
        Plot a field of arrows.

        Parameters
        ----------
        x
            An array containing the x-positions of data points.
        y
            An array containing the y-positions of data points.
        u
            An array of vector data in the u-direction.
        v
            An array of vector data in the v-direction.

        Other Parameters
        ----------------
        transform: :class:`cartopy.crs.Projection` or Matplotlib transform
            The coordinate system in which the vectors are defined.
        regrid_shape: int or 2-tuple of ints
            If given, specifies that the points where the arrows are
            located will be interpolated onto a regular grid in
            projection space. If a single integer is given then that
            will be used as the minimum grid length dimension, while the
            other dimension will be scaled up according to the target
            extent's aspect ratio. If a pair of ints are given they
            determine the grid length in the x and y directions
            respectively.
        target_extent: 4-tuple
            If given, specifies the extent in the target CRS that the
            regular grid defined by *regrid_shape* will have. Defaults
            to the current extent of the map projection.


        See :func:`matplotlib.pyplot.quiver` for details on arguments
        and other keyword arguments.

        Note
        ----
            The vector components must be defined as grid eastward and
            grid northward.

        """
        t = kwargs['transform']
        regrid_shape = kwargs.pop('regrid_shape', None)
        target_extent = kwargs.pop('target_extent',
                                   self.get_extent(self.projection))
        if regrid_shape is not None:
            # If regridding is required then we'll be handling transforms
            # manually and plotting in native coordinates.
            regrid_shape = self._regrid_shape_aspect(regrid_shape,
                                                     target_extent)
            # Lazy load vector_scalar_to_grid due to the optional
            # scipy dependency
            from cartopy.vector_transform import vector_scalar_to_grid
            if args:
                # Interpolate color array as well as vector components.
                x, y, u, v, c = vector_scalar_to_grid(
                    t, self.projection, regrid_shape, x, y, u, v, args[0],
                    target_extent=target_extent)
                args = (c,) + args[1:]
            else:
                x, y, u, v = vector_scalar_to_grid(
                    t, self.projection, regrid_shape, x, y, u, v,
                    target_extent=target_extent)
            kwargs.pop('transform', None)
        elif t != self.projection:
            # Transform the vectors if the projection is not the same as the
            # data transform.
            if (x.ndim == 1 and y.ndim == 1) and (x.shape != u.shape):
                x, y = np.meshgrid(x, y)
            u, v = self.projection.transform_vectors(t, x, y, u, v)
        return super().quiver(x, y, u, v, *args, **kwargs)

    @_add_transform
    def barbs(self, x, y, u, v, *args, **kwargs):
        """
        Plot a field of barbs.

        Parameters
        ----------
        x
            An array containing the x-positions of data points.
        y
            An array containing the y-positions of data points.
        u
            An array of vector data in the u-direction.
        v
            An array of vector data in the v-direction.

        Other Parameters
        ----------------
        transform: :class:`cartopy.crs.Projection` or Matplotlib transform
            The coordinate system in which the vectors are defined.
        regrid_shape: int or 2-tuple of ints
            If given, specifies that the points where the barbs are
            located will be interpolated onto a regular grid in
            projection space. If a single integer is given then that
            will be used as the minimum grid length dimension, while the
            other dimension will be scaled up according to the target
            extent's aspect ratio. If a pair of ints are given they
            determine the grid length in the x and y directions
            respectively.
        target_extent: 4-tuple
            If given, specifies the extent in the target CRS that the
            regular grid defined by *regrid_shape* will have. Defaults
            to the current extent of the map projection.


        See :func:`matplotlib.pyplot.barbs` for details on arguments
        and other keyword arguments.

        Note
        ----
            The vector components must be defined as grid eastward and
            grid northward.

        """
        t = kwargs['transform']
        regrid_shape = kwargs.pop('regrid_shape', None)
        target_extent = kwargs.pop('target_extent',
                                   self.get_extent(self.projection))
        if regrid_shape is not None:
            # If regridding is required then we'll be handling transforms
            # manually and plotting in native coordinates.
            regrid_shape = self._regrid_shape_aspect(regrid_shape,
                                                     target_extent)
            # Lazy load vector_scalar_to_grid due to the optional
            # scipy dependency
            from cartopy.vector_transform import vector_scalar_to_grid
            if args:
                # Interpolate color array as well as vector components.
                x, y, u, v, c = vector_scalar_to_grid(
                    t, self.projection, regrid_shape, x, y, u, v, args[0],
                    target_extent=target_extent)
                args = (c,) + args[1:]
            else:
                x, y, u, v = vector_scalar_to_grid(
                    t, self.projection, regrid_shape, x, y, u, v,
                    target_extent=target_extent)
            kwargs.pop('transform', None)
        elif t != self.projection:
            # Transform the vectors if the projection is not the same as the
            # data transform.
            if (x.ndim == 1 and y.ndim == 1) and (x.shape != u.shape):
                x, y = np.meshgrid(x, y)
            u, v = self.projection.transform_vectors(t, x, y, u, v)
        return super().barbs(x, y, u, v, *args, **kwargs)

    @_add_transform
    def streamplot(self, x, y, u, v, **kwargs):
        """
        Plot streamlines of a vector flow.

        Parameters
        ----------
        x
            An array containing the x-positions of data points.
        y
            An array containing the y-positions of data points.
        u
            An array of vector data in the u-direction.
        v
            An array of vector data in the v-direction.

        Other Parameters
        ----------------
        transform: :class:`cartopy.crs.Projection` or Matplotlib transform.
            The coordinate system in which the vector field is defined.


        See :func:`matplotlib.pyplot.streamplot` for details on arguments
        and keyword arguments.

        Note
        ----
            The vector components must be defined as grid eastward and
            grid northward.

        """
        t = kwargs.pop('transform')
        # Regridding is required for streamplot, it must have an evenly spaced
        # grid to work correctly. Choose our destination grid based on the
        # density keyword. The grid need not be bigger than the grid used by
        # the streamplot integrator.
        density = kwargs.get('density', 1)
        if np.isscalar(density):
            regrid_shape = [int(30 * density)] * 2
        else:
            regrid_shape = [int(25 * d) for d in density]
        # The color and linewidth keyword arguments can be arrays so they will
        # need to be gridded also.
        col = kwargs.get('color', None)
        lw = kwargs.get('linewidth', None)
        scalars = []
        color_array = isinstance(col, np.ndarray)
        linewidth_array = isinstance(lw, np.ndarray)
        if color_array:
            scalars.append(col)
        if linewidth_array:
            scalars.append(lw)
        # Do the regridding including any scalar fields.
        target_extent = self.get_extent(self.projection)
        # Lazy load vector_scalar_to_grid due to the optional
        # scipy dependency
        from cartopy.vector_transform import vector_scalar_to_grid
        gridded = vector_scalar_to_grid(t, self.projection, regrid_shape,
                                        x, y, u, v, *scalars,
                                        target_extent=target_extent)
        x, y, u, v = gridded[:4]
        # If scalar fields were regridded then replace the appropriate keyword
        # arguments with the gridded arrays.
        scalars = list(gridded[4:])
        if linewidth_array:
            kwargs['linewidth'] = scalars.pop()
        if color_array:
            kwargs['color'] = ma.masked_invalid(scalars.pop())
        with warnings.catch_warnings():
            # The workaround for nan values in streamplot colors gives rise to
            # a warning which is not at all important so it is hidden from the
            # user to avoid confusion.
            message = 'Warning: converting a masked element to nan.'
            warnings.filterwarnings('ignore', message=message,
                                    category=UserWarning)
            sp = super().streamplot(x, y, u, v, **kwargs)
        return sp

    def add_wmts(self, wmts, layer_name, wmts_kwargs=None, cache=False, **kwargs):
        """
        Add the specified WMTS layer to the axes.

        This function requires owslib and PIL to work.

        Parameters
        ----------
        wmts
            The URL of the WMTS, or an owslib.wmts.WebMapTileService instance.
        layer_name
            The name of the layer to use.
        wmts_kwargs: dict or None, optional
            Passed through to the
            :class:`~cartopy.io.ogc_clients.WMTSRasterSource` constructor's
            ``gettile_extra_kwargs`` (e.g. time).


        All other keywords are passed through to the construction of the
        image artist. See :meth:`~matplotlib.axes.Axes.imshow()` for
        more details.

        """
        from cartopy.io.ogc_clients import WMTSRasterSource
        wmts = WMTSRasterSource(wmts, layer_name,
                                gettile_extra_kwargs=wmts_kwargs, cache=cache)
        return self.add_raster(wmts, **kwargs)

    def add_wms(self, wms, layers, wms_kwargs=None, **kwargs):
        """
        Add the specified WMS layer to the axes.

        This function requires owslib and PIL to work.

        Parameters
        ----------
        wms: string or :class:`owslib.wms.WebMapService` instance
            The web map service URL or owslib WMS instance to use.
        layers: string or iterable of string
            The name of the layer(s) to use.
        wms_kwargs: dict or None, optional
            Passed through to the
            :class:`~cartopy.io.ogc_clients.WMSRasterSource`
            constructor's ``getmap_extra_kwargs`` for defining
            getmap time keyword arguments.


        All other keywords are passed through to the construction of the
        image artist. See :meth:`~matplotlib.axes.Axes.imshow()` for
        more details.

        """
        from cartopy.io.ogc_clients import WMSRasterSource
        wms = WMSRasterSource(wms, layers, getmap_extra_kwargs=wms_kwargs)
        return self.add_raster(wms, **kwargs)


# Define the GeoAxesSubplot class, so that a type(ax) will emanate from
# cartopy.mpl.geoaxes, not matplotlib.axes.
GeoAxesSubplot = matplotlib.axes.subplot_class_factory(GeoAxes)
GeoAxesSubplot.__module__ = GeoAxes.__module__


def _trigger_patch_reclip(axes):
    """
    Define an event callback for a GeoAxes which forces the background patch to
    be re-clipped next time it is drawn.

    """
    # trigger the outline and background patches to be re-clipped
    axes.spines['geo'].stale = True
    axes.patch.stale = True
