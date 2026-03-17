# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""
Cartopy can produce gridlines and ticks in any projection and add
them to the current geoaxes projection, providing a way to add detailed
location information to the plots.

"""

import inspect
import itertools
import operator
import warnings

import matplotlib
import matplotlib.artist
import matplotlib.collections as mcollections
import matplotlib.text
import matplotlib.ticker as mticker
import matplotlib.transforms as mtrans
import numpy as np
import shapely
import shapely.geometry as sgeom

import cartopy
from cartopy.crs import PlateCarree, Projection, _RectangularProjection
from cartopy.mpl.ticker import (
    LatitudeFormatter,
    LatitudeLocator,
    LongitudeFormatter,
    LongitudeLocator,
)


degree_locator = mticker.MaxNLocator(nbins=9, steps=[1, 1.5, 1.8, 2, 3, 6, 10])
classic_locator = mticker.MaxNLocator(nbins=9)
classic_formatter = mticker.ScalarFormatter

_X_INLINE_PROJS = (
    cartopy.crs.InterruptedGoodeHomolosine,
    cartopy.crs.LambertConformal,
    cartopy.crs.Mollweide,
    cartopy.crs.Sinusoidal,
    cartopy.crs.RotatedPole,
)
_POLAR_PROJS = (
    cartopy.crs.NorthPolarStereo,
    cartopy.crs.SouthPolarStereo,
    cartopy.crs.Stereographic
)
_ROTATE_LABEL_PROJS = _POLAR_PROJS + (
    cartopy.crs.AlbersEqualArea,
    cartopy.crs.AzimuthalEquidistant,
    cartopy.crs.EquidistantConic,
    cartopy.crs.LambertConformal,
    cartopy.crs.TransverseMercator,
    cartopy.crs.Gnomonic,
    cartopy.crs.ObliqueMercator,
)


def _lon_hemisphere(longitude):
    """Return the hemisphere (E, W or '' for 0) for the given longitude."""
    # Wrap the longitude to the range -180 to 180, keeping positive 180s
    lon_wrapped = ((longitude + 180) % 360) - 180
    longitude = 180 if (longitude > 0 and lon_wrapped == -180) else lon_wrapped
    if longitude > 0:
        hemisphere = 'E'
    elif longitude < 0:
        hemisphere = 'W'
    else:
        hemisphere = ''
    return hemisphere


def _lat_hemisphere(latitude):
    """Return the hemisphere (N, S or '' for 0) for the given latitude."""
    if latitude > 0:
        hemisphere = 'N'
    elif latitude < 0:
        hemisphere = 'S'
    else:
        hemisphere = ''
    return hemisphere


def _east_west_formatted(longitude, num_format='g'):
    hemisphere = _lon_hemisphere(longitude)
    return f'{abs(longitude):{num_format}}\N{Degree Sign}{hemisphere}'


def _north_south_formatted(latitude, num_format='g'):
    hemisphere = _lat_hemisphere(latitude)
    return f'{abs(latitude):{num_format}}\N{Degree Sign}{hemisphere}'


#: A formatter which turns longitude values into nice longitudes such as 110W
LONGITUDE_FORMATTER = mticker.FuncFormatter(lambda v, pos:
                                            _east_west_formatted(v))
#: A formatter which turns longitude values into nice longitudes such as 45S
LATITUDE_FORMATTER = mticker.FuncFormatter(lambda v, pos:
                                           _north_south_formatted(v))


class Gridliner(matplotlib.artist.Artist):
    def __init__(self, axes, crs, draw_labels=False, xlocator=None,
                 ylocator=None, collection_kwargs=None,
                 xformatter=None, yformatter=None, dms=False,
                 x_inline=None, y_inline=None, auto_inline=True,
                 xlim=None, ylim=None, rotate_labels=None,
                 xlabel_style=None, ylabel_style=None, labels_bbox_style=None,
                 xpadding=5, ypadding=5, offset_angle=25,
                 auto_update=None, formatter_kwargs=None):
        """
        Artist used by :meth:`cartopy.mpl.geoaxes.GeoAxes.gridlines`
        to add gridlines and tick labels to a map.

        Parameters
        ----------
        axes
            The :class:`cartopy.mpl.geoaxes.GeoAxes` object to be drawn on.
        crs
            The :class:`cartopy.crs.CRS` defining the coordinate system that
            the gridlines are drawn in.
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
            and top/bottom edges respectively, unless explicitly requested.

        xlocator: optional
            A :class:`matplotlib.ticker.Locator` instance which will be used
            to determine the locations of the gridlines in the x-coordinate of
            the given CRS. Defaults to None, which implies automatic locating
            of the gridlines.
        ylocator: optional
            A :class:`matplotlib.ticker.Locator` instance which will be used
            to determine the locations of the gridlines in the y-coordinate of
            the given CRS. Defaults to None, which implies automatic locating
            of the gridlines.
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
        collection_kwargs: optional
            Dictionary controlling line properties, passed to
            :class:`matplotlib.collections.Collection`. Defaults to None.
        dms: bool
            When default locators and formatters are used,
            ticks are able to stop on minutes and seconds if minutes
            is set to True, and not fraction of degrees.
        x_inline: optional
            Toggle whether the x labels drawn should be inline.
        y_inline: optional
            Toggle whether the y labels drawn should be inline.
        auto_inline: optional
            Set x_inline and y_inline automatically based on projection.
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
            bbox style for all text labels
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
            Whether to redraw the gridlines and labels when the figure is
            updated.

            .. deprecated:: 0.23
               In future the gridlines and labels will always be redrawn.

        formatter_kwargs: dict, optional
            Options passed to the default formatters.
            See :class:`~cartopy.mpl.ticker.LongitudeFormatter` and
            :class:`~cartopy.mpl.ticker.LatitudeFormatter`

        Notes
        -----
        The "x" and "y" labels for locators and formatters do not necessarily
        correspond to X and Y, but to the first and second coordinates of the
        specified CRS. For the common case of PlateCarree gridlines, these
        correspond to longitudes and latitudes. Depending on the projection
        used for the map, meridians and parallels can cross both the X axis and
        the Y axis.
        """
        super().__init__()

        # We do not want the labels clipped to axes.
        self.set_clip_on(False)
        # Backcompat: the LineCollection was previously added directly to the
        # axes, having a default zorder of 2.
        self.set_zorder(2)

        #: The :class:`~matplotlib.ticker.Locator` to use for the x
        #: gridlines and labels.
        if xlocator is not None:
            if not isinstance(xlocator, mticker.Locator):
                xlocator = mticker.FixedLocator(xlocator)
            self.xlocator = xlocator
        elif isinstance(crs, PlateCarree):
            self.xlocator = LongitudeLocator(dms=dms)
        else:
            self.xlocator = classic_locator

        #: The :class:`~matplotlib.ticker.Locator` to use for the y
        #: gridlines and labels.
        if ylocator is not None:
            if not isinstance(ylocator, mticker.Locator):
                ylocator = mticker.FixedLocator(ylocator)
            self.ylocator = ylocator
        elif isinstance(crs, PlateCarree):
            self.ylocator = LatitudeLocator(dms=dms)
        else:
            self.ylocator = classic_locator

        formatter_kwargs = {
            **(formatter_kwargs or {}),
            "dms": dms,
        }

        if xformatter is None:
            if isinstance(crs, PlateCarree):
                xformatter = LongitudeFormatter(**formatter_kwargs)
            else:
                xformatter = classic_formatter()
        #: The :class:`~matplotlib.ticker.Formatter` to use for the lon labels.
        self.xformatter = xformatter

        if yformatter is None:
            if isinstance(crs, PlateCarree):
                yformatter = LatitudeFormatter(**formatter_kwargs)
            else:
                yformatter = classic_formatter()
        #: The :class:`~matplotlib.ticker.Formatter` to use for the lat labels.
        self.yformatter = yformatter

        # Draw label argument
        if isinstance(draw_labels, list):

            # Select to which coordinate it is applied
            if 'x' not in draw_labels and 'y' not in draw_labels:
                value = True
            elif 'x' in draw_labels and 'y' in draw_labels:
                value = ['x', 'y']
            elif 'x' in draw_labels:
                value = 'x'
            else:
                value = 'y'

            #: Whether to draw labels on the top of the map.
            self.top_labels = value if 'top' in draw_labels else False

            #: Whether to draw labels on the bottom of the map.
            self.bottom_labels = value if 'bottom' in draw_labels else False

            #: Whether to draw labels on the left hand side of the map.
            self.left_labels = value if 'left' in draw_labels else False

            #: Whether to draw labels on the right hand side of the map.
            self.right_labels = value if 'right' in draw_labels else False

            #: Whether to draw labels near the geographic limits of the map.
            self.geo_labels = value if 'geo' in draw_labels else False

        elif isinstance(draw_labels, dict):

            self.top_labels = draw_labels.get('top', False)
            self.bottom_labels = draw_labels.get('bottom', False)
            self.left_labels = draw_labels.get('left', False)
            self.right_labels = draw_labels.get('right', False)
            self.geo_labels = draw_labels.get('geo', False)

        else:

            self.top_labels = draw_labels
            self.bottom_labels = draw_labels
            self.left_labels = draw_labels
            self.right_labels = draw_labels
            self.geo_labels = draw_labels

        for loc in 'top', 'bottom', 'left', 'right', 'geo':
            value = getattr(self, f'{loc}_labels')
            if isinstance(value, str):
                value = value.lower()
            if (not isinstance(value, (list, bool)) and
                    value not in ('x', 'y')):
                raise ValueError(f"Invalid draw_labels argument: {value}")

        if auto_inline:
            if isinstance(axes.projection, _X_INLINE_PROJS):
                self.x_inline = True
                self.y_inline = False
            elif isinstance(axes.projection, _POLAR_PROJS):
                self.x_inline = False
                self.y_inline = True
            else:
                self.x_inline = False
                self.y_inline = False

        # overwrite auto_inline if necessary
        if x_inline is not None:
            #: Whether to draw x labels inline
            self.x_inline = x_inline
        elif not auto_inline:
            self.x_inline = False

        if y_inline is not None:
            #: Whether to draw y labels inline
            self.y_inline = y_inline
        elif not auto_inline:
            self.y_inline = False

        # Apply inline args
        if not draw_labels:
            self.inline_labels = False
        elif self.x_inline and self.y_inline:
            self.inline_labels = True
        elif self.x_inline:
            self.inline_labels = "x"
        elif self.y_inline:
            self.inline_labels = "y"
        else:
            self.inline_labels = False

        # Gridline limits so that the gridlines don't extend all the way
        # to the edge of the boundary
        self.xlim = xlim
        self.ylim = ylim

        #: Whether to draw the x gridlines.
        self.xlines = True

        #: Whether to draw the y gridlines.
        self.ylines = True

        #: A dictionary passed through to ``ax.text`` on x label creation
        #: for styling of the text labels.
        self.xlabel_style = xlabel_style or {}

        #: A dictionary passed through to ``ax.text`` on y label creation
        #: for styling of the text labels.
        self.ylabel_style = ylabel_style or {}

        #: bbox style for grid labels
        self.labels_bbox_style = (
            labels_bbox_style or {'pad': 0, 'visible': False})

        #: The padding from the map edge to the x labels in points.
        self.xpadding = xpadding

        #: The padding from the map edge to the y labels in points.
        self.ypadding = ypadding

        #: Control the rotation of labels.
        if rotate_labels is None:
            rotate_labels = (
                axes.projection.__class__ in _ROTATE_LABEL_PROJS)
        if not isinstance(rotate_labels, (bool, float, int)):
            raise ValueError("Invalid rotate_labels argument")
        self.rotate_labels = rotate_labels

        self.offset_angle = offset_angle

        # Current transform
        self.crs = crs

        # if the user specifies tick labels at this point, check if they can
        # be drawn. The same check will take place at draw time in case
        # public attributes are changed after instantiation.
        if draw_labels and not (x_inline or y_inline or auto_inline):
            self._assert_can_draw_ticks()

        #: The number of interpolation points which are used to draw the
        #: gridlines.
        self.n_steps = 100

        #: A dictionary passed through to
        #: ``matplotlib.collections.LineCollection`` on grid line creation.
        self.collection_kwargs = collection_kwargs

        #: The x gridlines which were created at draw time.
        self.xline_artists = []

        #: The y gridlines which were created at draw time.
        self.yline_artists = []

        # List of all labels (Label objects)
        self._all_labels = []

        # List of active labels (used in current draw)
        self._labels = []

        # Draw status
        self._drawn = False
        if auto_update is None:
            auto_update = True
        else:
            # Note #2394 should be addressed before this deprecation expires.
            calling_module = inspect.stack()[1].filename
            warnings.warn(
                "The auto_update parameter was deprecated at Cartopy 0.23.  In future "
                "the gridlines and labels will always be updated.",
                DeprecationWarning,
                stacklevel=(3 if calling_module.endswith('cartopy/mpl/geoaxes.py')
                            else 2))
        self._auto_update = auto_update

    def has_labels(self):
        return len(self._labels) != 0

    @property
    def label_artists(self):
        """All the labels which were created at draw time"""
        return [label.artist for label in self._labels]

    @property
    def top_label_artists(self):
        """The top labels which were created at draw time"""
        return [label.artist for label in self._labels
                if label.loc == "top"]

    @property
    def bottom_label_artists(self):
        """The bottom labels which were created at draw time"""
        return [label.artist for label in self._labels
                if label.loc == "bottom"]

    @property
    def left_label_artists(self):
        """The left labels which were created at draw time"""
        return [label.artist for label in self._labels
                if label.loc == "left"]

    @property
    def right_label_artists(self):
        """The right labels which were created at draw time"""
        return [label.artist for label in self._labels
                if label.loc == "right"]

    @property
    def geo_label_artists(self):
        """The geo spine labels which were created at draw time"""
        return [label.artist for label in self._labels
                if label.loc == "geo"]

    @property
    def x_inline_label_artists(self):
        """The x-coordinate inline labels which were created at draw time"""
        return [label.artist for label in self._labels
                if label.loc == "x_inline"]

    @property
    def y_inline_label_artists(self):
        """The y-coordinate inline labels which were created at draw time"""
        return [label.artist for label in self._labels
                if label.loc == "y_inline"]

    @property
    def xlabel_artists(self):
        """The x-coordinate labels which were created at draw time"""
        return [label.artist for label in self._labels
                if label.xy == "x"]

    @property
    def ylabel_artists(self):
        """The y-coordinate labels which were created at draw time"""
        return [label.artist for label in self._labels
                if label.xy == "y"]

    def _crs_transform(self):
        """
        Get the drawing transform for our gridlines.

        Note
        ----
            The drawing transform depends on the transform of our 'axes', so
            it may change dynamically.

        """
        transform = self.crs
        if not isinstance(transform, mtrans.Transform):
            transform = transform._as_mpl_transform(self.axes)
        return transform

    @staticmethod
    def _round(x, base=5):
        if np.isnan(base):
            base = 5
        return int(base * round(x / base))

    def _find_midpoints(self, lim, ticks):
        # Find the center point between each lat gridline.
        if len(ticks) > 1:
            cent = np.diff(ticks).mean() / 2
        else:
            cent = np.nan
        if isinstance(self.axes.projection, _POLAR_PROJS):
            lq = 90
            uq = 90
        else:
            lq = 25
            uq = 75
        midpoints = (self._round(np.percentile(lim, lq), cent),
                     self._round(np.percentile(lim, uq), cent))
        return midpoints

    def _draw_this_label(self, xylabel, loc):
        """Should I draw this kind of label here?"""
        draw_labels = getattr(self, loc + '_labels')

        # By default, only x on top/bottom and only y on left/right
        if draw_labels is True and loc != 'geo':
            draw_labels = "x" if loc in ["top", "bottom"] else "y"

        # Don't draw
        if not draw_labels:
            return False

        # Explicit x or y
        if isinstance(draw_labels, str):
            draw_labels = [draw_labels]

        # Explicit list of x and/or y
        if isinstance(draw_labels, list) and xylabel not in draw_labels:
            return False

        return True

    def _generate_labels(self):
        """
        A generator to yield as many labels as needed, reusing existing ones
        where possible.
        """
        for label in self._all_labels:
            yield label

        while True:
            # Ran out of existing labels.  Create some empty ones.
            new_artist = matplotlib.text.Text()
            new_artist.set_figure(self.axes.figure)
            new_artist.axes = self.axes

            new_label = Label(new_artist, None, None, None)
            self._all_labels.append(new_label)

            yield new_label

    def _draw_gridliner(self, nx=None, ny=None, renderer=None):
        """Create Artists for all visible elements and add to our Axes.

        The following rules apply for the visibility of labels:

        - X-type labels are plotted along the bottom, top and geo spines.
        - Y-type labels are plotted along the left, right and geo spines.
        - A label must not overlap another label marked as visible.
        - A label must not overlap the map boundary.
        - When a label is about to be hidden, its padding is slightly
          increase until it can be drawn or until a padding limit is reached.
        """
        # Update only when needed or requested
        if self._drawn and not self._auto_update:
            return
        self._drawn = True

        # Inits
        lon_lim, lat_lim = self._axes_domain(nx=nx, ny=ny)
        transform = self._crs_transform()
        n_steps = self.n_steps
        crs = self.crs

        # Get nice ticks within crs domain
        lon_ticks = self.xlocator.tick_values(lon_lim[0], lon_lim[1])
        lat_ticks = self.ylocator.tick_values(lat_lim[0], lat_lim[1])

        inf = max(lon_lim[0], crs.x_limits[0])
        sup = min(lon_lim[1], crs.x_limits[1])
        lon_ticks = [value for value in lon_ticks if inf <= value <= sup]
        inf = max(lat_lim[0], crs.y_limits[0])
        sup = min(lat_lim[1], crs.y_limits[1])
        lat_ticks = [value for value in lat_ticks if inf <= value <= sup]

        #####################
        # Gridlines drawing #
        #####################

        collection_kwargs = self.collection_kwargs
        if collection_kwargs is None:
            collection_kwargs = {}
        collection_kwargs = collection_kwargs.copy()
        collection_kwargs['transform'] = transform
        if not any(x in collection_kwargs for x in ['c', 'color']):
            collection_kwargs.setdefault('color',
                                         matplotlib.rcParams['grid.color'])
        if not any(x in collection_kwargs for x in ['ls', 'linestyle']):
            collection_kwargs.setdefault('linestyle',
                                         matplotlib.rcParams['grid.linestyle'])
        if not any(x in collection_kwargs for x in ['lw', 'linewidth']):
            collection_kwargs.setdefault('linewidth',
                                         matplotlib.rcParams['grid.linewidth'])
        collection_kwargs.setdefault('clip_path', self.axes.patch)

        # Meridians
        lat_min, lat_max = lat_lim
        if lat_ticks:
            lat_min = min(lat_min, min(lat_ticks))
            lat_max = max(lat_max, max(lat_ticks))
        lon_lines = np.empty((len(lon_ticks), n_steps, 2))
        lon_lines[:, :, 0] = np.array(lon_ticks)[:, np.newaxis]
        lon_lines[:, :, 1] = np.linspace(
            lat_min, lat_max, n_steps)[np.newaxis, :]

        if self.xlines:
            nx = len(lon_lines) + 1
            # XXX this bit is cartopy specific. (for circular longitudes)
            # Purpose: omit plotting the last x line,
            # as it may overlap the first.
            if (isinstance(crs, Projection) and
                    isinstance(crs, _RectangularProjection) and
                    abs(np.diff(lon_lim)) == abs(np.diff(crs.x_limits))):
                nx -= 1

            if self.xline_artists:
                # Update existing collection.
                lon_lc, = self.xline_artists
                lon_lc.set(segments=lon_lines, **collection_kwargs)
            else:
                # Create new collection.
                lon_lc = mcollections.LineCollection(lon_lines,
                                                     **collection_kwargs)
                self.xline_artists.append(lon_lc)

        # Parallels
        lon_min, lon_max = lon_lim
        if lon_ticks:
            lon_min = min(lon_min, min(lon_ticks))
            lon_max = max(lon_max, max(lon_ticks))
        lat_lines = np.empty((len(lat_ticks), n_steps, 2))
        lat_lines[:, :, 0] = np.linspace(lon_min, lon_max,
                                         n_steps)[np.newaxis, :]
        lat_lines[:, :, 1] = np.array(lat_ticks)[:, np.newaxis]
        if self.ylines:
            if self.yline_artists:
                # Update existing collection.
                lat_lc, = self.yline_artists
                lat_lc.set(segments=lat_lines, **collection_kwargs)
            else:
                lat_lc = mcollections.LineCollection(lat_lines,
                                                     **collection_kwargs)
                self.yline_artists.append(lat_lc)

        #################
        # Label drawing #
        #################

        # Clear drawn labels
        self._labels.clear()

        if not any((self.left_labels, self.right_labels, self.bottom_labels,
                    self.top_labels, self.inline_labels, self.geo_labels)):
            return
        self._assert_can_draw_ticks()

        # Inits for labels
        max_padding_factor = 5
        delta_padding_factor = 0.2
        spines_specs = {
            'left': {
                'index': 0,
                'coord_type': "x",
                'opcmp': operator.le,
                'opval': max,
            },
            'bottom': {
                'index': 1,
                'coord_type': "y",
                'opcmp': operator.le,
                'opval': max,
            },
            'right': {
                'index': 0,
                'coord_type': "x",
                'opcmp': operator.ge,
                'opval': min,
            },
            'top': {
                'index': 1,
                'coord_type': "y",
                'opcmp': operator.ge,
                'opval': min,
            },
        }
        for side, specs in spines_specs.items():
            bbox = self.axes.spines[side].get_window_extent(renderer)
            specs['coords'] = [
                getattr(bbox, specs['coord_type'] + idx) for idx in "01"]

        def update_artist(artist, renderer):
            artist.update_bbox_position_size(renderer)
            this_patch = artist.get_bbox_patch()
            this_path = this_patch.get_path().transformed(
                this_patch.get_transform())
            return this_path

        # Get the real map boundaries
        self.axes.spines["geo"].get_window_extent(renderer)  # update coords
        map_boundary_path = self.axes.spines["geo"].get_path().transformed(
            self.axes.spines["geo"].get_transform())
        map_boundary = sgeom.Polygon(map_boundary_path.vertices)

        if self.x_inline:
            y_midpoints = self._find_midpoints(lat_lim, lat_ticks)
        if self.y_inline:
            x_midpoints = self._find_midpoints(lon_lim, lon_ticks)

        # Cache a few things so they aren't re-calculated in the loops.
        crs_transform = self._crs_transform().transform
        inverse_data_transform = self.axes.transData.inverted().transform_point

        # Create a generator for the Label objects.
        generate_labels = self._generate_labels()

        for xylabel, lines, line_ticks, formatter, label_style in (
                ('x', lon_lines, lon_ticks,
                 self.xformatter, self.xlabel_style.copy()),
                ('y', lat_lines, lat_ticks,
                 self.yformatter, self.ylabel_style.copy())):

            x_inline = self.x_inline and xylabel == 'x'
            y_inline = self.y_inline and xylabel == 'y'
            padding = getattr(self, f'{xylabel}padding')
            bbox_style = self.labels_bbox_style.copy()
            if "bbox" in label_style:
                bbox_style.update(label_style["bbox"])
            label_style["bbox"] = bbox_style

            formatter.set_locs(line_ticks)

            for line_coords, tick_value in zip(lines, line_ticks):
                # Intersection of line with map boundary
                line_coords = crs_transform(line_coords)
                infs = np.isnan(line_coords).any(axis=1)
                line_coords = line_coords.compress(~infs, axis=0)
                if line_coords.size == 0:
                    continue
                line = sgeom.LineString(line_coords)
                if not line.intersects(map_boundary):
                    continue
                intersection = line.intersection(map_boundary)
                del line
                if intersection.is_empty:
                    continue
                if isinstance(intersection, sgeom.MultiPoint):
                    if len(intersection) < 2:
                        continue
                    n2 = min(len(intersection), 3)
                    tails = [[(pt.x, pt.y)
                              for pt in intersection[:n2:n2 - 1]]]
                    heads = [[(pt.x, pt.y)
                              for pt in intersection[-1:-n2 - 1:-n2 + 1]]]
                elif isinstance(intersection, (sgeom.LineString,
                                               sgeom.MultiLineString)):
                    if isinstance(intersection, sgeom.LineString):
                        intersection = [intersection]
                    elif len(intersection.geoms) > 4:
                        # If lines are parallel, there will be many intersections
                        # merge them to get only one for the calculations below
                        merged_line = shapely.line_merge(intersection)
                        if isinstance(merged_line, sgeom.MultiLineString):
                            # our merge still produced a multilinestring, so
                            # manually concatenate the original coordinates
                            xy = np.concatenate(
                                [inter.coords for inter in intersection.geoms], axis=0)
                            merged_line = shapely.LineString(xy)
                        intersection = [merged_line]
                    else:
                        intersection = intersection.geoms
                    tails = []
                    heads = []
                    for inter in intersection:
                        if len(inter.coords) < 2:
                            continue
                        n2 = min(len(inter.coords), 8)
                        tails.append(inter.coords[:n2:n2 - 1])
                        heads.append(inter.coords[-1:-n2 - 1:-n2 + 1])
                    if not tails:
                        continue
                elif isinstance(intersection, sgeom.GeometryCollection):
                    # This is a collection of Point and LineString that
                    # represent the same gridline.  We only consider the first
                    # geometries, merge their coordinates and keep first two
                    # points to get only one tail ...
                    xy = []
                    for geom in intersection.geoms:
                        for coord in geom.coords:
                            xy.append(coord)
                            if len(xy) == 2:
                                break
                        if len(xy) == 2:
                            break
                    tails = [xy]
                    # ... and the last geometries, merge their coordinates and
                    # keep last two points to get only one head.
                    xy = []
                    for geom in reversed(intersection.geoms):
                        for coord in reversed(geom.coords):
                            xy.append(coord)
                            if len(xy) == 2:
                                break
                        if len(xy) == 2:
                            break
                    heads = [xy]
                else:
                    warnings.warn(
                        'Unsupported intersection geometry for gridline '
                        f'labels: {intersection.__class__.__name__}')
                    continue
                del intersection

                # Loop on head and tail and plot label by extrapolation
                for i, (pt0, pt1) in itertools.chain.from_iterable(
                        enumerate(pair) for pair in zip(tails, heads)):

                    # Initial text specs
                    x0, y0 = pt0
                    if x_inline or y_inline:
                        kw = {'rotation': 0, 'transform': self.crs,
                              'ha': 'center', 'va': 'center'}
                        loc = 'inline'
                    else:
                        x1, y1 = pt1
                        segment_angle = np.arctan2(y0 - y1,
                                                   x0 - x1) * 180 / np.pi
                        loc = self._get_loc_from_spine_intersection(
                            spines_specs, xylabel, x0, y0)
                        if not self._draw_this_label(xylabel, loc):
                            visible = False
                        kw = self._get_text_specs(segment_angle, loc, xylabel)
                        kw['transform'] = self._get_padding_transform(
                            segment_angle, loc, xylabel)
                    kw.update(label_style)

                    # Get x and y in data coords
                    pt0 = inverse_data_transform(pt0)
                    if y_inline:
                        # 180 degrees isn't formatted with a suffix and adds
                        # confusion if it's inline.
                        if abs(tick_value) == 180:
                            continue
                        x = x_midpoints[i]
                        y = tick_value
                        kw.update(clip_on=True)
                        y_set = True
                    else:
                        x = pt0[0]
                        y_set = False

                    if x_inline:
                        if abs(tick_value) == 180:
                            continue
                        x = tick_value
                        y = y_midpoints[i]
                        kw.update(clip_on=True)
                    elif not y_set:
                        y = pt0[1]

                    # Update generated label.
                    label = next(generate_labels)
                    text = formatter(tick_value)
                    artist = label.artist
                    artist.set(x=x, y=y, text=text, **kw)

                    # Update loc from spine overlapping now that we have a bbox
                    # of the label.
                    this_path = update_artist(artist, renderer)
                    if not x_inline and not y_inline and loc == 'geo':
                        new_loc = self._get_loc_from_spine_overlapping(
                            spines_specs, xylabel, this_path)
                        if new_loc and loc != new_loc:
                            loc = new_loc
                            transform = self._get_padding_transform(
                                segment_angle, loc, xylabel)
                            artist.set_transform(transform)
                            artist.update(
                                self._get_text_specs(
                                    segment_angle, loc, xylabel))
                            artist.update(label_style.copy())
                            this_path = update_artist(artist, renderer)

                    # Is this kind label allowed to be drawn?
                    if not self._draw_this_label(xylabel, loc):
                        visible = False

                    elif x_inline or y_inline:
                        # Check that it does not overlap the map.
                        # Inline must be within the map.
                        # TODO: When Matplotlib clip path works on text, this
                        # clipping can be left to it.
                        center = (artist
                                  .get_transform()
                                  .transform_point(artist.get_position()))
                        visible = map_boundary_path.contains_point(center)
                    else:
                        # Now loop on padding factors until it does not overlap
                        # the boundary.
                        visible = False
                        padding_factor = 1
                        while padding_factor < max_padding_factor:

                            # Non-inline must not run through the outline.
                            if map_boundary_path.intersects_path(
                                    this_path, filled=padding > 0):

                                # Apply new padding.
                                transform = self._get_padding_transform(
                                    segment_angle, loc, xylabel,
                                    padding_factor)
                                artist.set_transform(transform)
                                this_path = update_artist(artist, renderer)
                                padding_factor += delta_padding_factor

                            else:
                                visible = True
                                break

                    # Updates
                    label.set_visible(visible)
                    label.path = this_path
                    label.xy = xylabel
                    label.loc = loc
                    self._labels.append(label)

        # Now check overlapping of ordered visible labels
        if self._labels:
            self._labels.sort(
                key=operator.attrgetter("priority"), reverse=True)
            visible_labels = []
            for label in self._labels:
                if label.get_visible():
                    for other_label in visible_labels:
                        if label.check_overlapping(other_label):
                            break
                    else:
                        visible_labels.append(label)

    def _get_loc_from_angle(self, angle):
        angle %= 360
        if angle > 180:
            angle -= 360
        if abs(angle) <= 45:
            loc = 'right'
        elif abs(angle) >= 135:
            loc = 'left'
        elif angle > 45:
            loc = 'top'
        else:  # (-135, -45)
            loc = 'bottom'
        return loc

    def _get_loc_from_spine_overlapping(
            self, spines_specs, xylabel, label_path):
        """Try to get the location from side spines and label path

        Returns None if it does not apply

        For instance, for each side, if any of label_path x coordinates
        are beyond this side, the distance to this side is computed.
        If several sides are matching (max 2), then the one with a greater
        distance is kept.

        This helps finding the side of labels for non-rectangular projection
        with a rectangular map boundary.

        """
        side_max = dist_max = None
        for side, specs in spines_specs.items():
            if specs['coord_type'] == xylabel:
                continue

            label_coords = label_path.vertices[:-1, specs['index']]

            spine_coord = specs['opval'](specs['coords'])
            if not specs['opcmp'](label_coords, spine_coord).any():
                continue
            if specs['opcmp'] is operator.ge:  # top, right
                dist = label_coords.min() - spine_coord
            else:
                dist = spine_coord - label_coords.max()

            if side_max is None or dist > dist_max:
                side_max = side
                dist_max = dist
        if side_max is None:
            return "geo"
        return side_max

    def _get_loc_from_spine_intersection(self, spines_specs, xylabel, x, y):
        """Get the loc the intersection of a gridline with a spine

        Defaults to "geo".
        """
        if xylabel == "x":
            sides = ["bottom", "top", "left", "right"]
        else:
            sides = ["left", "right", "bottom", "top"]
        for side in sides:
            xy = x if side in ["left", "right"] else y
            coords = np.round(spines_specs[side]["coords"], 2)
            if round(xy, 2) in coords:
                return side
        return "geo"

    def _get_text_specs(self, angle, loc, xylabel):
        """Get rotation and alignments specs for a single label"""

        # Angle from -180 to 180
        if angle > 180:
            angle -= 360

        # Fake for geo spine
        if loc == "geo":
            loc = self._get_loc_from_angle(angle)

        # Check rotation
        if not self.rotate_labels:

            # No rotation
            kw = {'rotation': 0, "ha": "center", "va": "center"}
            if loc == 'right':
                kw.update(ha='left')
            elif loc == 'left':
                kw.update(ha='right')
            elif loc == 'top':
                kw.update(va='bottom')
            elif loc == 'bottom':
                kw.update(va='top')

        else:

            # Rotation along gridlines
            if (isinstance(self.rotate_labels, (float, int)) and
                    not isinstance(self.rotate_labels, bool)):
                angle = self.rotate_labels
            kw = {'rotation': angle, 'rotation_mode': 'anchor', 'va': 'center'}
            if (angle < 90 + self.offset_angle and
                    angle > -90 + self.offset_angle):
                kw.update(ha="left", rotation=angle)
            else:
                kw.update(ha="right", rotation=angle + 180)

        # Inside labels
        if getattr(self, xylabel + "padding") < 0:
            if "ha" in kw:
                if kw["ha"] == "left":
                    kw["ha"] = "right"
                elif kw["ha"] == "right":
                    kw["ha"] = "left"
            if "va" in kw:
                if kw["va"] == "top":
                    kw["va"] = "bottom"
                elif kw["va"] == "bottom":
                    kw["va"] = "top"

        return kw

    def _get_padding_transform(
            self, padding_angle, loc, xylabel, padding_factor=1):
        """Get transform from angle and padding for non-inline labels"""

        # No rotation
        if self.rotate_labels is False and loc != "geo":
            padding_angle = {
                'top': 90., 'right': 0., 'bottom': -90., 'left': 180.}[loc]

        # Padding
        if xylabel == "x":
            padding = (self.xpadding if self.xpadding is not None
                       else matplotlib.rcParams['xtick.major.pad'])
        else:
            padding = (self.ypadding if self.ypadding is not None
                       else matplotlib.rcParams['ytick.major.pad'])
        dx = padding_factor * padding * np.cos(padding_angle * np.pi / 180)
        dy = padding_factor * padding * np.sin(padding_angle * np.pi / 180)

        # Final transform
        return mtrans.offset_copy(
            self.axes.transData, fig=self.axes.figure,
            x=dx, y=dy, units='points')

    def _assert_can_draw_ticks(self):
        """
        Check to see if ticks can be drawn. Either returns True or raises
        an exception.

        """
        # Check labelling is supported, currently a limited set of options.
        if not isinstance(self.crs, PlateCarree):
            raise TypeError(f'Cannot label {self.crs.__class__.__name__} '
                            'gridlines. Only PlateCarree gridlines are '
                            'currently supported.')
        return True

    def _axes_domain(self, nx=None, ny=None):
        """Return lon_range, lat_range"""
        DEBUG = False

        transform = self._crs_transform()

        ax_transform = self.axes.transAxes
        desired_trans = ax_transform - transform

        nx = nx or 100
        ny = ny or 100
        x = np.linspace(1e-9, 1 - 1e-9, nx)
        y = np.linspace(1e-9, 1 - 1e-9, ny)
        x, y = np.meshgrid(x, y)

        coords = np.column_stack((x.ravel(), y.ravel()))

        in_data = desired_trans.transform(coords)

        ax_to_bkg_patch = self.axes.transAxes - self.axes.patch.get_transform()

        # convert the coordinates of the data to the background patches
        # coordinates
        background_coord = ax_to_bkg_patch.transform(coords)
        ok = self.axes.patch.get_path().contains_points(background_coord)

        if DEBUG:
            import matplotlib.pyplot as plt
            plt.plot(coords[ok, 0], coords[ok, 1], 'or',
                     clip_on=False, transform=ax_transform)
            plt.plot(coords[~ok, 0], coords[~ok, 1], 'ob',
                     clip_on=False, transform=ax_transform)

        inside = in_data[ok, :]

        # If there were no data points in the axes we just use the x and y
        # range of the projection.
        if inside.size == 0:
            lon_range = self.crs.x_limits
            lat_range = self.crs.y_limits
        else:
            # np.isfinite must be used to prevent np.inf values that
            # not filtered by np.nanmax for some projections
            lat_max = np.compress(np.isfinite(inside[:, 1]),
                                  inside[:, 1])
            if lat_max.size == 0:
                lon_range = self.crs.x_limits
                lat_range = self.crs.y_limits
            else:
                lat_max = lat_max.max()
                lon_range = np.nanmin(inside[:, 0]), np.nanmax(inside[:, 0])
                lat_range = np.nanmin(inside[:, 1]), lat_max

        # XXX Cartopy specific thing. Perhaps make this bit a specialisation
        # in a subclass...
        crs = self.crs
        if isinstance(crs, Projection):
            lon_range = np.clip(lon_range, *crs.x_limits)
            lat_range = np.clip(lat_range, *crs.y_limits)

            # if the limit is >90% of the full x limit, then just use the full
            # x limit (this makes circular handling better)
            prct = np.abs(np.diff(lon_range) / np.diff(crs.x_limits))
            if prct > 0.9:
                lon_range = crs.x_limits

        if self.xlim is not None:
            if np.iterable(self.xlim):
                # tuple, list or ndarray was passed in: (-140, 160)
                lon_range = self.xlim
            else:
                # A single int/float was passed in: 140
                lon_range = (-self.xlim, self.xlim)

        if self.ylim is not None:
            if np.iterable(self.ylim):
                # tuple, list or ndarray was passed in: (-140, 160)
                lat_range = self.ylim
            else:
                # A single int/float was passed in: 140
                lat_range = (-self.ylim, self.ylim)

        return lon_range, lat_range

    def get_visible_children(self):
        r"""Return a list of the visible child `.Artist`\s."""
        all_children = (self.xline_artists + self.yline_artists
                        + self.label_artists)
        return [c for c in all_children if c.get_visible()]

    def get_tightbbox(self, renderer=None):
        self._draw_gridliner(renderer=renderer)
        bboxes = [c.get_tightbbox(renderer=renderer)
                  for c in self.get_visible_children()]
        if bboxes:
            return mtrans.Bbox.union(bboxes)
        else:
            return mtrans.Bbox.null()

    def draw(self, renderer=None):
        self._draw_gridliner(renderer=renderer)
        for c in self.get_visible_children():
            c.draw(renderer=renderer)


class Label:
    """Helper class to manage the attributes for a single label"""

    def __init__(self, artist, path, xy, loc):

        self.artist = artist
        self.loc = loc
        self.path = path
        self.xy = xy

    @property
    def priority(self):
        return self.loc in ["left", "right", "top", "bottom"]

    def set_visible(self, value):
        self.artist.set_visible(value)

    def get_visible(self):
        return self.artist.get_visible()

    def check_overlapping(self, label):
        overlapping = self.path.intersects_path(label.path)
        if overlapping:
            self.set_visible(False)
        return overlapping
