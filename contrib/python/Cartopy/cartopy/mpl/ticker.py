# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
"""Tools for handling tick marks in cartopy."""

import matplotlib as mpl
from matplotlib.ticker import Formatter, MaxNLocator
import numpy as np

import cartopy.crs as ccrs
from cartopy.mpl.geoaxes import GeoAxes


class _PlateCarreeFormatter(Formatter):
    """
    Base class for formatting ticks on geographical axes using a
    rectangular projection (e.g. Plate Carree, Mercator).

    """
    def __init__(self, direction_label=True, degree_symbol='°',
                 number_format='g', transform_precision=1e-8, dms=False,
                 minute_symbol='′', second_symbol='″',
                 seconds_number_format='g',
                 auto_hide=True, decimal_point=None, cardinal_labels=None):
        """
        Base class for simpler implementation of specialised formatters
        for latitude and longitude axes.

        """
        self._direction_labels = direction_label
        self._degree_symbol = degree_symbol
        self._degrees_number_format = number_format
        self._transform_precision = transform_precision
        self._dms = dms
        self._minute_symbol = minute_symbol
        self._second_symbol = second_symbol
        self._seconds_num_format = seconds_number_format
        self._auto_hide = auto_hide
        self._auto_hide_degrees = False
        self._auto_hide_minutes = False
        self._precision = 5  # locator precision
        if (decimal_point is None and
                mpl.rcParams['axes.formatter.use_locale']):
            import locale
            decimal_point = locale.localeconv()["decimal_point"]
        if cardinal_labels is None:
            cardinal_labels = {}
        self._cardinal_labels = cardinal_labels
        self._decimal_point = decimal_point
        self._source_projection = None
        self._target_projection = None

    def __call__(self, value, pos=None):
        if self._source_projection is not None:
            projected_value = self._apply_transform(value,
                                                    self._target_projection,
                                                    self._source_projection)

            # Round the transformed value using a given precision for display
            # purposes. Transforms can introduce minor rounding errors that
            # make the tick values look bad, these need to be accounted for.
            f = 1. / self._transform_precision
            projected_value = round(f * projected_value) / f

        else:

            # There is no projection so we assume it is already PlateCarree
            projected_value = value

        # Return the formatted values, the formatter has both the re-projected
        # tick value and the original axis value available to it.
        return self._format_value(projected_value, value)

    def _format_value(self, value, original_value):

        hemisphere = ''
        sign = ''

        if self._direction_labels:
            hemisphere = self._hemisphere(value, original_value)
        else:
            if (value != 0 and
                    self._hemisphere(value, original_value) in ['W', 'S']):
                sign = '-'

        if not self._dms:
            return (sign + self._format_degrees(abs(value)) +
                    hemisphere)

        value, deg, mn, sec = self._get_dms(abs(value))

        # Format
        label = ''
        if sec:
            label = self._format_seconds(sec)

        if mn or (not self._auto_hide_minutes and label):
            label = self._format_minutes(mn) + label

        if not self._auto_hide_degrees or not label:
            label = sign + self._format_degrees(deg) + label + hemisphere

        return label

    def _get_dms(self, x):
        """Convert to degrees, minutes, seconds

        Parameters
        ----------
        x: float or array of floats
            Degrees

        Return
        ------
        x: degrees rounded to the requested precision
        degs: degrees
        mins: minutes
        secs: seconds
        """
        self._precision = 6
        x = np.asarray(x, 'd')
        degs = np.round(x, self._precision).astype('i')
        y = (x - degs) * 60
        mins = np.round(y, self._precision).astype('i')
        secs = np.round((y - mins) * 60, self._precision - 3)
        return x, degs, mins, secs

    def set_axis(self, axis):
        super().set_axis(axis)

        # Set the source and target projections for the formatter
        # setting them to None if we aren't interacting with a GeoAxes
        if self.axis is None or not isinstance(self.axis.axes, GeoAxes):
            self._source_projection = None
            self._target_projection = None
            return

        self._source_projection = self.axis.axes.projection
        if not isinstance(self._source_projection, (ccrs._RectangularProjection,
                                                    ccrs.Mercator)):
            raise TypeError("This formatter cannot be used with "
                            "non-rectangular projections.")
        # The transforms need to use the same globe
        self._target_projection = ccrs.PlateCarree(globe=self._source_projection.globe)

    def set_locs(self, locs):
        Formatter.set_locs(self, locs)
        if not self._auto_hide:
            return
        self.locs, degs, mins, secs = self._get_dms(self.locs)
        secs = np.round(secs, self._precision - 3).astype('i')
        secs0 = secs == 0
        mins0 = mins == 0

        def auto_hide(valid, values):
            """Should I switch on auto_hide?"""
            if not valid.any():
                return False
            if valid.sum() == 1:
                return True
            return np.diff(values.compress(valid)).max() == 1

        # Potentially hide minutes labels when pure minutes are all displayed
        self._auto_hide_minutes = auto_hide(secs0, mins)

        # Potentially hide degrees labels when pure degrees are all displayed
        self._auto_hide_degrees = auto_hide(secs0 & mins0, degs)

    def _format_degrees(self, deg):
        """Format degrees as an integer"""
        if self._dms:
            deg = int(deg)
            number_format = 'd'
        else:
            number_format = self._degrees_number_format
        value = f"{abs(deg):{number_format}}{self._degree_symbol}"
        if self._decimal_point is not None:
            value = value.replace(".", self._decimal_point)
        return value

    def _format_minutes(self, mn):
        """Format minutes as an integer"""
        return f'{int(mn):d}{self._minute_symbol}'

    def _format_seconds(self, sec):
        """Format seconds as an float"""
        return f'{sec:{self._seconds_num_format}}{self._second_symbol}'

    def _apply_transform(self, value, target_proj, source_crs):
        """
        Given a single value, a target projection and a source CRS,
        transform the value from the source CRS to the target
        projection, returning a single value.

        """
        raise NotImplementedError("A subclass must implement this method.")

    def _hemisphere(self, value, value_source_crs):
        """
        Given both a tick value in the Plate Carree projection and the
        same value in the source CRS, return a string indicating the
        hemisphere that the value is in.

        Must be over-ridden by the derived class.

        """
        raise NotImplementedError("A subclass must implement this method.")


class LatitudeFormatter(_PlateCarreeFormatter):
    """Tick formatter for latitude axes."""

    def __init__(self, direction_label=True,
                 degree_symbol='°', number_format='g',
                 transform_precision=1e-8, dms=False,
                 minute_symbol='′', second_symbol='″',
                 seconds_number_format='g', auto_hide=True,
                 decimal_point=None, cardinal_labels=None
                 ):
        """
        Tick formatter for latitudes.

        When bound to an axis, the axis must be part of an axes defined
        on a rectangular projection (e.g. Plate Carree, Mercator).


        Parameters
        ----------
        direction_label: optional
            If *True* a direction label (N or S) will be drawn next to
            latitude labels. If *False* then these
            labels will not be drawn. Defaults to *True* (draw direction
            labels).
        degree_symbol: optional
            The character(s) used to represent the degree symbol in the tick
            labels. Defaults to '°'. Can be an empty string if no degree symbol
            is desired.
        number_format: optional
            Format string to represent the longitude values when `dms`
            is set to False. Defaults to 'g'.
        transform_precision: optional
            Sets the precision (in degrees) to which transformed tick
            values are rounded. The default is 1e-7, and should be
            suitable for most use cases. To control the appearance of
            tick labels use the *number_format* keyword.
        dms: bool, optional
            Whether or not formatting as degrees-minutes-seconds and not
            as decimal degrees.
        minute_symbol: str, optional
            The character(s) used to represent the minute symbol.
        second_symbol: str, optional
            The character(s) used to represent the second symbol.
        seconds_number_format: optional
            Format string to represent the "seconds" component of the longitude
            values. Defaults to 'g'.
        auto_hide: bool, optional
            Auto-hide degrees or minutes when redundant.
        decimal_point: bool, optional
            Decimal point character. If not provided and
            ``mpl.rcParams['axes.formatter.use_locale'] == True``,
            the locale decimal point is used.
        cardinal_labels: dict, optional
            A dictionary with "south" and/or "north" keys to replace south and
            north cardinal labels, which defaults to "S" and "N".

        Note
        ----
            A formatter can only be used for one axis. A new formatter
            must be created for every axis that needs formatted labels.

        Examples
        --------
        Label latitudes from -90 to 90 on a Plate Carree projection::

            ax = plt.axes(projection=PlateCarree())
            ax.set_global()
            ax.set_yticks([-90, -60, -30, 0, 30, 60, 90],
                          crs=ccrs.PlateCarree())
            lat_formatter = LatitudeFormatter()
            ax.yaxis.set_major_formatter(lat_formatter)

        Label latitudes from -80 to 80 on a Mercator projection, this
        time omitting the degree symbol::

            ax = plt.axes(projection=Mercator())
            ax.set_global()
            ax.set_yticks([-90, -60, -30, 0, 30, 60, 90],
                          crs=ccrs.PlateCarree())
            lat_formatter = LatitudeFormatter(degree_symbol='')
            ax.yaxis.set_major_formatter(lat_formatter)

        When not bound to an axis::

            lat_formatter = LatitudeFormatter()
            ticks = [-90, -60, -30, 0, 30, 60, 90]
            lat_formatter.set_locs(ticks)
            labels = [lat_formatter(value) for value in ticks]

        """
        super().__init__(
            direction_label=direction_label,
            degree_symbol=degree_symbol,
            number_format=number_format,
            transform_precision=transform_precision,
            dms=dms,
            minute_symbol=minute_symbol,
            second_symbol=second_symbol,
            seconds_number_format=seconds_number_format,
            auto_hide=auto_hide,
            decimal_point=decimal_point,
            cardinal_labels=cardinal_labels
        )

    def _apply_transform(self, value, target_proj, source_crs):
        return target_proj.transform_point(0, value, source_crs)[1]

    def _hemisphere(self, value, value_source_crs):

        if value > 0:
            hemisphere = self._cardinal_labels.get('north', 'N')
        elif value < 0:
            hemisphere = self._cardinal_labels.get('south', 'S')
        else:
            hemisphere = ''
        return hemisphere


class LongitudeFormatter(_PlateCarreeFormatter):
    """Tick formatter for a longitude axis."""

    def __init__(self,
                 direction_label=True,
                 zero_direction_label=False,
                 dateline_direction_label=False,
                 degree_symbol='°',
                 number_format='g',
                 transform_precision=1e-8,
                 dms=False,
                 minute_symbol='′',
                 second_symbol='″',
                 seconds_number_format='g',
                 auto_hide=True,
                 decimal_point=None,
                 cardinal_labels=None
                 ):
        """
        Create a formatter for longitudes.

        When bound to an axis, the axis must be part of an axes defined
        on a rectangular projection (e.g. Plate Carree, Mercator).

        Parameters
        ----------
        direction_label: optional
            If *True* a direction label (E or W) will be drawn next to
            longitude labels. If *False* then these
            labels will not be drawn. Defaults to *True* (draw direction
            labels).
        zero_direction_label: optional
            If *True* a direction label (E or W) will be drawn next to
            longitude labels with the value 0. If *False* then these
            labels will not be drawn. Defaults to *False* (no direction
            labels).
        dateline_direction_label: optional
            If *True* a direction label (E or W) will be drawn next to
            longitude labels with the value 180. If *False* then these
            labels will not be drawn. Defaults to *False* (no direction
            labels).
        degree_symbol: optional
            The symbol used to represent degrees. Defaults to '°'.
        number_format: optional
            Format string to represent the latitude values when `dms`
            is set to False. Defaults to 'g'.
        transform_precision: optional
            Sets the precision (in degrees) to which transformed tick
            values are rounded. The default is 1e-7, and should be
            suitable for most use cases. To control the appearance of
            tick labels use the *number_format* keyword.
        dms: bool, optional
            Whether or not formatting as degrees-minutes-seconds and not
            as decimal degrees.
        minute_symbol: str, optional
            The character(s) used to represent the minute symbol.
        second_symbol: str, optional
            The character(s) used to represent the second symbol.
        seconds_number_format: optional
            Format string to represent the "seconds" component of the latitude
            values. Defaults to 'g'.
        auto_hide: bool, optional
            Auto-hide degrees or minutes when redundant.
        decimal_point: bool, optional
            Decimal point character. If not provided and
            ``mpl.rcParams['axes.formatter.use_locale'] == True``,
            the locale decimal point is used.
        cardinal_labels: dict, optional
            A dictionary with "west" and/or "east" keys to replace west and
            east cardinal labels, which defaults to "W" and "E".

        Note
        ----
            A formatter can only be used for one axis. A new formatter
            must be created for every axis that needs formatted labels.

        Examples
        --------
        Label longitudes from -180 to 180 on a Plate Carree projection
        with a central longitude of 0::

            ax = plt.axes(projection=PlateCarree())
            ax.set_global()
            ax.set_xticks([-180, -120, -60, 0, 60, 120, 180],
                          crs=ccrs.PlateCarree())
            lon_formatter = LongitudeFormatter()
            ax.xaxis.set_major_formatter(lon_formatter)

        Label longitudes from 0 to 360 on a Plate Carree projection
        with a central longitude of 180::

            ax = plt.axes(projection=PlateCarree(central_longitude=180))
            ax.set_global()
            ax.set_xticks([0, 60, 120, 180, 240, 300, 360],
                          crs=ccrs.PlateCarree())
            lon_formatter = LongitudeFormatter()
            ax.xaxis.set_major_formatter(lon_formatter)


        When not bound to an axis::

            lon_formatter = LongitudeFormatter()
            ticks = [0, 60, 120, 180, 240, 300, 360]
            lon_formatter.set_locs(ticks)
            labels = [lon_formatter(value) for value in ticks]
        """
        super().__init__(
            direction_label=direction_label,
            degree_symbol=degree_symbol,
            number_format=number_format,
            transform_precision=transform_precision,
            dms=dms,
            minute_symbol=minute_symbol,
            second_symbol=second_symbol,
            seconds_number_format=seconds_number_format,
            auto_hide=auto_hide,
            decimal_point=decimal_point,
            cardinal_labels=cardinal_labels
        )
        self._zero_direction_labels = zero_direction_label
        self._dateline_direction_labels = dateline_direction_label

    def _apply_transform(self, value, target_proj, source_crs):
        return target_proj.transform_point(value, 0, source_crs)[0]

    @classmethod
    def _fix_lons(cls, lons):
        if isinstance(lons, list):
            return [cls._fix_lons(lon) for lon in lons]
        p180 = lons == 180
        m180 = lons == -180

        # Wrap
        lons = ((lons + 180) % 360) - 180

        # Keep -180 and 180 when requested
        for mp180, value in [(m180, -180), (p180, 180)]:
            if np.any(mp180):
                if isinstance(lons, np.ndarray):
                    lons = np.where(mp180, value, lons)
                else:
                    lons = value

        return lons

    def set_locs(self, locs):
        _PlateCarreeFormatter.set_locs(self, self._fix_lons(locs))

    def _format_degrees(self, deg):
        return _PlateCarreeFormatter._format_degrees(self, self._fix_lons(deg))

    def _hemisphere(self, value, value_source_crs):

        value = self._fix_lons(value)
        # Perform basic hemisphere detection.
        if value < 0:
            hemisphere = self._cardinal_labels.get('west', 'W')
        elif value > 0:
            hemisphere = self._cardinal_labels.get('east', 'E')
        else:
            hemisphere = ''
        # Correct for user preferences:
        if value == 0 and self._zero_direction_labels:
            # Use the original tick value to determine the hemisphere.
            if value_source_crs < 0:
                hemisphere = self._cardinal_labels.get('east', 'E')
            else:
                hemisphere = self._cardinal_labels.get('west', 'W')
        if value in (-180, 180) and not self._dateline_direction_labels:
            hemisphere = ''
        return hemisphere


class LongitudeLocator(MaxNLocator):
    """
    A locator for longitudes that works even at very small scale.

    Parameters
    ----------
    dms: bool
        Allow the locator to stop on minutes and seconds (False by default)
    """

    def __init__(self, nbins=8, *, dms=False, **kwargs):
        super().__init__(nbins=nbins, dms=dms, **kwargs)

    def set_params(self, **kwargs):
        """Set parameters within this locator."""
        if 'dms' in kwargs:
            self._dms = kwargs.pop('dms')
        MaxNLocator.set_params(self, **kwargs)

    def _guess_steps(self, vmin, vmax):

        dv = abs(vmax - vmin)
        if dv > 180:
            dv -= 180

        if dv > 50.:

            steps = np.array([1, 2, 3, 6, 10])

        elif not self._dms or dv > 3.:

            steps = np.array([1, 1.5, 2, 2.5, 3, 5, 10])

        else:
            steps = np.array([1, 10 / 6., 15 / 6., 20 / 6., 30 / 6., 10])

        self.set_params(steps=np.array(steps))

    def _raw_ticks(self, vmin, vmax):
        self._guess_steps(vmin, vmax)
        return MaxNLocator._raw_ticks(self, vmin, vmax)

    def bin_boundaries(self, vmin, vmax):
        self._guess_steps(vmin, vmax)
        return MaxNLocator.bin_boundaries(self, vmin, vmax)


class LatitudeLocator(LongitudeLocator):
    """
    A locator for latitudes that works even at very small scale.

    Parameters
    ----------
    dms: bool
        Allow the locator to stop on minutes and seconds (False by default)
    """

    def tick_values(self, vmin, vmax):
        vmin = max(vmin, -90.)
        vmax = min(vmax, 90.)
        return LongitudeLocator.tick_values(self, vmin, vmax)

    def _guess_steps(self, vmin, vmax):
        vmin = max(vmin, -90.)
        vmax = min(vmax, 90.)
        LongitudeLocator._guess_steps(self, vmin, vmax)

    def _raw_ticks(self, vmin, vmax):
        ticks = LongitudeLocator._raw_ticks(self, vmin, vmax)
        return [t for t in ticks if -90 <= t <= 90]

    def bin_boundaries(self, vmin, vmax):
        ticks = LongitudeLocator.bin_boundaries(self, vmin, vmax)
        return [t for t in ticks if -90 <= t <= 90]
