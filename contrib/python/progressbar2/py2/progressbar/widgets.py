# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import abc
import sys
import pprint
import datetime
import functools

from python_utils import converters

import six

from . import base
from . import utils

MAX_DATE = datetime.date.max
MAX_TIME = datetime.time.max
MAX_DATETIME = datetime.datetime.max


def string_or_lambda(input_):
    if isinstance(input_, six.string_types):
        def render_input(progress, data, width):
            return input_ % data

        return render_input
    else:
        return input_


def create_wrapper(wrapper):
    '''Convert a wrapper tuple or format string to a format string

    >>> create_wrapper('')

    >>> print(create_wrapper('a{}b'))
    a{}b

    >>> print(create_wrapper(('a', 'b')))
    a{}b
    '''
    if isinstance(wrapper, tuple) and len(wrapper) == 2:
        a, b = wrapper
        wrapper = (a or '') + '{}' + (b or '')
    elif not wrapper:
        return

    if isinstance(wrapper, six.string_types):
        assert '{}' in wrapper, 'Expected string with {} for formatting'
    else:
        raise RuntimeError('Pass either a begin/end string as a tuple or a'
                           ' template string with {}')

    return wrapper


def wrapper(function, wrapper):
    '''Wrap the output of a function in a template string or a tuple with
    begin/end strings

    '''
    wrapper = create_wrapper(wrapper)
    if not wrapper:
        return function

    @functools.wraps(function)
    def wrap(*args, **kwargs):
        return wrapper.format(function(*args, **kwargs))

    return wrap


def create_marker(marker, wrap=None):
    def _marker(progress, data, width):
        if progress.max_value is not base.UnknownLength \
                and progress.max_value > 0:
            length = int(progress.value / progress.max_value * width)
            return (marker * length)
        else:
            return marker

    if isinstance(marker, six.string_types):
        marker = converters.to_unicode(marker)
        assert utils.len_color(marker) == 1, \
            'Markers are required to be 1 char'
        return wrapper(_marker, wrap)
    else:
        return wrapper(marker, wrap)


class FormatWidgetMixin(object):
    '''Mixin to format widgets using a formatstring

    Variables available:
     - max_value: The maximum value (can be None with iterators)
     - value: The current value
     - total_seconds_elapsed: The seconds since the bar started
     - seconds_elapsed: The seconds since the bar started modulo 60
     - minutes_elapsed: The minutes since the bar started modulo 60
     - hours_elapsed: The hours since the bar started modulo 24
     - days_elapsed: The hours since the bar started
     - time_elapsed: Shortcut for HH:MM:SS time since the bar started including
       days
     - percentage: Percentage as a float
    '''
    required_values = []

    def __init__(self, format, new_style=False, **kwargs):
        self.new_style = new_style
        self.format = format

    def get_format(self, progress, data, format=None):
        return format or self.format

    def __call__(self, progress, data, format=None):
        '''Formats the widget into a string'''
        format = self.get_format(progress, data, format)
        try:
            if self.new_style:
                return format.format(**data)
            else:
                return format % data
        except (TypeError, KeyError):
            print('Error while formatting %r' % format, file=sys.stderr)
            pprint.pprint(data, stream=sys.stderr)
            raise


class WidthWidgetMixin(object):
    '''Mixing to make sure widgets are only visible if the screen is within a
    specified size range so the progressbar fits on both large and small
    screens..

    Variables available:
     - min_width: Only display the widget if at least `min_width` is left
     - max_width: Only display the widget if at most `max_width` is left

    >>> class Progress(object):
    ...     term_width = 0

    >>> WidthWidgetMixin(5, 10).check_size(Progress)
    False
    >>> Progress.term_width = 5
    >>> WidthWidgetMixin(5, 10).check_size(Progress)
    True
    >>> Progress.term_width = 10
    >>> WidthWidgetMixin(5, 10).check_size(Progress)
    True
    >>> Progress.term_width = 11
    >>> WidthWidgetMixin(5, 10).check_size(Progress)
    False
    '''

    def __init__(self, min_width=None, max_width=None, **kwargs):
        self.min_width = min_width
        self.max_width = max_width

    def check_size(self, progress):
        if self.min_width and self.min_width > progress.term_width:
            return False
        elif self.max_width and self.max_width < progress.term_width:
            return False
        else:
            return True


class WidgetBase(WidthWidgetMixin):
    __metaclass__ = abc.ABCMeta
    '''The base class for all widgets

    The ProgressBar will call the widget's update value when the widget should
    be updated. The widget's size may change between calls, but the widget may
    display incorrectly if the size changes drastically and repeatedly.

    The boolean INTERVAL informs the ProgressBar that it should be
    updated more often because it is time sensitive.

    The widgets are only visible if the screen is within a
    specified size range so the progressbar fits on both large and small
    screens.

    WARNING: Widgets can be shared between multiple progressbars so any state
    information specific to a progressbar should be stored within the
    progressbar instead of the widget.

    Variables available:
     - min_width: Only display the widget if at least `min_width` is left
     - max_width: Only display the widget if at most `max_width` is left
     - weight: Widgets with a higher `weigth` will be calculated before widgets
       with a lower one
    - copy: Copy this widget when initializing the progress bar so the
      progressbar can be reused. Some widgets such as the FormatCustomText
      require the shared state so this needs to be optional
    '''
    copy = True

    @abc.abstractmethod
    def __call__(self, progress, data):
        '''Updates the widget.

        progress - a reference to the calling ProgressBar
        '''


class AutoWidthWidgetBase(WidgetBase):
    '''The base class for all variable width widgets.

    This widget is much like the \\hfill command in TeX, it will expand to
    fill the line. You can use more than one in the same line, and they will
    all have the same width, and together will fill the line.
    '''

    @abc.abstractmethod
    def __call__(self, progress, data, width):
        '''Updates the widget providing the total width the widget must fill.

        progress - a reference to the calling ProgressBar
        width - The total width the widget must fill
        '''


class TimeSensitiveWidgetBase(WidgetBase):
    '''The base class for all time sensitive widgets.

    Some widgets like timers would become out of date unless updated at least
    every `INTERVAL`
    '''
    INTERVAL = datetime.timedelta(milliseconds=100)


class FormatLabel(FormatWidgetMixin, WidgetBase):
    '''Displays a formatted label

    >>> label = FormatLabel('%(value)s', min_width=5, max_width=10)
    >>> class Progress(object):
    ...     pass
    >>> label = FormatLabel('{value} :: {value:^6}', new_style=True)
    >>> str(label(Progress, dict(value='test')))
    'test ::  test '

    '''

    mapping = {
        'finished': ('end_time', None),
        'last_update': ('last_update_time', None),
        'max': ('max_value', None),
        'seconds': ('seconds_elapsed', None),
        'start': ('start_time', None),
        'elapsed': ('total_seconds_elapsed', utils.format_time),
        'value': ('value', None),
    }

    def __init__(self, format, **kwargs):
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, **kwargs)

    def __call__(self, progress, data, **kwargs):
        for name, (key, transform) in self.mapping.items():
            try:
                if transform is None:
                    data[name] = data[key]
                else:
                    data[name] = transform(data[key])
            except (KeyError, ValueError, IndexError):  # pragma: no cover
                pass

        return FormatWidgetMixin.__call__(self, progress, data, **kwargs)


class Timer(FormatLabel, TimeSensitiveWidgetBase):
    '''WidgetBase which displays the elapsed seconds.'''

    def __init__(self, format='Elapsed Time: %(elapsed)s', **kwargs):
        FormatLabel.__init__(self, format=format, **kwargs)
        TimeSensitiveWidgetBase.__init__(self, **kwargs)

    # This is exposed as a static method for backwards compatibility
    format_time = staticmethod(utils.format_time)


class SamplesMixin(TimeSensitiveWidgetBase):
    '''
    Mixing for widgets that average multiple measurements

    Note that samples can be either an integer or a timedelta to indicate a
    certain amount of time

    >>> class progress:
    ...     last_update_time = datetime.datetime.now()
    ...     value = 1
    ...     extra = dict()

    >>> samples = SamplesMixin(samples=2)
    >>> samples(progress, None, True)
    (None, None)
    >>> progress.last_update_time += datetime.timedelta(seconds=1)
    >>> samples(progress, None, True) == (datetime.timedelta(seconds=1), 0)
    True

    >>> progress.last_update_time += datetime.timedelta(seconds=1)
    >>> samples(progress, None, True) == (datetime.timedelta(seconds=1), 0)
    True

    >>> samples = SamplesMixin(samples=datetime.timedelta(seconds=1))
    >>> _, value = samples(progress, None)
    >>> value
    [1, 1]

    >>> samples(progress, None, True) == (datetime.timedelta(seconds=1), 0)
    True
    '''

    def __init__(self, samples=datetime.timedelta(seconds=2), key_prefix=None,
                 **kwargs):
        self.samples = samples
        self.key_prefix = (self.__class__.__name__ or key_prefix) + '_'
        TimeSensitiveWidgetBase.__init__(self, **kwargs)

    def get_sample_times(self, progress, data):
        return progress.extra.setdefault(self.key_prefix + 'sample_times', [])

    def get_sample_values(self, progress, data):
        return progress.extra.setdefault(self.key_prefix + 'sample_values', [])

    def __call__(self, progress, data, delta=False):
        sample_times = self.get_sample_times(progress, data)
        sample_values = self.get_sample_values(progress, data)

        if sample_times:
            sample_time = sample_times[-1]
        else:
            sample_time = datetime.datetime.min

        if progress.last_update_time - sample_time > self.INTERVAL:
            # Add a sample but limit the size to `num_samples`
            sample_times.append(progress.last_update_time)
            sample_values.append(progress.value)

            if isinstance(self.samples, datetime.timedelta):
                minimum_time = progress.last_update_time - self.samples
                minimum_value = sample_values[-1]
                while (sample_times[2:] and
                       minimum_time > sample_times[1] and
                       minimum_value > sample_values[1]):
                    sample_times.pop(0)
                    sample_values.pop(0)
            else:
                if len(sample_times) > self.samples:
                    sample_times.pop(0)
                    sample_values.pop(0)

        if delta:
            delta_time = sample_times[-1] - sample_times[0]
            delta_value = sample_values[-1] - sample_values[0]
            if delta_time:
                return delta_time, delta_value
            else:
                return None, None
        else:
            return sample_times, sample_values


class ETA(Timer):
    '''WidgetBase which attempts to estimate the time of arrival.'''

    def __init__(
            self,
            format_not_started='ETA:  --:--:--',
            format_finished='Time: %(elapsed)8s',
            format='ETA:  %(eta)8s',
            format_zero='ETA:  00:00:00',
            format_NA='ETA:      N/A',
            **kwargs):

        Timer.__init__(self, **kwargs)
        self.format_not_started = format_not_started
        self.format_finished = format_finished
        self.format = format
        self.format_zero = format_zero
        self.format_NA = format_NA

    def _calculate_eta(self, progress, data, value, elapsed):
        '''Updates the widget to show the ETA or total time when finished.'''
        if elapsed:
            # The max() prevents zero division errors
            per_item = elapsed.total_seconds() / max(value, 1e-6)
            remaining = progress.max_value - data['value']
            eta_seconds = remaining * per_item
        else:
            eta_seconds = 0

        return eta_seconds

    def __call__(self, progress, data, value=None, elapsed=None):
        '''Updates the widget to show the ETA or total time when finished.'''
        if value is None:
            value = data['value']

        if elapsed is None:
            elapsed = data['time_elapsed']

        ETA_NA = False
        try:
            data['eta_seconds'] = self._calculate_eta(
                progress, data, value=value, elapsed=elapsed)
        except TypeError:
            data['eta_seconds'] = None
            ETA_NA = True

        data['eta'] = None
        if data['eta_seconds']:
            try:
                data['eta'] = utils.format_time(data['eta_seconds'])
            except (ValueError, OverflowError):  # pragma: no cover
                pass

        if data['value'] == progress.min_value:
            format = self.format_not_started
        elif progress.end_time:
            format = self.format_finished
        elif data['eta']:
            format = self.format
        elif ETA_NA:
            format = self.format_NA
        else:
            format = self.format_zero

        return Timer.__call__(self, progress, data, format=format)


class AbsoluteETA(ETA):
    '''Widget which attempts to estimate the absolute time of arrival.'''

    def _calculate_eta(self, progress, data, value, elapsed):
        eta_seconds = ETA._calculate_eta(self, progress, data, value, elapsed)
        now = datetime.datetime.now()
        try:
            return now + datetime.timedelta(seconds=eta_seconds)
        except OverflowError:  # pragma: no cover
            return datetime.datetime.max

    def __init__(
            self,
            format_not_started='Estimated finish time:  ----/--/-- --:--:--',
            format_finished='Finished at: %(elapsed)s',
            format='Estimated finish time: %(eta)s',
            **kwargs):
        ETA.__init__(self, format_not_started=format_not_started,
                     format_finished=format_finished, format=format, **kwargs)


class AdaptiveETA(ETA, SamplesMixin):
    '''WidgetBase which attempts to estimate the time of arrival.

    Uses a sampled average of the speed based on the 10 last updates.
    Very convenient for resuming the progress halfway.
    '''

    def __init__(self, **kwargs):
        ETA.__init__(self, **kwargs)
        SamplesMixin.__init__(self, **kwargs)

    def __call__(self, progress, data):
        elapsed, value = SamplesMixin.__call__(self, progress, data,
                                               delta=True)
        if not elapsed:
            value = None
            elapsed = 0

        return ETA.__call__(self, progress, data, value=value, elapsed=elapsed)


class DataSize(FormatWidgetMixin, WidgetBase):
    '''
    Widget for showing an amount of data transferred/processed.

    Automatically formats the value (assumed to be a count of bytes) with an
    appropriate sized unit, based on the IEC binary prefixes (powers of 1024).
    '''

    def __init__(
            self, variable='value',
            format='%(scaled)5.1f %(prefix)s%(unit)s', unit='B',
            prefixes=('', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
            **kwargs):
        self.variable = variable
        self.unit = unit
        self.prefixes = prefixes
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, **kwargs)

    def __call__(self, progress, data):
        value = data[self.variable]
        if value is not None:
            scaled, power = utils.scale_1024(value, len(self.prefixes))
        else:
            scaled = power = 0

        data['scaled'] = scaled
        data['prefix'] = self.prefixes[power]
        data['unit'] = self.unit

        return FormatWidgetMixin.__call__(self, progress, data)


class FileTransferSpeed(FormatWidgetMixin, TimeSensitiveWidgetBase):
    '''
    WidgetBase for showing the transfer speed (useful for file transfers).
    '''

    def __init__(
            self, format='%(scaled)5.1f %(prefix)s%(unit)-s/s',
            inverse_format='%(scaled)5.1f s/%(prefix)s%(unit)-s', unit='B',
            prefixes=('', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
            **kwargs):
        self.unit = unit
        self.prefixes = prefixes
        self.inverse_format = inverse_format
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        TimeSensitiveWidgetBase.__init__(self, **kwargs)

    def _speed(self, value, elapsed):
        speed = float(value) / elapsed
        return utils.scale_1024(speed, len(self.prefixes))

    def __call__(self, progress, data, value=None, total_seconds_elapsed=None):
        '''Updates the widget with the current SI prefixed speed.'''
        if value is None:
            value = data['value']

        elapsed = utils.deltas_to_seconds(
            total_seconds_elapsed,
            data['total_seconds_elapsed'])

        if value is not None and elapsed is not None \
                and elapsed > 2e-6 and value > 2e-6:  # =~ 0
            scaled, power = self._speed(value, elapsed)
        else:
            scaled = power = 0

        data['unit'] = self.unit
        if power == 0 and scaled < 0.1:
            if scaled > 0:
                scaled = 1 / scaled
            data['scaled'] = scaled
            data['prefix'] = self.prefixes[0]
            return FormatWidgetMixin.__call__(self, progress, data,
                                              self.inverse_format)
        else:
            data['scaled'] = scaled
            data['prefix'] = self.prefixes[power]
            return FormatWidgetMixin.__call__(self, progress, data)


class AdaptiveTransferSpeed(FileTransferSpeed, SamplesMixin):
    '''WidgetBase for showing the transfer speed, based on the last X samples
    '''

    def __init__(self, **kwargs):
        FileTransferSpeed.__init__(self, **kwargs)
        SamplesMixin.__init__(self, **kwargs)

    def __call__(self, progress, data):
        elapsed, value = SamplesMixin.__call__(self, progress, data,
                                               delta=True)
        return FileTransferSpeed.__call__(self, progress, data, value, elapsed)


class AnimatedMarker(TimeSensitiveWidgetBase):
    '''An animated marker for the progress bar which defaults to appear as if
    it were rotating.
    '''

    def __init__(self, markers='|/-\\', default=None, fill='',
                 marker_wrap=None, fill_wrap=None, **kwargs):
        self.markers = markers
        self.marker_wrap = create_wrapper(marker_wrap)
        self.default = default or markers[0]
        self.fill_wrap = create_wrapper(fill_wrap)
        self.fill = create_marker(fill, self.fill_wrap) if fill else None
        WidgetBase.__init__(self, **kwargs)

    def __call__(self, progress, data, width=None):
        '''Updates the widget to show the next marker or the first marker when
        finished'''

        if progress.end_time:
            return self.default

        marker = self.markers[data['updates'] % len(self.markers)]
        if self.marker_wrap:
            marker = self.marker_wrap.format(marker)

        if self.fill:
            # Cut the last character so we can replace it with our marker
            fill = self.fill(progress, data, width - progress.custom_len(
                marker))
        else:
            fill = ''

        # Python 3 returns an int when indexing bytes
        if isinstance(marker, int):  # pragma: no cover
            marker = bytes(marker)
            fill = fill.encode()
        else:
            # cast fill to the same type as marker
            fill = type(marker)(fill)

        return fill + marker


# Alias for backwards compatibility
RotatingMarker = AnimatedMarker


class Counter(FormatWidgetMixin, WidgetBase):
    '''Displays the current count'''

    def __init__(self, format='%(value)d', **kwargs):
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, format=format, **kwargs)

    def __call__(self, progress, data, format=None):
        return FormatWidgetMixin.__call__(self, progress, data, format)


class Percentage(FormatWidgetMixin, WidgetBase):
    '''Displays the current percentage as a number with a percent sign.'''

    def __init__(self, format='%(percentage)3d%%', na='N/A%%', **kwargs):
        self.na = na
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, format=format, **kwargs)

    def get_format(self, progress, data, format=None):
        # If percentage is not available, display N/A%
        percentage = data.get('percentage', base.Undefined)
        if not percentage and percentage != 0:
            return self.na

        return FormatWidgetMixin.get_format(self, progress, data, format)


class SimpleProgress(FormatWidgetMixin, WidgetBase):
    '''Returns progress as a count of the total (e.g.: "5 of 47")'''

    DEFAULT_FORMAT = '%(value_s)s of %(max_value_s)s'

    def __init__(self, format=DEFAULT_FORMAT, **kwargs):
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, format=format, **kwargs)
        self.max_width_cache = dict(default=self.max_width)

    def __call__(self, progress, data, format=None):
        # If max_value is not available, display N/A
        if data.get('max_value'):
            data['max_value_s'] = data.get('max_value')
        else:
            data['max_value_s'] = 'N/A'

        # if value is not available it's the zeroth iteration
        if data.get('value'):
            data['value_s'] = data['value']
        else:
            data['value_s'] = 0

        formatted = FormatWidgetMixin.__call__(self, progress, data,
                                               format=format)

        # Guess the maximum width from the min and max value
        key = progress.min_value, progress.max_value
        max_width = self.max_width_cache.get(key, self.max_width)
        if not max_width:
            temporary_data = data.copy()
            for value in key:
                if value is None:  # pragma: no cover
                    continue

                temporary_data['value'] = value
                width = progress.custom_len(FormatWidgetMixin.__call__(
                    self, progress, temporary_data, format=format))
                if width:  # pragma: no branch
                    max_width = max(max_width or 0, width)

            self.max_width_cache[key] = max_width

        # Adjust the output to have a consistent size in all cases
        if max_width:  # pragma: no branch
            formatted = formatted.rjust(max_width)

        return formatted


class Bar(AutoWidthWidgetBase):
    '''A progress bar which stretches to fill the line.'''

    def __init__(self, marker='#', left='|', right='|', fill=' ',
                 fill_left=True, marker_wrap=None, **kwargs):
        '''Creates a customizable progress bar.

        The callable takes the same parameters as the `__call__` method

        marker - string or callable object to use as a marker
        left - string or callable object to use as a left border
        right - string or callable object to use as a right border
        fill - character to use for the empty part of the progress bar
        fill_left - whether to fill from the left or the right
        '''

        self.marker = create_marker(marker, marker_wrap)
        self.left = string_or_lambda(left)
        self.right = string_or_lambda(right)
        self.fill = string_or_lambda(fill)
        self.fill_left = fill_left

        AutoWidthWidgetBase.__init__(self, **kwargs)

    def __call__(self, progress, data, width):
        '''Updates the progress bar and its subcomponents'''

        left = converters.to_unicode(self.left(progress, data, width))
        right = converters.to_unicode(self.right(progress, data, width))
        width -= progress.custom_len(left) + progress.custom_len(right)
        marker = converters.to_unicode(self.marker(progress, data, width))
        fill = converters.to_unicode(self.fill(progress, data, width))

        # Make sure we ignore invisible characters when filling
        width += len(marker) - progress.custom_len(marker)

        if self.fill_left:
            marker = marker.ljust(width, fill)
        else:
            marker = marker.rjust(width, fill)

        return left + marker + right


class ReverseBar(Bar):
    '''A bar which has a marker that goes from right to left'''

    def __init__(self, marker='#', left='|', right='|', fill=' ',
                 fill_left=False, **kwargs):
        '''Creates a customizable progress bar.

        marker - string or updatable object to use as a marker
        left - string or updatable object to use as a left border
        right - string or updatable object to use as a right border
        fill - character to use for the empty part of the progress bar
        fill_left - whether to fill from the left or the right
        '''
        Bar.__init__(self, marker=marker, left=left, right=right, fill=fill,
                     fill_left=fill_left, **kwargs)


class BouncingBar(Bar, TimeSensitiveWidgetBase):
    '''A bar which has a marker which bounces from side to side.'''

    INTERVAL = datetime.timedelta(milliseconds=100)

    def __call__(self, progress, data, width):
        '''Updates the progress bar and its subcomponents'''

        left = converters.to_unicode(self.left(progress, data, width))
        right = converters.to_unicode(self.right(progress, data, width))
        width -= progress.custom_len(left) + progress.custom_len(right)
        marker = converters.to_unicode(self.marker(progress, data, width))

        fill = converters.to_unicode(self.fill(progress, data, width))

        if width:  # pragma: no branch
            value = int(
                data['total_seconds_elapsed'] / self.INTERVAL.total_seconds())

            a = value % width
            b = width - a - 1
            if value % (width * 2) >= width:
                a, b = b, a

            if self.fill_left:
                marker = a * fill + marker + b * fill
            else:
                marker = b * fill + marker + a * fill

        return left + marker + right


class FormatCustomText(FormatWidgetMixin, WidgetBase):
    mapping = {}
    copy = False

    def __init__(self, format, mapping=mapping, **kwargs):
        self.format = format
        self.mapping = mapping
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, **kwargs)

    def update_mapping(self, **mapping):
        self.mapping.update(mapping)

    def __call__(self, progress, data):
        return FormatWidgetMixin.__call__(
            self, progress, self.mapping, self.format)


class VariableMixin(object):
    '''Mixin to display a custom user variable '''

    def __init__(self, name, **kwargs):
        if not isinstance(name, six.string_types):
            raise TypeError('Variable(): argument must be a string')
        if len(name.split()) > 1:
            raise ValueError('Variable(): argument must be single word')
        self.name = name


class MultiRangeBar(Bar, VariableMixin):
    '''
    A bar with multiple sub-ranges, each represented by a different symbol

    The various ranges are represented on a user-defined variable, formatted as

    .. code-block:: python

        [
            ['Symbol1', amount1],
            ['Symbol2', amount2],
            ...
        ]
    '''

    def __init__(self, name, markers, **kwargs):
        VariableMixin.__init__(self, name)
        Bar.__init__(self, **kwargs)
        self.markers = [
            string_or_lambda(marker)
            for marker in markers
        ]

    def get_values(self, progress, data):
        return data['variables'][self.name] or []

    def __call__(self, progress, data, width):
        '''Updates the progress bar and its subcomponents'''

        left = converters.to_unicode(self.left(progress, data, width))
        right = converters.to_unicode(self.right(progress, data, width))
        width -= progress.custom_len(left) + progress.custom_len(right)
        values = self.get_values(progress, data)

        values_sum = sum(values)
        if width and values_sum:
            middle = ''
            values_accumulated = 0
            width_accumulated = 0
            for marker, value in zip(self.markers, values):
                marker = converters.to_unicode(marker(progress, data, width))
                assert progress.custom_len(marker) == 1

                values_accumulated += value
                item_width = int(values_accumulated / values_sum * width)
                item_width -= width_accumulated
                width_accumulated += item_width
                middle += item_width * marker
        else:
            fill = converters.to_unicode(self.fill(progress, data, width))
            assert progress.custom_len(fill) == 1
            middle = fill * width

        return left + middle + right


class MultiProgressBar(MultiRangeBar):
    def __init__(self,
                 name,
                 # NOTE: the markers are not whitespace even though some
                 # terminals don't show the characters correctly!
                 markers=' ▁▂▃▄▅▆▇█',
                 **kwargs):
        MultiRangeBar.__init__(self, name=name,
                               markers=list(reversed(markers)), **kwargs)

    def get_values(self, progress, data):
        ranges = [0] * len(self.markers)
        for progress in data['variables'][self.name] or []:
            if not isinstance(progress, (int, float)):
                # Progress is (value, max)
                progress_value, progress_max = progress
                progress = float(progress_value) / float(progress_max)

            if progress < 0 or progress > 1:
                raise ValueError(
                    'Range value needs to be in the range [0..1], got %s' %
                    progress)

            range_ = progress * (len(ranges) - 1)
            pos = int(range_)
            frac = range_ % 1
            ranges[pos] += (1 - frac)
            if (frac):
                ranges[pos + 1] += (frac)

        if self.fill_left:
            ranges = list(reversed(ranges))
        return ranges


class GranularMarkers:
    smooth = ' ▏▎▍▌▋▊▉█'
    bar = ' ▁▂▃▄▅▆▇█'
    snake = ' ▖▌▛█'
    fade_in = ' ░▒▓█'
    dots = ' ⡀⡄⡆⡇⣇⣧⣷⣿'
    growing_circles = ' .oO'


class GranularBar(AutoWidthWidgetBase):
    '''A progressbar that can display progress at a sub-character granularity
    by using multiple marker characters.

    Examples of markers:
     - Smooth: ` ▏▎▍▌▋▊▉█` (default)
     - Bar: ` ▁▂▃▄▅▆▇█`
     - Snake: ` ▖▌▛█`
     - Fade in: ` ░▒▓█`
     - Dots: ` ⡀⡄⡆⡇⣇⣧⣷⣿`
     - Growing circles: ` .oO`

    The markers can be accessed through GranularMarkers. GranularMarkers.dots
    for example
    '''

    def __init__(self, markers=GranularMarkers.smooth, left='|', right='|',
                 **kwargs):
        '''Creates a customizable progress bar.

        markers - string of characters to use as granular progress markers. The
                  first character should represent 0% and the last 100%.
                  Ex: ` .oO`.
        left - string or callable object to use as a left border
        right - string or callable object to use as a right border
        '''
        self.markers = markers
        self.left = string_or_lambda(left)
        self.right = string_or_lambda(right)

        AutoWidthWidgetBase.__init__(self, **kwargs)

    def __call__(self, progress, data, width):
        left = converters.to_unicode(self.left(progress, data, width))
        right = converters.to_unicode(self.right(progress, data, width))
        width -= progress.custom_len(left) + progress.custom_len(right)

        if progress.max_value is not base.UnknownLength \
                and progress.max_value > 0:
            percent = progress.value / progress.max_value
        else:
            percent = 0

        num_chars = percent * width

        marker = self.markers[-1] * int(num_chars)

        marker_idx = int((num_chars % 1) * (len(self.markers) - 1))
        if marker_idx:
            marker += self.markers[marker_idx]

        marker = converters.to_unicode(marker)

        # Make sure we ignore invisible characters when filling
        width += len(marker) - progress.custom_len(marker)
        marker = marker.ljust(width, self.markers[0])

        return left + marker + right


class FormatLabelBar(FormatLabel, Bar):
    '''A bar which has a formatted label in the center.'''
    def __init__(self, format, **kwargs):
        FormatLabel.__init__(self, format, **kwargs)
        Bar.__init__(self, **kwargs)

    def __call__(self, progress, data, width, format=None):
        center = FormatLabel.__call__(self, progress, data, format=format)
        bar = Bar.__call__(self, progress, data, width)

        # Aligns the center of the label to the center of the bar
        center_len = progress.custom_len(center)
        center_left = int((width - center_len) / 2)
        center_right = center_left + center_len
        return bar[:center_left] + center + bar[center_right:]


class PercentageLabelBar(Percentage, FormatLabelBar):
    '''A bar which displays the current percentage in the center.'''
    # %3d adds an extra space that makes it look off-center
    # %2d keeps the label somewhat consistently in-place
    def __init__(self, format='%(percentage)2d%%', na='N/A%%', **kwargs):
        Percentage.__init__(self, format, na=na, **kwargs)
        FormatLabelBar.__init__(self, format, **kwargs)


class Variable(FormatWidgetMixin, VariableMixin, WidgetBase):
    '''Displays a custom variable.'''

    def __init__(self, name, format='{name}: {formatted_value}',
                 width=6, precision=3, **kwargs):
        '''Creates a Variable associated with the given name.'''
        self.format = format
        self.width = width
        self.precision = precision
        VariableMixin.__init__(self, name=name)
        WidgetBase.__init__(self, **kwargs)

    def __call__(self, progress, data):
        value = data['variables'][self.name]
        context = data.copy()
        context['value'] = value
        context['name'] = self.name
        context['width'] = self.width
        context['precision'] = self.precision

        try:
            # Make sure to try and cast the value first, otherwise the
            # formatting will generate warnings/errors on newer Python releases
            value = float(value)
            fmt = '{value:{width}.{precision}}'
            context['formatted_value'] = fmt.format(**context)
        except (TypeError, ValueError):
            if value:
                context['formatted_value'] = '{value:{width}}'.format(
                    **context)
            else:
                context['formatted_value'] = '-' * self.width

        return self.format.format(**context)


class DynamicMessage(Variable):
    '''Kept for backwards compatibility, please use `Variable` instead.'''
    pass


class CurrentTime(FormatWidgetMixin, TimeSensitiveWidgetBase):
    '''Widget which displays the current (date)time with seconds resolution.'''
    INTERVAL = datetime.timedelta(seconds=1)

    def __init__(self, format='Current Time: %(current_time)s',
                 microseconds=False, **kwargs):
        self.microseconds = microseconds
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        TimeSensitiveWidgetBase.__init__(self, **kwargs)

    def __call__(self, progress, data):
        data['current_time'] = self.current_time()
        data['current_datetime'] = self.current_datetime()

        return FormatWidgetMixin.__call__(self, progress, data)

    def current_datetime(self):
        now = datetime.datetime.now()
        if not self.microseconds:
            now = now.replace(microsecond=0)

        return now

    def current_time(self):
        return self.current_datetime().time()

