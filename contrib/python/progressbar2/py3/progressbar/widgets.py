from __future__ import annotations

import abc
import contextlib
import datetime
import functools
import logging
import typing

# Ruff is being stupid and doesn't understand `ClassVar` if it comes from the
# `types` module
from typing import ClassVar

from python_utils import containers, converters, types

from . import algorithms, base, terminal, utils
from .terminal import colors

if types.TYPE_CHECKING:
    from .bar import NumberT, ProgressBarMixinBase

logger = logging.getLogger(__name__)

MAX_DATE = datetime.date.max
MAX_TIME = datetime.time.max
MAX_DATETIME = datetime.datetime.max

Data = types.Dict[str, types.Any]
FormatString = typing.Optional[str]

T = typing.TypeVar('T')


def string_or_lambda(input_):
    if isinstance(input_, str):

        def render_input(progress, data, width):
            return input_ % data

        return render_input
    else:
        return input_


def create_wrapper(wrapper):
    """Convert a wrapper tuple or format string to a format string.

    >>> create_wrapper('')

    >>> print(create_wrapper('a{}b'))
    a{}b

    >>> print(create_wrapper(('a', 'b')))
    a{}b
    """
    if isinstance(wrapper, tuple) and len(wrapper) == 2:
        a, b = wrapper
        wrapper = (a or '') + '{}' + (b or '')
    elif not wrapper:
        return None

    if isinstance(wrapper, str):
        assert '{}' in wrapper, 'Expected string with {} for formatting'
    else:
        raise RuntimeError(  # noqa: TRY004
            'Pass either a begin/end string as a tuple or a template string '
            'with `{}`',
        )

    return wrapper


def wrapper(function, wrapper_):
    """Wrap the output of a function in a template string or a tuple with
    begin/end strings.

    """
    wrapper_ = create_wrapper(wrapper_)
    if not wrapper_:
        return function

    @functools.wraps(function)
    def wrap(*args, **kwargs):
        return wrapper_.format(function(*args, **kwargs))

    return wrap


def create_marker(marker, wrap=None):
    def _marker(progress, data, width):
        if (
            progress.max_value is not base.UnknownLength
            and progress.max_value > 0
        ):
            length = int(progress.value / progress.max_value * width)
            return marker * length
        else:
            return marker

    if isinstance(marker, str):
        marker = converters.to_unicode(marker)
        # Ruff is silly at times... the format is not compatible with the check
        marker_length_error = 'Markers are required to be 1 char'
        assert utils.len_color(marker) == 1, marker_length_error
        return wrapper(_marker, wrap)
    else:
        return wrapper(marker, wrap)


class FormatWidgetMixin(abc.ABC):
    """Mixin to format widgets using a formatstring.

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
    """

    def __init__(self, format: str, new_style: bool = False, **kwargs):
        self.new_style = new_style
        self.format = format

    def get_format(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        format: types.Optional[str] = None,
    ) -> str:
        return format or self.format

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        format: types.Optional[str] = None,
    ) -> str:
        """Formats the widget into a string."""
        format_ = self.get_format(progress, data, format)
        try:
            if self.new_style:
                return format_.format(**data)
            else:
                return format_ % data
        except (TypeError, KeyError):
            logger.exception(
                'Error while formatting %r with data: %r',
                format_,
                data,
            )
            raise


class WidthWidgetMixin(abc.ABC):
    """Mixing to make sure widgets are only visible if the screen is within a
    specified size range so the progressbar fits on both large and small
    screens.

    Variables available:
     - min_width: Only display the widget if at least `min_width` is left
     - max_width: Only display the widget if at most `max_width` is left

    >>> class Progress:
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
    """

    def __init__(self, min_width=None, max_width=None, **kwargs):
        self.min_width = min_width
        self.max_width = max_width

    def check_size(self, progress: ProgressBarMixinBase):
        max_width = self.max_width
        min_width = self.min_width
        if min_width and min_width > progress.term_width:
            return False
        elif max_width and max_width < progress.term_width:  # noqa: SIM103
            return False
        else:
            return True


class TGradientColors(typing.TypedDict):
    fg: types.Optional[terminal.OptionalColor | None]
    bg: types.Optional[terminal.OptionalColor | None]


class TFixedColors(typing.TypedDict):
    fg_none: types.Optional[terminal.Color | None]
    bg_none: types.Optional[terminal.Color | None]


class WidgetBase(WidthWidgetMixin, metaclass=abc.ABCMeta):
    """The base class for all widgets.

    The ProgressBar will call the widget's update value when the widget should
    be updated. The widget's size may change between calls, but the widget may
    display incorrectly if the size changes drastically and repeatedly.

    The INTERVAL timedelta informs the ProgressBar that it should be
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
     - weight: Widgets with a higher `weight` will be calculated before widgets
       with a lower one
     - copy: Copy this widget when initializing the progress bar so the
       progressbar can be reused. Some widgets such as the FormatCustomText
       require the shared state so this needs to be optional

    """

    copy = True

    @abc.abstractmethod
    def __call__(self, progress: ProgressBarMixinBase, data: Data) -> str:
        """Updates the widget.

        progress - a reference to the calling ProgressBar
        """

    _fixed_colors: ClassVar[TFixedColors] = TFixedColors(
        fg_none=None,
        bg_none=None,
    )
    _gradient_colors: ClassVar[TGradientColors] = TGradientColors(
        fg=None,
        bg=None,
    )
    # _fixed_colors: ClassVar[dict[str, terminal.Color | None]] = dict()
    # _gradient_colors: ClassVar[dict[str, terminal.OptionalColor | None]] = (
    #     dict())
    _len: typing.Callable[[str | bytes], int] = len

    @functools.cached_property
    def uses_colors(self):
        for value in self._gradient_colors.values():  # pragma: no branch
            if value is not None:  # pragma: no branch
                return True

        return any(value is not None for value in self._fixed_colors.values())

    def _apply_colors(self, text: str, data: Data) -> str:
        if self.uses_colors:
            return terminal.apply_colors(
                text,
                data.get('percentage'),
                **self._gradient_colors,
                **self._fixed_colors,
            )
        else:
            return text

    def __init__(
        self,
        *args,
        fixed_colors=None,
        gradient_colors=None,
        **kwargs,
    ):
        if fixed_colors is not None:
            self._fixed_colors.update(fixed_colors)

        if gradient_colors is not None:
            self._gradient_colors.update(gradient_colors)

        if self.uses_colors:
            self._len = utils.len_color

        super().__init__(*args, **kwargs)


class AutoWidthWidgetBase(WidgetBase, metaclass=abc.ABCMeta):
    """The base class for all variable width widgets.

    This widget is much like the \\hfill command in TeX, it will expand to
    fill the line. You can use more than one in the same line, and they will
    all have the same width, and together will fill the line.
    """

    @abc.abstractmethod
    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        width: int = 0,
    ) -> str:
        """Updates the widget providing the total width the widget must fill.

        progress - a reference to the calling ProgressBar
        width - The total width the widget must fill
        """


class TimeSensitiveWidgetBase(WidgetBase, metaclass=abc.ABCMeta):
    """The base class for all time sensitive widgets.

    Some widgets like timers would become out of date unless updated at least
    every `INTERVAL`
    """

    INTERVAL = datetime.timedelta(milliseconds=100)


class FormatLabel(FormatWidgetMixin, WidgetBase):
    """Displays a formatted label.

    >>> label = FormatLabel('%(value)s', min_width=5, max_width=10)
    >>> class Progress:
    ...     pass
    >>> label = FormatLabel('{value} :: {value:^6}', new_style=True)
    >>> str(label(Progress, dict(value='test')))
    'test ::  test '

    """

    mapping: ClassVar[types.Dict[str, types.Tuple[str, types.Any]]] = dict(
        finished=('end_time', None),
        last_update=('last_update_time', None),
        max=('max_value', None),
        seconds=('seconds_elapsed', None),
        start=('start_time', None),
        elapsed=('total_seconds_elapsed', utils.format_time),
        value=('value', None),
    )

    def __init__(self, format: str, **kwargs):
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, **kwargs)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        format: types.Optional[str] = None,
    ):
        for name, (key, transform) in self.mapping.items():
            with contextlib.suppress(KeyError, ValueError, IndexError):
                if transform is None:
                    data[name] = data[key]
                else:
                    data[name] = transform(data[key])

        return FormatWidgetMixin.__call__(self, progress, data, format)


class Timer(FormatLabel, TimeSensitiveWidgetBase):
    """WidgetBase which displays the elapsed seconds."""

    def __init__(self, format='Elapsed Time: %(elapsed)s', **kwargs):
        if '%s' in format and '%(elapsed)s' not in format:
            format = format.replace('%s', '%(elapsed)s')

        FormatLabel.__init__(self, format=format, **kwargs)
        TimeSensitiveWidgetBase.__init__(self, **kwargs)

    # This is exposed as a static method for backwards compatibility
    format_time = staticmethod(utils.format_time)


class SamplesMixin(TimeSensitiveWidgetBase, metaclass=abc.ABCMeta):
    """
    Mixing for widgets that average multiple measurements.

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
    SliceableDeque([1, 1])

    >>> samples(progress, None, True) == (datetime.timedelta(seconds=1), 0)
    True
    """

    def __init__(
        self,
        samples=datetime.timedelta(seconds=2),
        key_prefix=None,
        **kwargs,
    ):
        self.samples = samples
        self.key_prefix = (key_prefix or self.__class__.__name__) + '_'
        TimeSensitiveWidgetBase.__init__(self, **kwargs)

    def get_sample_times(self, progress: ProgressBarMixinBase, data: Data):
        return progress.extra.setdefault(
            f'{self.key_prefix}sample_times',
            containers.SliceableDeque(),
        )

    def get_sample_values(self, progress: ProgressBarMixinBase, data: Data):
        return progress.extra.setdefault(
            f'{self.key_prefix}sample_values',
            containers.SliceableDeque(),
        )

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        delta: bool = False,
    ):
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
                while (
                    sample_times[2:]
                    and minimum_time > sample_times[1]
                    and minimum_value > sample_values[1]
                ):
                    sample_times.pop(0)
                    sample_values.pop(0)
            elif len(sample_times) > self.samples:
                sample_times.pop(0)
                sample_values.pop(0)

        if delta:
            if delta_time := sample_times[-1] - sample_times[0]:
                delta_value = sample_values[-1] - sample_values[0]
                return delta_time, delta_value
            else:
                return None, None
        else:
            return sample_times, sample_values


class ETA(Timer):
    """WidgetBase which attempts to estimate the time of arrival."""

    def __init__(
        self,
        format_not_started='ETA:  --:--:--',
        format_finished='Time: %(elapsed)8s',
        format='ETA:  %(eta)8s',
        format_zero='ETA:  00:00:00',
        format_na='ETA:      N/A',
        **kwargs,
    ):
        if '%s' in format and '%(eta)s' not in format:
            format = format.replace('%s', '%(eta)s')

        Timer.__init__(self, **kwargs)
        self.format_not_started = format_not_started
        self.format_finished = format_finished
        self.format = format
        self.format_zero = format_zero
        self.format_NA = format_na

    def _calculate_eta(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        value,
        elapsed,
    ):
        """Updates the widget to show the ETA or total time when finished."""
        if elapsed:
            # The max() prevents zero division errors
            per_item = elapsed.total_seconds() / max(value, 1e-6)
            remaining = progress.max_value - data['value']
            return remaining * per_item
        else:
            return 0

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        value=None,
        elapsed=None,
    ):
        """Updates the widget to show the ETA or total time when finished."""
        if value is None:
            value = data['value']

        if elapsed is None:
            elapsed = data['time_elapsed']

        eta_na = False
        try:
            data['eta_seconds'] = self._calculate_eta(
                progress,
                data,
                value=value,
                elapsed=elapsed,
            )
        except TypeError:
            data['eta_seconds'] = None
            eta_na = True

        data['eta'] = None
        if data['eta_seconds']:
            with contextlib.suppress(ValueError, OverflowError, OSError):
                data['eta'] = utils.format_time(data['eta_seconds'])

        if data['value'] == progress.min_value:
            fmt = self.format_not_started
        elif progress.end_time:
            fmt = self.format_finished
        elif data['eta']:
            fmt = self.format
        elif eta_na:
            fmt = self.format_NA
        else:
            fmt = self.format_zero

        return Timer.__call__(self, progress, data, format=fmt)


class AbsoluteETA(ETA):
    """Widget which attempts to estimate the absolute time of arrival."""

    def _calculate_eta(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        value,
        elapsed,
    ):
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
        **kwargs,
    ):
        ETA.__init__(
            self,
            format_not_started=format_not_started,
            format_finished=format_finished,
            format=format,
            **kwargs,
        )


class AdaptiveETA(ETA, SamplesMixin):
    """WidgetBase which attempts to estimate the time of arrival.

    Uses a sampled average of the speed based on the 10 last updates.
    Very convenient for resuming the progress halfway.
    """

    exponential_smoothing: bool
    exponential_smoothing_factor: float

    def __init__(
        self,
        exponential_smoothing=True,
        exponential_smoothing_factor=0.1,
        **kwargs,
    ):
        self.exponential_smoothing = exponential_smoothing
        self.exponential_smoothing_factor = exponential_smoothing_factor
        ETA.__init__(self, **kwargs)
        SamplesMixin.__init__(self, **kwargs)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        value=None,
        elapsed=None,
    ):
        elapsed, value = SamplesMixin.__call__(
            self,
            progress,
            data,
            delta=True,
        )
        if not elapsed:
            value = None
            elapsed = 0

        return ETA.__call__(self, progress, data, value=value, elapsed=elapsed)


class SmoothingETA(ETA):
    """
    WidgetBase which attempts to estimate the time of arrival using an
    exponential moving average (EMA) of the speed.

    EMA applies more weight to recent data points and less to older ones,
    and doesn't require storing all past values. This approach works well
    with varying data points and smooths out fluctuations effectively.
    """

    smoothing_algorithm: algorithms.SmoothingAlgorithm
    smoothing_parameters: dict[str, float]

    def __init__(
        self,
        smoothing_algorithm: type[
            algorithms.SmoothingAlgorithm
        ] = algorithms.ExponentialMovingAverage,
        smoothing_parameters: dict[str, float] | None = None,
        **kwargs,
    ):
        self.smoothing_parameters = smoothing_parameters or {}
        self.smoothing_algorithm = smoothing_algorithm(
            **(self.smoothing_parameters or {}),
        )
        ETA.__init__(self, **kwargs)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        value=None,
        elapsed=None,
    ):
        if value is None:  # pragma: no branch
            value = data['value']

        if elapsed is None:  # pragma: no branch
            elapsed = data['time_elapsed']

        self.smoothing_algorithm.update(value, elapsed)
        return ETA.__call__(self, progress, data, value=value, elapsed=elapsed)


class DataSize(FormatWidgetMixin, WidgetBase):
    """
    Widget for showing an amount of data transferred/processed.

    Automatically formats the value (assumed to be a count of bytes) with an
    appropriate sized unit, based on the IEC binary prefixes (powers of 1024).
    """

    def __init__(
        self,
        variable='value',
        format='%(scaled)5.1f %(prefix)s%(unit)s',
        unit='B',
        prefixes=('', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
        **kwargs,
    ):
        self.variable = variable
        self.unit = unit
        self.prefixes = prefixes
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, **kwargs)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        format: types.Optional[str] = None,
    ):
        value = data[self.variable]
        if value is not None:
            scaled, power = utils.scale_1024(value, len(self.prefixes))
        else:
            scaled = power = 0

        data['scaled'] = scaled
        data['prefix'] = self.prefixes[power]
        data['unit'] = self.unit

        return FormatWidgetMixin.__call__(self, progress, data, format)


class FileTransferSpeed(FormatWidgetMixin, TimeSensitiveWidgetBase):
    """
    Widget for showing the current transfer speed (useful for file transfers).
    """

    def __init__(
        self,
        format='%(scaled)5.1f %(prefix)s%(unit)-s/s',
        inverse_format='%(scaled)5.1f s/%(prefix)s%(unit)-s',
        unit='B',
        prefixes=('', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
        **kwargs,
    ):
        self.unit = unit
        self.prefixes = prefixes
        self.inverse_format = inverse_format
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        TimeSensitiveWidgetBase.__init__(self, **kwargs)

    def _speed(self, value, elapsed):
        speed = float(value) / elapsed
        return utils.scale_1024(speed, len(self.prefixes))

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data,
        value=None,
        total_seconds_elapsed=None,
    ):
        """Updates the widget with the current SI prefixed speed."""
        if value is None:
            value = data['value']

        elapsed = utils.deltas_to_seconds(
            total_seconds_elapsed,
            data['total_seconds_elapsed'],
        )

        if (
            value is not None
            and elapsed is not None
            and elapsed > 2e-6
            and value > 2e-6
        ):  # =~ 0
            scaled, power = self._speed(value, elapsed)
        else:
            scaled = power = 0

        data['unit'] = self.unit
        if power == 0 and scaled < 0.1:
            if scaled > 0:
                scaled = 1 / scaled
            data['scaled'] = scaled
            data['prefix'] = self.prefixes[0]
            return FormatWidgetMixin.__call__(
                self,
                progress,
                data,
                self.inverse_format,
            )
        else:
            data['scaled'] = scaled
            data['prefix'] = self.prefixes[power]
            return FormatWidgetMixin.__call__(self, progress, data)


class AdaptiveTransferSpeed(FileTransferSpeed, SamplesMixin):
    """Widget for showing the transfer speed based on the last X samples."""

    def __init__(self, **kwargs):
        FileTransferSpeed.__init__(self, **kwargs)
        SamplesMixin.__init__(self, **kwargs)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data,
        value=None,
        total_seconds_elapsed=None,
    ):
        elapsed, value = SamplesMixin.__call__(
            self,
            progress,
            data,
            delta=True,
        )
        return FileTransferSpeed.__call__(self, progress, data, value, elapsed)


class AnimatedMarker(TimeSensitiveWidgetBase):
    """An animated marker for the progress bar which defaults to appear as if
    it were rotating.
    """

    def __init__(
        self,
        markers='|/-\\',
        default=None,
        fill='',
        marker_wrap=None,
        fill_wrap=None,
        **kwargs,
    ):
        self.markers = markers
        self.marker_wrap = create_wrapper(marker_wrap)
        self.default = default or markers[0]
        self.fill_wrap = create_wrapper(fill_wrap)
        self.fill = create_marker(fill, self.fill_wrap) if fill else None
        WidgetBase.__init__(self, **kwargs)

    def __call__(self, progress: ProgressBarMixinBase, data: Data, width=None):
        """Updates the widget to show the next marker or the first marker when
        finished.
        """
        if progress.end_time:
            return self.default

        marker = self.markers[data['updates'] % len(self.markers)]
        if self.marker_wrap:
            marker = self.marker_wrap.format(marker)

        if self.fill:
            # Cut the last character so we can replace it with our marker
            fill = self.fill(
                progress,
                data,
                width - progress.custom_len(marker),  # type: ignore
            )
        else:
            fill = ''

        # Python 3 returns an int when indexing bytes
        if isinstance(marker, int):  # pragma: no cover
            marker = bytes(marker)
            fill = fill.encode()
        else:
            # cast fill to the same type as marker
            fill = type(marker)(fill)

        return fill + marker  # type: ignore


# Alias for backwards compatibility
RotatingMarker = AnimatedMarker


class Counter(FormatWidgetMixin, WidgetBase):
    """Displays the current count."""

    def __init__(self, format='%(value)d', **kwargs):
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, format=format, **kwargs)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        format=None,
    ):
        return FormatWidgetMixin.__call__(self, progress, data, format)


class ColoredMixin:
    _fixed_colors: ClassVar[TFixedColors] = TFixedColors(
        fg_none=colors.yellow,
        bg_none=None,
    )
    _gradient_colors: ClassVar[TGradientColors] = TGradientColors(
        fg=colors.gradient,
        bg=None,
    )
    # _fixed_colors: ClassVar[dict[str, terminal.Color | None]] = dict(
    #     fg_none=colors.yellow, bg_none=None)
    # _gradient_colors: ClassVar[dict[str, terminal.OptionalColor |
    #                                      None]] = dict(fg=colors.gradient,
    #                                                    bg=None)


class Percentage(FormatWidgetMixin, ColoredMixin, WidgetBase):
    """Displays the current percentage as a number with a percent sign."""

    def __init__(self, format='%(percentage)3d%%', na='N/A%%', **kwargs):
        self.na = na
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, format=format, **kwargs)

    def get_format(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        format=None,
    ):
        # If percentage is not available, display N/A%
        percentage = data.get('percentage', base.Undefined)
        if not percentage and percentage != 0:
            output = self.na
        else:
            output = FormatWidgetMixin.get_format(self, progress, data, format)

        return self._apply_colors(output, data)


class SimpleProgress(FormatWidgetMixin, ColoredMixin, WidgetBase):
    """Returns progress as a count of the total (e.g.: "5 of 47")."""

    max_width_cache: dict[
        str
        | tuple[
            NumberT | types.Type[base.UnknownLength] | None,
            NumberT | types.Type[base.UnknownLength] | None,
        ],
        types.Optional[int],
    ]

    DEFAULT_FORMAT = '%(value_s)s of %(max_value_s)s'

    def __init__(self, format=DEFAULT_FORMAT, **kwargs):
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, format=format, **kwargs)
        self.max_width_cache = dict()
        # Pyright isn't happy when we set the key in the initialiser
        self.max_width_cache['default'] = self.max_width or 0

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        format=None,
    ):
        # If max_value is not available, display N/A
        if data.get('max_value'):
            data['max_value_s'] = data['max_value']
        else:
            data['max_value_s'] = 'N/A'

        # if value is not available it's the zeroth iteration
        if data.get('value'):
            data['value_s'] = data['value']
        else:
            data['value_s'] = 0

        formatted = FormatWidgetMixin.__call__(
            self,
            progress,
            data,
            format=format,
        )

        # Guess the maximum width from the min and max value
        key = progress.min_value, progress.max_value
        max_width: types.Optional[int] = self.max_width_cache.get(
            key,
            self.max_width,
        )
        if not max_width:
            temporary_data = data.copy()
            for value in key:
                if value is None:  # pragma: no cover
                    continue

                temporary_data['value'] = value
                if width := progress.custom_len(  # pragma: no branch
                    FormatWidgetMixin.__call__(
                        self,
                        progress,
                        temporary_data,
                        format=format,
                    ),
                ):
                    max_width = max(max_width or 0, width)

            self.max_width_cache[key] = max_width

        # Adjust the output to have a consistent size in all cases
        if max_width:  # pragma: no branch
            formatted = formatted.rjust(max_width)

        return self._apply_colors(formatted, data)


class Bar(AutoWidthWidgetBase):
    """A progress bar which stretches to fill the line."""

    fg: terminal.OptionalColor | None = colors.gradient
    bg: terminal.OptionalColor | None = None

    def __init__(
        self,
        marker='#',
        left='|',
        right='|',
        fill=' ',
        fill_left=True,
        marker_wrap=None,
        **kwargs,
    ):
        """Creates a customizable progress bar.

        The callable takes the same parameters as the `__call__` method

        marker - string or callable object to use as a marker
        left - string or callable object to use as a left border
        right - string or callable object to use as a right border
        fill - character to use for the empty part of the progress bar
        fill_left - whether to fill from the left or the right
        """
        self.marker = create_marker(marker, marker_wrap)
        self.left = string_or_lambda(left)
        self.right = string_or_lambda(right)
        self.fill = string_or_lambda(fill)
        self.fill_left = fill_left

        AutoWidthWidgetBase.__init__(self, **kwargs)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        width: int = 0,
        color=True,
    ):
        """Updates the progress bar and its subcomponents."""
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

        if color:
            marker = self._apply_colors(marker, data)

        return left + marker + right


class ReverseBar(Bar):
    """A bar which has a marker that goes from right to left."""

    def __init__(
        self,
        marker='#',
        left='|',
        right='|',
        fill=' ',
        fill_left=False,
        **kwargs,
    ):
        """Creates a customizable progress bar.

        marker - string or updatable object to use as a marker
        left - string or updatable object to use as a left border
        right - string or updatable object to use as a right border
        fill - character to use for the empty part of the progress bar
        fill_left - whether to fill from the left or the right
        """
        Bar.__init__(
            self,
            marker=marker,
            left=left,
            right=right,
            fill=fill,
            fill_left=fill_left,
            **kwargs,
        )


class BouncingBar(Bar, TimeSensitiveWidgetBase):
    """A bar which has a marker which bounces from side to side."""

    INTERVAL = datetime.timedelta(milliseconds=100)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        width: int = 0,
        color=True,
    ):
        """Updates the progress bar and its subcomponents."""
        left = converters.to_unicode(self.left(progress, data, width))
        right = converters.to_unicode(self.right(progress, data, width))
        width -= progress.custom_len(left) + progress.custom_len(right)
        marker = converters.to_unicode(self.marker(progress, data, width))

        fill = converters.to_unicode(self.fill(progress, data, width))

        if width:  # pragma: no branch
            value = int(
                data['total_seconds_elapsed'] / self.INTERVAL.total_seconds(),
            )

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
    mapping: types.Dict[str, types.Any] = dict()  # noqa: RUF012
    copy = False

    def __init__(
        self,
        format: str,
        mapping: types.Optional[types.Dict[str, types.Any]] = None,
        **kwargs,
    ):
        self.format = format
        self.mapping = mapping or self.mapping
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        WidgetBase.__init__(self, **kwargs)

    def update_mapping(self, **mapping: types.Dict[str, types.Any]):
        self.mapping.update(mapping)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        format: types.Optional[str] = None,
    ):
        return FormatWidgetMixin.__call__(
            self,
            progress,
            self.mapping,
            format or self.format,
        )


class VariableMixin:
    """Mixin to display a custom user variable."""

    def __init__(self, name, **kwargs):
        if not isinstance(name, str):
            raise TypeError('Variable(): argument must be a string')
        if len(name.split()) > 1:
            raise ValueError('Variable(): argument must be single word')
        self.name = name


class MultiRangeBar(Bar, VariableMixin):
    """
    A bar with multiple sub-ranges, each represented by a different symbol.

    The various ranges are represented on a user-defined variable, formatted as

    .. code-block:: python

        [['Symbol1', amount1], ['Symbol2', amount2], ...]
    """

    def __init__(self, name, markers, **kwargs):
        VariableMixin.__init__(self, name)
        Bar.__init__(self, **kwargs)
        self.markers = [string_or_lambda(marker) for marker in markers]

    def get_values(self, progress: ProgressBarMixinBase, data: Data):
        return data['variables'][self.name] or []

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        width: int = 0,
        color=True,
    ):
        """Updates the progress bar and its subcomponents."""
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
    def __init__(
        self,
        name,
        # NOTE: the markers are not whitespace even though some
        # terminals don't show the characters correctly!
        markers=' ▁▂▃▄▅▆▇█',
        **kwargs,
    ):
        MultiRangeBar.__init__(
            self,
            name=name,
            markers=list(reversed(markers)),
            **kwargs,
        )

    def get_values(self, progress: ProgressBarMixinBase, data: Data):
        ranges = [0.0] * len(self.markers)
        for value in data['variables'][self.name] or []:
            if not isinstance(value, (int, float)):
                # Progress is (value, max)
                progress_value, progress_max = value
                value = float(progress_value) / float(progress_max)

            if not 0 <= value <= 1:
                raise ValueError(
                    'Range value needs to be in the range [0..1], '
                    f'got {value}',
                )

            range_ = value * (len(ranges) - 1)
            pos = int(range_)
            frac = range_ % 1
            ranges[pos] += 1 - frac
            if frac:
                ranges[pos + 1] += frac

        if self.fill_left:  # pragma: no branch
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
    """A progressbar that can display progress at a sub-character granularity
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
    """

    def __init__(
        self,
        markers=GranularMarkers.smooth,
        left='|',
        right='|',
        **kwargs,
    ):
        """Creates a customizable progress bar.

        markers - string of characters to use as granular progress markers. The
                  first character should represent 0% and the last 100%.
                  Ex: ` .oO`.
        left - string or callable object to use as a left border
        right - string or callable object to use as a right border
        """
        self.markers = markers
        self.left = string_or_lambda(left)
        self.right = string_or_lambda(right)

        AutoWidthWidgetBase.__init__(self, **kwargs)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        width: int = 0,
    ):
        left = converters.to_unicode(self.left(progress, data, width))
        right = converters.to_unicode(self.right(progress, data, width))
        width -= progress.custom_len(left) + progress.custom_len(right)

        max_value = progress.max_value
        if (
            max_value is not base.UnknownLength
            and typing.cast(float, max_value) > 0
        ):
            percent = progress.value / max_value  # type: ignore
        else:
            percent = 0

        num_chars = percent * width

        marker = self.markers[-1] * int(num_chars)

        if marker_idx := int((num_chars % 1) * (len(self.markers) - 1)):
            marker += self.markers[marker_idx]

        marker = converters.to_unicode(marker)

        # Make sure we ignore invisible characters when filling
        width += len(marker) - progress.custom_len(marker)
        marker = marker.ljust(width, self.markers[0])

        return left + marker + right


class FormatLabelBar(FormatLabel, Bar):
    """A bar which has a formatted label in the center."""

    def __init__(self, format, **kwargs):
        FormatLabel.__init__(self, format, **kwargs)
        Bar.__init__(self, **kwargs)

    def __call__(  # type: ignore
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        width: int = 0,
        format: FormatString = None,
    ):
        center = FormatLabel.__call__(self, progress, data, format=format)
        bar = Bar.__call__(self, progress, data, width, color=False)

        # Aligns the center of the label to the center of the bar
        center_len = progress.custom_len(center)
        center_left = int((width - center_len) / 2)
        center_right = center_left + center_len

        return (
            self._apply_colors(
                bar[:center_left],
                data,
            )
            + self._apply_colors(
                center,
                data,
            )
            + self._apply_colors(
                bar[center_right:],
                data,
            )
        )


class PercentageLabelBar(Percentage, FormatLabelBar):
    """A bar which displays the current percentage in the center."""

    # %3d adds an extra space that makes it look off-center
    # %2d keeps the label somewhat consistently in-place
    def __init__(self, format='%(percentage)2d%%', na='N/A%%', **kwargs):
        Percentage.__init__(self, format, na=na, **kwargs)
        FormatLabelBar.__init__(self, format, **kwargs)

    def __call__(  # type: ignore
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        width: int = 0,
        format: FormatString = None,
    ):
        return super().__call__(progress, data, width, format=format)


class Variable(FormatWidgetMixin, VariableMixin, WidgetBase):
    """Displays a custom variable."""

    def __init__(
        self,
        name,
        format='{name}: {formatted_value}',
        width=6,
        precision=3,
        **kwargs,
    ):
        """Creates a Variable associated with the given name."""
        self.format = format
        self.width = width
        self.precision = precision
        VariableMixin.__init__(self, name=name)
        WidgetBase.__init__(self, **kwargs)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        format: types.Optional[str] = None,
    ):
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
                    **context,
                )
            else:
                context['formatted_value'] = '-' * self.width

        return self.format.format(**context)


class DynamicMessage(Variable):
    """Kept for backwards compatibility, please use `Variable` instead."""


class CurrentTime(FormatWidgetMixin, TimeSensitiveWidgetBase):
    """Widget which displays the current (date)time with seconds resolution."""

    INTERVAL = datetime.timedelta(seconds=1)

    def __init__(
        self,
        format='Current Time: %(current_time)s',
        microseconds=False,
        **kwargs,
    ):
        self.microseconds = microseconds
        FormatWidgetMixin.__init__(self, format=format, **kwargs)
        TimeSensitiveWidgetBase.__init__(self, **kwargs)

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        format: types.Optional[str] = None,
    ):
        data['current_time'] = self.current_time()
        data['current_datetime'] = self.current_datetime()

        return FormatWidgetMixin.__call__(self, progress, data, format=format)

    def current_datetime(self):
        now = datetime.datetime.now()
        if not self.microseconds:
            now = now.replace(microsecond=0)

        return now

    def current_time(self):
        return self.current_datetime().time()


class JobStatusBar(Bar, VariableMixin):
    """
    Widget which displays the job status as markers on the bar.

    The status updates can be given either as a boolean or as a string. If it's
    a string, it will be displayed as-is. If it's a boolean, it will be
    displayed as a marker (default: '█' for success, 'X' for failure)
    configurable through the `success_marker` and `failure_marker` parameters.

    Args:
        name: The name of the variable to use for the status updates.
        left: The left border of the bar.
        right: The right border of the bar.
        fill: The fill character of the bar.
        fill_left: Whether to fill the bar from the left or the right.
        success_fg_color: The foreground color to use for successful jobs.
        success_bg_color: The background color to use for successful jobs.
        success_marker: The marker to use for successful jobs.
        failure_fg_color: The foreground color to use for failed jobs.
        failure_bg_color: The background color to use for failed jobs.
        failure_marker: The marker to use for failed jobs.
    """

    success_fg_color: terminal.Color | None = colors.green
    success_bg_color: terminal.Color | None = None
    success_marker: str = '█'
    failure_fg_color: terminal.Color | None = colors.red
    failure_bg_color: terminal.Color | None = None
    failure_marker: str = 'X'
    job_markers: list[str]

    def __init__(
        self,
        name: str,
        left='|',
        right='|',
        fill=' ',
        fill_left=True,
        success_fg_color=colors.green,
        success_bg_color=None,
        success_marker='█',
        failure_fg_color=colors.red,
        failure_bg_color=None,
        failure_marker='X',
        **kwargs,
    ):
        VariableMixin.__init__(self, name)
        self.name = name
        self.job_markers = []
        self.left = string_or_lambda(left)
        self.right = string_or_lambda(right)
        self.fill = string_or_lambda(fill)
        self.success_fg_color = success_fg_color
        self.success_bg_color = success_bg_color
        self.success_marker = success_marker
        self.failure_fg_color = failure_fg_color
        self.failure_bg_color = failure_bg_color
        self.failure_marker = failure_marker

        Bar.__init__(
            self,
            left=left,
            right=right,
            fill=fill,
            fill_left=fill_left,
            **kwargs,
        )

    def __call__(
        self,
        progress: ProgressBarMixinBase,
        data: Data,
        width: int = 0,
        color=True,
    ):
        left = converters.to_unicode(self.left(progress, data, width))
        right = converters.to_unicode(self.right(progress, data, width))
        width -= progress.custom_len(left) + progress.custom_len(right)

        status: str | bool | None = data['variables'].get(self.name)

        if width and status is not None:
            if status is True:
                marker = self.success_marker
                fg_color = self.success_fg_color
                bg_color = self.success_bg_color
            elif status is False:  # pragma: no branch
                marker = self.failure_marker
                fg_color = self.failure_fg_color
                bg_color = self.failure_bg_color
            else:  # pragma: no cover
                marker = status
                fg_color = bg_color = None

            marker = converters.to_unicode(marker)
            if fg_color:  # pragma: no branch
                marker = fg_color.fg(marker)
            if bg_color:  # pragma: no cover
                marker = bg_color.bg(marker)

            self.job_markers.append(marker)
            marker = ''.join(self.job_markers)
            width -= progress.custom_len(marker)

            fill = converters.to_unicode(self.fill(progress, data, width))
            fill = self._apply_colors(fill * width, data)

            if self.fill_left:  # pragma: no branch
                marker += fill
            else:  # pragma: no cover
                marker = fill + marker
        else:
            marker = ''

        return left + marker + right
