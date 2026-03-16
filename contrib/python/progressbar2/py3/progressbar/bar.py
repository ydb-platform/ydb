from __future__ import annotations

import abc
import contextlib
import itertools
import logging
import math
import os
import sys
import time
import timeit
import typing
import warnings
from copy import deepcopy
from datetime import datetime
from types import FrameType

from python_utils import converters, types

import progressbar.env
import progressbar.terminal
import progressbar.terminal.stream

from . import (
    base,
    utils,
    widgets,
    widgets as widgets_module,  # Avoid name collision
)
from .terminal import os_specific

logger = logging.getLogger(__name__)

# float also accepts integers and longs but we don't want an explicit union
# due to type checking complexity
NumberT = float
ValueT = typing.Union[NumberT, typing.Type[base.UnknownLength], None]

T = types.TypeVar('T')


class ProgressBarMixinBase(abc.ABC):
    _started = False
    _finished = False
    _last_update_time: types.Optional[float] = None

    #: The terminal width. This should be automatically detected but will
    #: fall back to 80 if auto detection is not possible.
    term_width: int = 80
    #: The widgets to render, defaults to the result of `default_widget()`
    widgets: types.MutableSequence[widgets_module.WidgetBase | str]
    #: When going beyond the max_value, raise an error if True or silently
    #: ignore otherwise
    max_error: bool
    #: Prefix the progressbar with the given string
    prefix: types.Optional[str]
    #: Suffix the progressbar with the given string
    suffix: types.Optional[str]
    #: Justify to the left if `True` or the right if `False`
    left_justify: bool
    #: The default keyword arguments for the `default_widgets` if no widgets
    #: are configured
    widget_kwargs: types.Dict[str, types.Any]
    #: Custom length function for multibyte characters such as CJK
    # mypy and pyright can't agree on what the correct one is... so we'll
    # need to use a helper function :(
    # custom_len: types.Callable[['ProgressBarMixinBase', str], int]
    custom_len: types.Callable[[str], int]
    #: The time the progress bar was started
    initial_start_time: types.Optional[datetime]
    #: The interval to poll for updates in seconds if there are updates
    poll_interval: types.Optional[float]
    #: The minimum interval to poll for updates in seconds even if there are
    #: no updates
    min_poll_interval: float

    #: Deprecated: The number of intervals that can fit on the screen with a
    #: minimum of 100
    num_intervals: int = 0
    #: Deprecated: The `next_update` is kept for compatibility with external
    #: libs: https://github.com/WoLpH/python-progressbar/issues/207
    next_update: int = 0

    #: Current progress (min_value <= value <= max_value)
    value: NumberT
    #: Previous progress value
    previous_value: types.Optional[NumberT]
    #: The minimum/start value for the progress bar
    min_value: NumberT
    #: Maximum (and final) value. Beyond this value an error will be raised
    #: unless the `max_error` parameter is `False`.
    max_value: ValueT
    #: The time the progressbar reached `max_value` or when `finish()` was
    #: called.
    end_time: types.Optional[datetime]
    #: The time `start()` was called or iteration started.
    start_time: types.Optional[datetime]
    #: Seconds between `start_time` and last call to `update()`
    seconds_elapsed: float

    #: Extra data for widgets with persistent state. This is used by
    #: sampling widgets for example. Since widgets can be shared between
    #: multiple progressbars we need to store the state with the progressbar.
    extra: types.Dict[str, types.Any]

    def get_last_update_time(self) -> types.Optional[datetime]:
        if self._last_update_time:
            return datetime.fromtimestamp(self._last_update_time)
        else:
            return None

    def set_last_update_time(self, value: types.Optional[datetime]):
        if value:
            self._last_update_time = time.mktime(value.timetuple())
        else:
            self._last_update_time = None

    last_update_time = property(get_last_update_time, set_last_update_time)

    def __init__(self, **kwargs: typing.Any):  # noqa: B027
        pass

    def start(self, **kwargs: typing.Any):
        self._started = True

    def update(self, value: ValueT = None):  # noqa: B027
        pass

    def finish(self):  # pragma: no cover
        self._finished = True

    def __del__(self):
        if not self._finished and self._started:  # pragma: no cover
            # We're not using contextlib.suppress here because during teardown
            # contextlib is not available anymore.
            try:  # noqa: SIM105
                self.finish()
            except AttributeError:
                pass

    def __getstate__(self):
        return self.__dict__

    def data(self) -> types.Dict[str, types.Any]:  # pragma: no cover
        raise NotImplementedError()

    def started(self) -> bool:
        return self._finished or self._started

    def finished(self) -> bool:
        return self._finished


class ProgressBarBase(types.Iterable[NumberT], ProgressBarMixinBase):
    _index_counter = itertools.count()
    index: int = -1
    label: str = ''

    def __init__(self, **kwargs: typing.Any):
        self.index = next(self._index_counter)
        super().__init__(**kwargs)

    def __repr__(self):
        label = f': {self.label}' if self.label else ''
        return f'<{self.__class__.__name__}#{self.index}{label}>'


class DefaultFdMixin(ProgressBarMixinBase):
    # The file descriptor to write to. Defaults to `sys.stderr`
    fd: base.TextIO = sys.stderr
    #: Set the terminal to be ANSI compatible. If a terminal is ANSI
    #: compatible we will automatically enable `colors` and disable
    #: `line_breaks`.
    is_ansi_terminal: bool | None = False
    #: Whether the file descriptor is a terminal or not. This is used to
    #: determine whether to use ANSI escape codes or not.
    is_terminal: bool | None
    #: Whether to print line breaks. This is useful for logging the
    #: progressbar. When disabled the current line is overwritten.
    line_breaks: bool | None = True
    #: Specify the type and number of colors to support. Defaults to auto
    #: detection based on the file descriptor type (i.e. interactive terminal)
    #: environment variables such as `COLORTERM` and `TERM`. Color output can
    #: be forced in non-interactive terminals using the
    #: `PROGRESSBAR_ENABLE_COLORS` environment variable which can also be used
    #: to force a specific number of colors by specifying `24bit`, `256` or
    #: `16`.
    #: For true (24 bit/16M) color support you can use `COLORTERM=truecolor`.
    #: For 256 color support you can use `TERM=xterm-256color`.
    #: For 16 colorsupport you can use `TERM=xterm`.
    enable_colors: progressbar.env.ColorSupport = progressbar.env.COLOR_SUPPORT

    def __init__(
        self,
        fd: base.TextIO = sys.stderr,
        is_terminal: bool | None = None,
        line_breaks: bool | None = None,
        enable_colors: progressbar.env.ColorSupport | None = None,
        line_offset: int = 0,
        **kwargs: typing.Any,
    ):
        if fd is sys.stdout:
            fd = utils.streams.original_stdout
        elif fd is sys.stderr:
            fd = utils.streams.original_stderr

        fd = self._apply_line_offset(fd, line_offset)
        self.fd = fd
        self.is_ansi_terminal = progressbar.env.is_ansi_terminal(fd)
        self.is_terminal = progressbar.env.is_terminal(fd, is_terminal)
        self.line_breaks = self._determine_line_breaks(line_breaks)
        self.enable_colors = self._determine_enable_colors(enable_colors)

        super().__init__(**kwargs)

    def _apply_line_offset(
        self,
        fd: base.TextIO,
        line_offset: int,
    ) -> base.TextIO:
        if line_offset:
            return progressbar.terminal.stream.LineOffsetStreamWrapper(
                line_offset,
                fd,
            )
        else:
            return fd

    def _determine_line_breaks(self, line_breaks: bool | None) -> bool | None:
        if line_breaks is None:
            return progressbar.env.env_flag(
                'PROGRESSBAR_LINE_BREAKS',
                not self.is_terminal,
            )
        else:
            return line_breaks

    def _determine_enable_colors(
        self,
        enable_colors: progressbar.env.ColorSupport | None,
    ) -> progressbar.env.ColorSupport:
        """
        Determines the color support for the progress bar.

        This method checks the `enable_colors` parameter and the environment
        variables `PROGRESSBAR_ENABLE_COLORS` and `FORCE_COLOR` to determine
        the color support.

        If `enable_colors` is:
         - `None`, it checks the environment variables and the terminal
            compatibility to ANSI.
         - `True`, it sets the color support to XTERM_256.
         - `False`, it sets the color support to NONE.
         - For different values that are not instances of
           `progressbar.env.ColorSupport`, it raises a ValueError.

        Args:
             enable_colors (progressbar.env.ColorSupport | None): The color
             support setting from the user. It can be None, True, False,
             or an instance of `progressbar.env.ColorSupport`.

        Returns:
            progressbar.env.ColorSupport: The determined color support.

        Raises:
            ValueError: If `enable_colors` is not None, True, False, or an
            instance of `progressbar.env.ColorSupport`.
        """
        color_support: progressbar.env.ColorSupport
        if enable_colors is None:
            colors = (
                progressbar.env.env_flag('PROGRESSBAR_ENABLE_COLORS'),
                progressbar.env.env_flag('FORCE_COLOR'),
                self.is_ansi_terminal,
            )

            for color_enabled in colors:
                if color_enabled is not None:
                    if color_enabled:
                        color_support = progressbar.env.COLOR_SUPPORT
                    else:
                        color_support = progressbar.env.ColorSupport.NONE
                    break
            else:
                color_support = progressbar.env.ColorSupport.NONE

        elif enable_colors is True:
            color_support = progressbar.env.ColorSupport.XTERM_256
        elif enable_colors is False:
            color_support = progressbar.env.ColorSupport.NONE
        elif isinstance(enable_colors, progressbar.env.ColorSupport):
            color_support = enable_colors
        else:
            raise ValueError(f'Invalid color support value: {enable_colors}')

        return color_support

    def print(self, *args: types.Any, **kwargs: types.Any) -> None:
        print(*args, file=self.fd, **kwargs)

    def start(self, **kwargs: typing.Any):
        os_specific.set_console_mode()
        super().start()

    def update(self, *args: types.Any, **kwargs: types.Any) -> None:
        ProgressBarMixinBase.update(self, *args, **kwargs)

        line: str = converters.to_unicode(self._format_line())
        if not self.enable_colors:
            line = utils.no_color(line)

        line = line.rstrip() + '\n' if self.line_breaks else '\r' + line

        try:  # pragma: no cover
            self.fd.write(line)
        except UnicodeEncodeError:  # pragma: no cover
            self.fd.write(types.cast(str, line.encode('ascii', 'replace')))

    def finish(
        self,
        *args: types.Any,
        **kwargs: types.Any,
    ) -> None:  # pragma: no cover
        os_specific.reset_console_mode()

        if self._finished:
            return

        end = kwargs.pop('end', '\n')
        ProgressBarMixinBase.finish(self, *args, **kwargs)

        if end and not self.line_breaks:
            self.fd.write(end)

        self.fd.flush()

    def _format_line(self):
        "Joins the widgets and justifies the line."
        widgets = ''.join(self._to_unicode(self._format_widgets()))

        if self.left_justify:
            return widgets.ljust(self.term_width)
        else:
            return widgets.rjust(self.term_width)

    def _format_widgets(self):
        result = []
        expanding = []
        width = self.term_width
        data = self.data()

        for index, widget in enumerate(self.widgets):
            if isinstance(
                widget,
                widgets.WidgetBase,
            ) and not widget.check_size(self):
                continue
            elif isinstance(widget, widgets.AutoWidthWidgetBase):
                result.append(widget)
                expanding.insert(0, index)
            elif isinstance(widget, str):
                result.append(widget)
                width -= self.custom_len(widget)  # type: ignore
            else:
                widget_output = converters.to_unicode(widget(self, data))
                result.append(widget_output)
                width -= self.custom_len(widget_output)  # type: ignore

        count = len(expanding)
        while expanding:
            portion = max(int(math.ceil(width * 1.0 / count)), 0)
            index = expanding.pop()
            widget = result[index]
            count -= 1

            widget_output = widget(self, data, portion)
            width -= self.custom_len(widget_output)  # type: ignore
            result[index] = widget_output

        return result

    @classmethod
    def _to_unicode(cls, args: typing.Any):
        for arg in args:
            yield converters.to_unicode(arg)


class ResizableMixin(ProgressBarMixinBase):
    def __init__(self, term_width: int | None = None, **kwargs: typing.Any):
        ProgressBarMixinBase.__init__(self, **kwargs)

        self.signal_set = False
        if term_width:
            self.term_width = term_width
        else:  # pragma: no cover
            with contextlib.suppress(Exception):
                self._handle_resize()
                import signal

                self._prev_handle = signal.getsignal(
                    signal.SIGWINCH  # type: ignore
                )
                signal.signal(
                    signal.SIGWINCH,
                    self._handle_resize,  # type: ignore
                )
                self.signal_set = True

    def _handle_resize(
        self, signum: int | None = None, frame: None | FrameType = None
    ):
        "Tries to catch resize signals sent from the terminal."
        w, h = utils.get_terminal_size()
        self.term_width = w

    def finish(self):  # pragma: no cover
        ProgressBarMixinBase.finish(self)
        if self.signal_set:
            with contextlib.suppress(Exception):
                import signal

                signal.signal(
                    signal.SIGWINCH,
                    self._prev_handle,  # type: ignore
                )


class StdRedirectMixin(DefaultFdMixin):
    redirect_stderr: bool = False
    redirect_stdout: bool = False
    stdout: utils.WrappingIO | base.IO[typing.Any]
    stderr: utils.WrappingIO | base.IO[typing.Any]
    _stdout: base.IO[typing.Any]
    _stderr: base.IO[typing.Any]

    def __init__(
        self,
        redirect_stderr: bool = False,
        redirect_stdout: bool = False,
        **kwargs,
    ):
        DefaultFdMixin.__init__(self, **kwargs)
        self.redirect_stderr = redirect_stderr
        self.redirect_stdout = redirect_stdout
        self._stdout = self.stdout = sys.stdout
        self._stderr = self.stderr = sys.stderr

    def start(self, *args: typing.Any, **kwargs: typing.Any):
        if self.redirect_stdout:
            utils.streams.wrap_stdout()

        if self.redirect_stderr:
            utils.streams.wrap_stderr()

        self._stdout = utils.streams.original_stdout
        self._stderr = utils.streams.original_stderr

        self.stdout = utils.streams.stdout
        self.stderr = utils.streams.stderr

        utils.streams.start_capturing(self)
        DefaultFdMixin.start(self, *args, **kwargs)

    def update(self, value: types.Optional[NumberT] = None):
        if not self.line_breaks and utils.streams.needs_clear():
            self.fd.write('\r' + ' ' * self.term_width + '\r')

        utils.streams.flush()
        DefaultFdMixin.update(self, value=value)

    def finish(self, end='\n'):
        DefaultFdMixin.finish(self, end=end)
        utils.streams.stop_capturing(self)
        if self.redirect_stdout:
            utils.streams.unwrap_stdout()

        if self.redirect_stderr:
            utils.streams.unwrap_stderr()


class ProgressBar(
    StdRedirectMixin,
    ResizableMixin,
    ProgressBarBase,
):
    """The ProgressBar class which updates and prints the bar.

    Args:
        min_value (int): The minimum/start value for the progress bar
        max_value (int): The maximum/end value for the progress bar.
                            Defaults to `_DEFAULT_MAXVAL`
        widgets (list): The widgets to render, defaults to the result of
                        `default_widget()`
        left_justify (bool): Justify to the left if `True` or the right if
                                `False`
        initial_value (int): The value to start with
        poll_interval (float): The update interval in seconds.
            Note that if your widgets include timers or animations, the actual
            interval may be smaller (faster updates).  Also note that updates
            never happens faster than `min_poll_interval` which can be used for
            reduced output in logs
        min_poll_interval (float): The minimum update interval in seconds.
            The bar will _not_ be updated faster than this, despite changes in
            the progress, unless `force=True`.  This is limited to be at least
            `_MINIMUM_UPDATE_INTERVAL`.  If available, it is also bound by the
            environment variable PROGRESSBAR_MINIMUM_UPDATE_INTERVAL
        widget_kwargs (dict): The default keyword arguments for widgets
        custom_len (function): Method to override how the line width is
            calculated. When using non-latin characters the width
            calculation might be off by default
        max_error (bool): When True the progressbar will raise an error if it
            goes beyond it's set max_value. Otherwise the max_value is simply
            raised when needed
            prefix (str): Prefix the progressbar with the given string
            suffix (str): Prefix the progressbar with the given string
        variables (dict): User-defined variables variables that can be used
            from a label using `format='{variables.my_var}'`.  These values can
            be updated using `bar.update(my_var='newValue')` This can also be
            used to set initial values for variables' widgets
        line_offset (int): The number of lines to offset the progressbar from
            your current line. This is useful if you have other output or
            multiple progressbars

    A common way of using it is like:

    >>> progress = ProgressBar().start()
    >>> for i in range(100):
    ...     progress.update(i + 1)
    ...     # do something
    >>> progress.finish()

    You can also use a ProgressBar as an iterator:

    >>> progress = ProgressBar()
    >>> some_iterable = range(100)
    >>> for i in progress(some_iterable):
    ...     # do something
    ...     pass

    Since the progress bar is incredibly customizable you can specify
    different widgets of any type in any order. You can even write your own
    widgets! However, since there are already a good number of widgets you
    should probably play around with them before moving on to create your own
    widgets.

    The term_width parameter represents the current terminal width. If the
    parameter is set to an integer then the progress bar will use that,
    otherwise it will attempt to determine the terminal width falling back to
    80 columns if the width cannot be determined.

    When implementing a widget's update method you are passed a reference to
    the current progress bar. As a result, you have access to the
    ProgressBar's methods and attributes. Although there is nothing preventing
    you from changing the ProgressBar you should treat it as read only.
    """

    _iterable: types.Optional[types.Iterator]

    _DEFAULT_MAXVAL: type[base.UnknownLength] = base.UnknownLength
    # update every 50 milliseconds (up to a 20 times per second)
    _MINIMUM_UPDATE_INTERVAL: float = 0.050
    _last_update_time: types.Optional[float] = None
    paused: bool = False

    def __init__(
        self,
        min_value: NumberT = 0,
        max_value: ValueT = None,
        widgets: types.Optional[
            types.Sequence[widgets_module.WidgetBase | str]
        ] = None,
        left_justify: bool = True,
        initial_value: NumberT = 0,
        poll_interval: types.Optional[float] = None,
        widget_kwargs: types.Optional[types.Dict[str, types.Any]] = None,
        custom_len: types.Callable[[str], int] = utils.len_color,
        max_error=True,
        prefix=None,
        suffix=None,
        variables=None,
        min_poll_interval=None,
        **kwargs,
    ):  # sourcery skip: low-code-quality
        """Initializes a progress bar with sane defaults."""
        StdRedirectMixin.__init__(self, **kwargs)
        ResizableMixin.__init__(self, **kwargs)
        ProgressBarBase.__init__(self, **kwargs)
        if not max_value and kwargs.get('maxval') is not None:
            warnings.warn(
                'The usage of `maxval` is deprecated, please use '
                '`max_value` instead',
                DeprecationWarning,
                stacklevel=1,
            )
            max_value = kwargs.get('maxval')

        if not poll_interval and kwargs.get('poll'):
            warnings.warn(
                'The usage of `poll` is deprecated, please use '
                '`poll_interval` instead',
                DeprecationWarning,
                stacklevel=1,
            )
            poll_interval = kwargs.get('poll')

        if max_value and min_value > types.cast(NumberT, max_value):
            raise ValueError(
                'Max value needs to be bigger than the min value',
            )
        self.min_value = min_value
        # Legacy issue, `max_value` can be `None` before execution. After
        # that it either has a value or is `UnknownLength`
        self.max_value = max_value  # type: ignore
        self.max_error = max_error

        # Only copy the widget if it's safe to copy. Most widgets are so we
        # assume this to be true
        self.widgets = []
        for widget in widgets or []:
            if getattr(widget, 'copy', True):
                widget = deepcopy(widget)
            self.widgets.append(widget)

        self.prefix = prefix
        self.suffix = suffix
        self.widget_kwargs = widget_kwargs or {}
        self.left_justify = left_justify
        self.value = initial_value
        self._iterable = None
        self.custom_len = custom_len  # type: ignore
        self.initial_start_time = kwargs.get('start_time')
        self.init()

        # Convert a given timedelta to a floating point number as internal
        # interval. We're not using timedelta's internally for two reasons:
        # 1. Backwards compatibility (most important one)
        # 2. Performance. Even though the amount of time it takes to compare a
        # timedelta with a float versus a float directly is negligible, this
        # comparison is run for _every_ update. With billions of updates
        # (downloading a 1GiB file for example) this adds up.
        poll_interval = utils.deltas_to_seconds(poll_interval, default=None)
        min_poll_interval = utils.deltas_to_seconds(
            min_poll_interval,
            default=None,
        )
        self._MINIMUM_UPDATE_INTERVAL = (
            utils.deltas_to_seconds(self._MINIMUM_UPDATE_INTERVAL)
            or self._MINIMUM_UPDATE_INTERVAL
        )

        # Note that the _MINIMUM_UPDATE_INTERVAL sets the minimum in case of
        # low values.
        self.poll_interval = poll_interval
        self.min_poll_interval = max(
            min_poll_interval or self._MINIMUM_UPDATE_INTERVAL,
            self._MINIMUM_UPDATE_INTERVAL,
            float(os.environ.get('PROGRESSBAR_MINIMUM_UPDATE_INTERVAL', 0)),
        )  # type: ignore

        # A dictionary of names that can be used by Variable and FormatWidget
        self.variables = utils.AttributeDict(variables or {})
        for widget in self.widgets:
            if (
                isinstance(widget, widgets_module.VariableMixin)
                and widget.name not in self.variables
            ):
                self.variables[widget.name] = None

    @property
    def dynamic_messages(self):  # pragma: no cover
        return self.variables

    @dynamic_messages.setter
    def dynamic_messages(self, value):  # pragma: no cover
        self.variables = value

    def init(self):
        """
        (re)initialize values to original state so the progressbar can be
        used (again).
        """
        self.previous_value = None
        self.last_update_time = None
        self.start_time = None
        self.updates = 0
        self.end_time = None
        self.extra = dict()
        self._last_update_timer = timeit.default_timer()

    @property
    def percentage(self) -> float | None:
        """Return current percentage, returns None if no max_value is given.

        >>> progress = ProgressBar()
        >>> progress.max_value = 10
        >>> progress.min_value = 0
        >>> progress.value = 0
        >>> progress.percentage
        0.0
        >>>
        >>> progress.value = 1
        >>> progress.percentage
        10.0
        >>> progress.value = 10
        >>> progress.percentage
        100.0
        >>> progress.min_value = -10
        >>> progress.percentage
        100.0
        >>> progress.value = 0
        >>> progress.percentage
        50.0
        >>> progress.value = 5
        >>> progress.percentage
        75.0
        >>> progress.value = -5
        >>> progress.percentage
        25.0
        >>> progress.max_value = None
        >>> progress.percentage
        """
        if self.max_value is None or self.max_value is base.UnknownLength:
            return None
        elif self.max_value:
            todo = self.value - self.min_value
            total = self.max_value - self.min_value  # type: ignore
            percentage = 100.0 * todo / total
        else:
            percentage = 100.0

        return percentage

    def data(self) -> types.Dict[str, types.Any]:
        """

        Returns:
            dict:
                - `max_value`: The maximum value (can be None with
                  iterators)
                - `start_time`: Start time of the widget
                - `last_update_time`: Last update time of the widget
                - `end_time`: End time of the widget
                - `value`: The current value
                - `previous_value`: The previous value
                - `updates`: The total update count
                - `total_seconds_elapsed`: The seconds since the bar started
                - `seconds_elapsed`: The seconds since the bar started modulo
                  60
                - `minutes_elapsed`: The minutes since the bar started modulo
                  60
                - `hours_elapsed`: The hours since the bar started modulo 24
                - `days_elapsed`: The hours since the bar started
                - `time_elapsed`: The raw elapsed `datetime.timedelta` object
                - `percentage`: Percentage as a float or `None` if no max_value
                  is available
                - `dynamic_messages`: Deprecated, use `variables` instead.
                - `variables`: Dictionary of user-defined variables for the
                  :py:class:`~progressbar.widgets.Variable`'s.

        """
        self._last_update_time = time.time()
        self._last_update_timer = timeit.default_timer()
        elapsed = self.last_update_time - self.start_time  # type: ignore
        # For Python 2.7 and higher we have _`timedelta.total_seconds`, but we
        # want to support older versions as well
        total_seconds_elapsed = utils.deltas_to_seconds(elapsed)
        return dict(
            # The maximum value (can be None with iterators)
            max_value=self.max_value,
            # Start time of the widget
            start_time=self.start_time,
            # Last update time of the widget
            last_update_time=self.last_update_time,
            # End time of the widget
            end_time=self.end_time,
            # The current value
            value=self.value,
            # The previous value
            previous_value=self.previous_value,
            # The total update count
            updates=self.updates,
            # The seconds since the bar started
            total_seconds_elapsed=total_seconds_elapsed,
            # The seconds since the bar started modulo 60
            seconds_elapsed=(elapsed.seconds % 60)
            + (elapsed.microseconds / 1000000.0),
            # The minutes since the bar started modulo 60
            minutes_elapsed=(elapsed.seconds / 60) % 60,
            # The hours since the bar started modulo 24
            hours_elapsed=(elapsed.seconds / (60 * 60)) % 24,
            # The hours since the bar started
            days_elapsed=(elapsed.seconds / (60 * 60 * 24)),
            # The raw elapsed `datetime.timedelta` object
            time_elapsed=elapsed,
            # Percentage as a float or `None` if no max_value is available
            percentage=self.percentage,
            # Dictionary of user-defined
            # :py:class:`progressbar.widgets.Variable`'s
            variables=self.variables,
            # Deprecated alias for `variables`
            dynamic_messages=self.variables,
        )

    def default_widgets(self):
        if self.max_value:
            return [
                widgets.Percentage(**self.widget_kwargs),
                ' ',
                widgets.SimpleProgress(
                    format=f'({widgets.SimpleProgress.DEFAULT_FORMAT})',
                    **self.widget_kwargs,
                ),
                ' ',
                widgets.Bar(**self.widget_kwargs),
                ' ',
                widgets.Timer(**self.widget_kwargs),
                ' ',
                widgets.SmoothingETA(**self.widget_kwargs),
            ]
        else:
            return [
                widgets.AnimatedMarker(**self.widget_kwargs),
                ' ',
                widgets.BouncingBar(**self.widget_kwargs),
                ' ',
                widgets.Counter(**self.widget_kwargs),
                ' ',
                widgets.Timer(**self.widget_kwargs),
            ]

    def __call__(self, iterable, max_value=None):
        "Use a ProgressBar to iterate through an iterable."
        if max_value is not None:
            self.max_value = max_value
        elif self.max_value is None:
            try:
                self.max_value = len(iterable)
            except TypeError:  # pragma: no cover
                self.max_value = base.UnknownLength

        self._iterable = iter(iterable)
        return self

    def __iter__(self):
        return self

    def __next__(self):
        try:
            if self._iterable is None:  # pragma: no cover
                value = self.value
            else:
                value = next(self._iterable)

            if self.start_time is None:
                self.start()
            else:
                self.update(self.value + 1)

        except StopIteration:
            self.finish()
            raise
        except GeneratorExit:  # pragma: no cover
            self.finish(dirty=True)
            raise
        else:
            return value

    def __exit__(self, exc_type, exc_value, traceback):
        self.finish(dirty=bool(exc_type))

    def __enter__(self):
        return self

    # Create an alias so that Python 2.x won't complain about not being
    # an iterator.
    next = __next__

    def __iadd__(self, value):
        "Updates the ProgressBar by adding a new value."
        return self.increment(value)

    def increment(
        self, value: NumberT = 1, *args: typing.Any, **kwargs: typing.Any
    ):
        self.update(self.value + value, *args, **kwargs)
        return self

    def _needs_update(self):
        "Returns whether the ProgressBar should redraw the line."
        if self.paused:
            return False
        delta = timeit.default_timer() - self._last_update_timer
        if delta < self.min_poll_interval:
            # Prevent updating too often
            return False
        elif self.poll_interval and delta > self.poll_interval:
            # Needs to redraw timers and animations
            return True

        # Update if value increment is not large enough to
        # add more bars to progressbar (according to current
        # terminal width)
        with contextlib.suppress(Exception):
            divisor: float = self.max_value / self.term_width  # type: ignore
            value_divisor = self.value // divisor  # type: ignore
            pvalue_divisor = self.previous_value // divisor  # type: ignore
            if value_divisor != pvalue_divisor:
                return True
        # No need to redraw yet
        return False

    def update(
        self, value: ValueT = None, force: bool = False, **kwargs: typing.Any
    ):
        "Updates the ProgressBar to a new value."
        if self.start_time is None:
            self.start()

        if (
            value is not None
            and value is not base.UnknownLength
            and isinstance(value, (int, float))
        ):
            if self.max_value is base.UnknownLength:
                # Can't compare against unknown lengths so just update
                pass
            elif self.min_value > value:  # type: ignore
                raise ValueError(
                    f'Value {value} is too small. Should be '
                    f'between {self.min_value} and {self.max_value}',
                )
            elif self.max_value < value:  # type: ignore
                if self.max_error:
                    raise ValueError(
                        f'Value {value} is too large. Should be between '
                        f'{self.min_value} and {self.max_value}',
                    )
                else:
                    value = typing.cast(NumberT, self.max_value)

            self.previous_value = self.value
            self.value = value

        # Save the updated values for dynamic messages
        variables_changed = self._update_variables(kwargs)

        if self._needs_update() or variables_changed or force:
            self._update_parents(value)

    def _update_variables(self, kwargs):
        variables_changed = False
        for key, value_ in kwargs.items():
            if key not in self.variables:
                raise TypeError(
                    'update() got an unexpected variable name as argument '
                    '{key!r}',
                )
            elif self.variables[key] != value_:
                self.variables[key] = kwargs[key]
                variables_changed = True
        return variables_changed

    def _update_parents(self, value: ValueT):
        self.updates += 1
        ResizableMixin.update(self, value=value)
        ProgressBarBase.update(self, value=value)
        StdRedirectMixin.update(self, value=value)  # type: ignore

        # Only flush if something was actually written
        self.fd.flush()

    def start(
        self,
        max_value: NumberT | None = None,
        init: bool = True,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> ProgressBar:
        """Starts measuring time, and prints the bar at 0%.

        It returns self so you can use it like this:

        Args:
            max_value (int): The maximum value of the progressbar
            init (bool): (Re)Initialize the progressbar, this is useful if you
                wish to reuse the same progressbar but can be disabled if
                data needs to be persisted between runs

        >>> pbar = ProgressBar().start()
        >>> for i in range(100):
        ...     # do something
        ...     pbar.update(i + 1)
        >>> pbar.finish()
        """
        if init:
            self.init()

        # Prevent multiple starts
        if self.start_time is not None:  # pragma: no cover
            return self

        if max_value is not None:
            self.max_value = max_value

        if self.max_value is None:
            self.max_value = self._DEFAULT_MAXVAL

        StdRedirectMixin.start(self, max_value=max_value)
        ResizableMixin.start(self, max_value=max_value)
        ProgressBarBase.start(self, max_value=max_value)

        # Constructing the default widgets is only done when we know max_value
        if not self.widgets:
            self.widgets = self.default_widgets()

        self._init_prefix()
        self._init_suffix()
        self._calculate_poll_interval()
        self._verify_max_value()

        now = datetime.now()
        self.start_time = self.initial_start_time or now
        self.last_update_time = now
        self._last_update_timer = timeit.default_timer()
        self.update(self.min_value, force=True)

        return self

    def _init_suffix(self):
        if self.suffix:
            self.widgets.append(
                widgets.FormatLabel(self.suffix, new_style=True),
            )
            # Unset the suffix variable after applying so an extra start()
            # won't keep copying it
            self.suffix = None

    def _init_prefix(self):
        if self.prefix:
            self.widgets.insert(
                0,
                widgets.FormatLabel(self.prefix, new_style=True),
            )
            # Unset the prefix variable after applying so an extra start()
            # won't keep copying it
            self.prefix = None

    def _verify_max_value(self):
        if (
            self.max_value is not base.UnknownLength
            and self.max_value is not None
            and self.max_value < 0  # type: ignore
        ):
            raise ValueError(f'max_value out of range, got {self.max_value!r}')

    def _calculate_poll_interval(self) -> None:
        self.num_intervals = max(100, self.term_width)
        for widget in self.widgets:
            interval: int | float | None = utils.deltas_to_seconds(
                getattr(widget, 'INTERVAL', None),
                default=None,
            )
            if interval is not None:
                self.poll_interval = min(
                    self.poll_interval or interval,
                    interval,
                )

    def finish(self, end: str = '\n', dirty: bool = False):
        """
        Puts the ProgressBar bar in the finished state.

        Also flushes and disables output buffering if this was the last
        progressbar running.

        Args:
            end (str): The string to end the progressbar with, defaults to a
                newline
            dirty (bool): When True the progressbar kept the current state and
                won't be set to 100 percent
        """
        if not dirty:
            self.end_time = datetime.now()
            self.update(self.max_value, force=True)

        StdRedirectMixin.finish(self, end=end)
        ResizableMixin.finish(self)
        ProgressBarBase.finish(self)

    @property
    def currval(self):
        """
        Legacy method to make progressbar-2 compatible with the original
        progressbar package.
        """
        warnings.warn(
            'The usage of `currval` is deprecated, please use '
            '`value` instead',
            DeprecationWarning,
            stacklevel=1,
        )
        return self.value


class DataTransferBar(ProgressBar):
    """A progress bar with sensible defaults for downloads etc.

    This assumes that the values its given are numbers of bytes.
    """

    def default_widgets(self):
        if self.max_value:
            return [
                widgets.Percentage(),
                ' of ',
                widgets.DataSize('max_value'),
                ' ',
                widgets.Bar(),
                ' ',
                widgets.Timer(),
                ' ',
                widgets.SmoothingETA(),
            ]
        else:
            return [
                widgets.AnimatedMarker(),
                ' ',
                widgets.DataSize(),
                ' ',
                widgets.Timer(),
            ]


class NullBar(ProgressBar):
    """
    Progress bar that does absolutely nothing. Useful for single verbosity
    flags.
    """

    def start(self, *args: typing.Any, **kwargs: typing.Any):
        return self

    def update(self, *args: typing.Any, **kwargs: typing.Any):
        return self

    def finish(self, *args: typing.Any, **kwargs: typing.Any):
        return self
