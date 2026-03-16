from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from __future__ import with_statement

import sys
import math
import os
import time
import timeit
import logging
import warnings
from datetime import datetime
from copy import deepcopy
try:  # pragma: no cover
    from collections import abc
except ImportError:  # pragma: no cover
    import collections as abc

from python_utils import converters

import six

from . import widgets
from . import widgets as widgets_module  # Avoid name collision
from . import base
from . import utils


logger = logging.getLogger(__name__)


class ProgressBarMixinBase(object):

    def __init__(self, **kwargs):
        self._finished = False

    def start(self, **kwargs):
        pass

    def update(self, value=None):
        pass

    def finish(self):  # pragma: no cover
        self._finished = True

    def __del__(self):
        if not self._finished:  # pragma: no cover
            try:
                self.finish()
            except Exception:
                pass


class ProgressBarBase(abc.Iterable, ProgressBarMixinBase):
    pass


class DefaultFdMixin(ProgressBarMixinBase):

    def __init__(self, fd=sys.stderr, is_terminal=None, line_breaks=None,
                 enable_colors=None, **kwargs):
        if fd is sys.stdout:
            fd = utils.streams.original_stdout

        elif fd is sys.stderr:
            fd = utils.streams.original_stderr

        self.fd = fd
        self.is_ansi_terminal = utils.is_ansi_terminal(fd)

        # Check if this is an interactive terminal
        self.is_terminal = utils.is_terminal(
            fd, is_terminal or self.is_ansi_terminal)

        # Check if it should overwrite the current line (suitable for
        # iteractive terminals) or write line breaks (suitable for log files)
        if line_breaks is None:
            line_breaks = utils.env_flag('PROGRESSBAR_LINE_BREAKS', not
                                         self.is_terminal)
        self.line_breaks = line_breaks

        # Check if ANSI escape characters are enabled (suitable for iteractive
        # terminals), or should be stripped off (suitable for log files)
        if enable_colors is None:
            enable_colors = utils.env_flag('PROGRESSBAR_ENABLE_COLORS',
                                           self.is_ansi_terminal)

        self.enable_colors = enable_colors

        ProgressBarMixinBase.__init__(self, **kwargs)

    def update(self, *args, **kwargs):
        ProgressBarMixinBase.update(self, *args, **kwargs)

        line = converters.to_unicode(self._format_line())
        if not self.enable_colors:
            line = utils.no_color(line)

        if self.line_breaks:
            line = line.rstrip() + '\n'
        else:
            line = '\r' + line

        try:  # pragma: no cover
            self.fd.write(line)
        except UnicodeEncodeError:  # pragma: no cover
            self.fd.write(line.encode('ascii', 'replace'))

    def finish(self, *args, **kwargs):  # pragma: no cover
        if self._finished:
            return

        end = kwargs.pop('end', '\n')
        ProgressBarMixinBase.finish(self, *args, **kwargs)

        if end and not self.line_breaks:
            self.fd.write(end)

        self.fd.flush()


class ResizableMixin(ProgressBarMixinBase):

    def __init__(self, term_width=None, **kwargs):
        ProgressBarMixinBase.__init__(self, **kwargs)

        self.signal_set = False
        if term_width:
            self.term_width = term_width
        else:  # pragma: no cover
            try:
                self._handle_resize()
                import signal
                self._prev_handle = signal.getsignal(signal.SIGWINCH)
                signal.signal(signal.SIGWINCH, self._handle_resize)
                self.signal_set = True
            except Exception:
                pass

    def _handle_resize(self, signum=None, frame=None):
        'Tries to catch resize signals sent from the terminal.'

        w, h = utils.get_terminal_size()
        self.term_width = w

    def finish(self):  # pragma: no cover
        ProgressBarMixinBase.finish(self)
        if self.signal_set:
            try:
                import signal
                signal.signal(signal.SIGWINCH, self._prev_handle)
            except Exception:  # pragma no cover
                pass


class StdRedirectMixin(DefaultFdMixin):

    def __init__(self, redirect_stderr=False, redirect_stdout=False, **kwargs):
        DefaultFdMixin.__init__(self, **kwargs)
        self.redirect_stderr = redirect_stderr
        self.redirect_stdout = redirect_stdout
        self._stdout = self.stdout = sys.stdout
        self._stderr = self.stderr = sys.stderr

    def start(self, *args, **kwargs):
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

    def update(self, value=None):
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


class ProgressBar(StdRedirectMixin, ResizableMixin, ProgressBarBase):

    '''The ProgressBar class which updates and prints the bar.

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

    A common way of using it is like:

    >>> progress = ProgressBar().start()
    >>> for i in range(100):
    ...     progress.update(i + 1)
    ...     # do something
    ...
    >>> progress.finish()

    You can also use a ProgressBar as an iterator:

    >>> progress = ProgressBar()
    >>> some_iterable = range(100)
    >>> for i in progress(some_iterable):
    ...     # do something
    ...     pass
    ...

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

    Useful methods and attributes include (Public API):
     - value: current progress (min_value <= value <= max_value)
     - max_value: maximum (and final) value
     - end_time: not None if the bar has finished (reached 100%)
     - start_time: the time when start() method of ProgressBar was called
     - seconds_elapsed: seconds elapsed since start_time and last call to
                        update
    '''

    _DEFAULT_MAXVAL = base.UnknownLength
    # update every 50 milliseconds (up to a 20 times per second)
    _MINIMUM_UPDATE_INTERVAL = 0.050

    def __init__(self, min_value=0, max_value=None, widgets=None,
                 left_justify=True, initial_value=0, poll_interval=None,
                 widget_kwargs=None, custom_len=utils.len_color,
                 max_error=True, prefix=None, suffix=None, variables=None,
                 min_poll_interval=None, **kwargs):
        '''
        Initializes a progress bar with sane defaults
        '''
        StdRedirectMixin.__init__(self, **kwargs)
        ResizableMixin.__init__(self, **kwargs)
        ProgressBarBase.__init__(self, **kwargs)
        if not max_value and kwargs.get('maxval') is not None:
            warnings.warn('The usage of `maxval` is deprecated, please use '
                          '`max_value` instead', DeprecationWarning)
            max_value = kwargs.get('maxval')

        if not poll_interval and kwargs.get('poll'):
            warnings.warn('The usage of `poll` is deprecated, please use '
                          '`poll_interval` instead', DeprecationWarning)
            poll_interval = kwargs.get('poll')

        if max_value:
            if min_value > max_value:
                raise ValueError('Max value needs to be bigger than the min '
                                 'value')
        self.min_value = min_value
        self.max_value = max_value
        self.max_error = max_error

        # Only copy the widget if it's safe to copy. Most widgets are so we
        # assume this to be true
        if widgets is None:
            self.widgets = widgets
        else:
            self.widgets = []
            for widget in widgets:
                if getattr(widget, 'copy', True):
                    widget = deepcopy(widget)
                self.widgets.append(widget)

        self.widgets = widgets
        self.prefix = prefix
        self.suffix = suffix
        self.widget_kwargs = widget_kwargs or {}
        self.left_justify = left_justify
        self.value = initial_value
        self._iterable = None
        self.custom_len = custom_len
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
        min_poll_interval = utils.deltas_to_seconds(min_poll_interval,
                                                    default=None)
        self._MINIMUM_UPDATE_INTERVAL = utils.deltas_to_seconds(
            self._MINIMUM_UPDATE_INTERVAL)

        # Note that the _MINIMUM_UPDATE_INTERVAL sets the minimum in case of
        # low values.
        self.poll_interval = poll_interval
        self.min_poll_interval = max(
            min_poll_interval or self._MINIMUM_UPDATE_INTERVAL,
            self._MINIMUM_UPDATE_INTERVAL,
            float(os.environ.get('PROGRESSBAR_MINIMUM_UPDATE_INTERVAL', 0)),
        )

        # A dictionary of names that can be used by Variable and FormatWidget
        self.variables = utils.AttributeDict(variables or {})
        for widget in (self.widgets or []):
            if isinstance(widget, widgets_module.VariableMixin):
                if widget.name not in self.variables:
                    self.variables[widget.name] = None

    @property
    def dynamic_messages(self):  # pragma: no cover
        return self.variables

    @dynamic_messages.setter
    def dynamic_messages(self, value):  # pragma: no cover
        self.variables = value

    def init(self):
        '''
        (re)initialize values to original state so the progressbar can be
        used (again)
        '''
        self.previous_value = None
        self.last_update_time = None
        self.start_time = None
        self.updates = 0
        self.end_time = None
        self.extra = dict()
        self._last_update_timer = timeit.default_timer()

    @property
    def percentage(self):
        '''Return current percentage, returns None if no max_value is given

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
        '''
        if self.max_value is None or self.max_value is base.UnknownLength:
            return None
        elif self.max_value:
            todo = self.value - self.min_value
            total = self.max_value - self.min_value
            percentage = 100.0 * todo / total
        else:
            percentage = 100.0

        return percentage

    def get_last_update_time(self):
        if self._last_update_time:
            return datetime.fromtimestamp(self._last_update_time)

    def set_last_update_time(self, value):
        if value:
            self._last_update_time = time.mktime(value.timetuple())
        else:
            self._last_update_time = None

    last_update_time = property(get_last_update_time, set_last_update_time)

    def data(self):
        '''

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
                  :py:class:`~progressbar.widgets.Variable`'s

        '''
        self._last_update_time = time.time()
        self._last_update_timer = timeit.default_timer()
        elapsed = self.last_update_time - self.start_time
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
            seconds_elapsed=(elapsed.seconds % 60) +
            (elapsed.microseconds / 1000000.),
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
                ' ', widgets.SimpleProgress(
                    format='(%s)' % widgets.SimpleProgress.DEFAULT_FORMAT,
                    **self.widget_kwargs),
                ' ', widgets.Bar(**self.widget_kwargs),
                ' ', widgets.Timer(**self.widget_kwargs),
                ' ', widgets.AdaptiveETA(**self.widget_kwargs),
            ]
        else:
            return [
                widgets.AnimatedMarker(**self.widget_kwargs),
                ' ', widgets.BouncingBar(**self.widget_kwargs),
                ' ', widgets.Counter(**self.widget_kwargs),
                ' ', widgets.Timer(**self.widget_kwargs),
            ]

    def __call__(self, iterable, max_value=None):
        'Use a ProgressBar to iterate through an iterable'
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
            value = next(self._iterable)
            if self.start_time is None:
                self.start()
            else:
                self.update(self.value + 1)
            return value
        except StopIteration:
            self.finish()
            raise
        except GeneratorExit:  # pragma: no cover
            self.finish(dirty=True)
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        self.finish(dirty=bool(exc_type))

    def __enter__(self):
        return self

    # Create an alias so that Python 2.x won't complain about not being
    # an iterator.
    next = __next__

    def __iadd__(self, value):
        'Updates the ProgressBar by adding a new value.'
        self.update(self.value + value)
        return self

    def _format_widgets(self):
        result = []
        expanding = []
        width = self.term_width
        data = self.data()

        for index, widget in enumerate(self.widgets):
            if isinstance(widget, widgets.WidgetBase) \
                    and not widget.check_size(self):
                continue
            elif isinstance(widget, widgets.AutoWidthWidgetBase):
                result.append(widget)
                expanding.insert(0, index)
            elif isinstance(widget, six.string_types):
                result.append(widget)
                width -= self.custom_len(widget)
            else:
                widget_output = converters.to_unicode(widget(self, data))
                result.append(widget_output)
                width -= self.custom_len(widget_output)

        count = len(expanding)
        while expanding:
            portion = max(int(math.ceil(width * 1. / count)), 0)
            index = expanding.pop()
            widget = result[index]
            count -= 1

            widget_output = widget(self, data, portion)
            width -= self.custom_len(widget_output)
            result[index] = widget_output

        return result

    @classmethod
    def _to_unicode(cls, args):
        for arg in args:
            yield converters.to_unicode(arg)

    def _format_line(self):
        'Joins the widgets and justifies the line'

        widgets = ''.join(self._to_unicode(self._format_widgets()))

        if self.left_justify:
            return widgets.ljust(self.term_width)
        else:
            return widgets.rjust(self.term_width)

    def _needs_update(self):
        'Returns whether the ProgressBar should redraw the line.'
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
        try:
            divisor = self.max_value / self.term_width  # float division
            if self.value // divisor != self.previous_value // divisor:
                return True
        except Exception:
            # ignore any division errors
            pass

        # No need to redraw yet
        return False

    def update(self, value=None, force=False, **kwargs):
        'Updates the ProgressBar to a new value.'
        if self.start_time is None:
            self.start()
            return self.update(value, force=force, **kwargs)

        if value is not None and value is not base.UnknownLength:
            if self.max_value is base.UnknownLength:
                # Can't compare against unknown lengths so just update
                pass
            elif self.min_value <= value <= self.max_value:  # pragma: no cover
                # Correct value, let's accept
                pass
            elif self.max_error:
                raise ValueError(
                    'Value %s is out of range, should be between %s and %s'
                    % (value, self.min_value, self.max_value))
            else:
                self.max_value = value

            self.previous_value = self.value
            self.value = value

        # Save the updated values for dynamic messages
        variables_changed = False
        for key in kwargs:
            if key not in self.variables:
                raise TypeError(
                    'update() got an unexpected keyword ' +
                    'argument {0!r}'.format(key))
            elif self.variables[key] != kwargs[key]:
                self.variables[key] = kwargs[key]
                variables_changed = True

        if self._needs_update() or variables_changed or force:
            self.updates += 1
            ResizableMixin.update(self, value=value)
            ProgressBarBase.update(self, value=value)
            StdRedirectMixin.update(self, value=value)

            # Only flush if something was actually written
            self.fd.flush()

    def start(self, max_value=None, init=True):
        '''Starts measuring time, and prints the bar at 0%.

        It returns self so you can use it like this:

        Args:
            max_value (int): The maximum value of the progressbar
            reinit (bool): Initialize the progressbar, this is useful if you
                wish to reuse the same progressbar but can be disabled if
                data needs to be passed along to the next run

        >>> pbar = ProgressBar().start()
        >>> for i in range(100):
        ...    # do something
        ...    pbar.update(i+1)
        ...
        >>> pbar.finish()
        '''
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
        if self.widgets is None:
            self.widgets = self.default_widgets()

        if self.prefix:
            self.widgets.insert(0, widgets.FormatLabel(
                self.prefix, new_style=True))
            # Unset the prefix variable after applying so an extra start()
            # won't keep copying it
            self.prefix = None

        if self.suffix:
            self.widgets.append(widgets.FormatLabel(
                self.suffix, new_style=True))
            # Unset the suffix variable after applying so an extra start()
            # won't keep copying it
            self.suffix = None

        for widget in self.widgets:
            interval = getattr(widget, 'INTERVAL', None)
            if interval is not None:
                interval = utils.deltas_to_seconds(interval)

                self.poll_interval = min(
                    self.poll_interval or interval,
                    interval,
                )

        self.num_intervals = max(100, self.term_width)
        # The `next_update` is kept for compatibility with external libs:
        # https://github.com/WoLpH/python-progressbar/issues/207
        self.next_update = 0

        if self.max_value is not base.UnknownLength and self.max_value < 0:
            raise ValueError('max_value out of range, got %r' % self.max_value)

        now = datetime.now()
        self.start_time = self.initial_start_time or now
        self.last_update_time = now
        self._last_update_timer = timeit.default_timer()
        self.update(self.min_value, force=True)

        return self

    def finish(self, end='\n', dirty=False):
        '''
        Puts the ProgressBar bar in the finished state.

        Also flushes and disables output buffering if this was the last
        progressbar running.

        Args:
            end (str): The string to end the progressbar with, defaults to a
                newline
            dirty (bool): When True the progressbar kept the current state and
                won't be set to 100 percent
        '''

        if not dirty:
            self.end_time = datetime.now()
            self.update(self.max_value, force=True)

        StdRedirectMixin.finish(self, end=end)
        ResizableMixin.finish(self)
        ProgressBarBase.finish(self)


class DataTransferBar(ProgressBar):
    '''A progress bar with sensible defaults for downloads etc.

    This assumes that the values its given are numbers of bytes.
    '''
    def default_widgets(self):
        if self.max_value:
            return [
                widgets.Percentage(),
                ' of ', widgets.DataSize('max_value'),
                ' ', widgets.Bar(),
                ' ', widgets.Timer(),
                ' ', widgets.AdaptiveETA(),
            ]
        else:
            return [
                widgets.AnimatedMarker(),
                ' ', widgets.DataSize(),
                ' ', widgets.Timer(),
            ]


class NullBar(ProgressBar):

    '''
    Progress bar that does absolutely nothing. Useful for single verbosity
    flags
    '''

    def start(self, *args, **kwargs):
        return self

    def update(self, *args, **kwargs):
        return self

    def finish(self, *args, **kwargs):
        return self
