
'''
Decode "candump" CAN frames or the output of "cantools decode"
read from standard input and plot them using matplotlib.
You can select which signals to plot by specifying them on the command line.
Each signal is one argument and has the pattern "[bo.]sg[:fmt]"
where bo is the name of the message, sg is the name of the signal
and fmt is the format of the graph.
The wildcards * (any number of any character)
and ? (exactly one arbitrary character)
can be used inside of sg and bo.
If bo is omitted it defaults to *.

fmt is passed to matplotlib and can be used to specify
the line style, markers and color.
For example the following values can be combined:
Line style:
    '-'  solid line style,
    '--' dashed line style,
    '-.' dash-dot line style and
    ':'  dotted line style.
Markers:
    '.' point marker,
    ',' pixel marker,
    'o' circle marker,
    's' square marker,
    'D' diamond marker,
    'x' x marker
    and many more.
Colors:
    'b' blue,
    'g' green,
    'r' red,
    'c' cyan,
    'm' magenta,
    'y' yellow,
    'k' black and
    'w' white.
    'C0'...'C9' the colors defined by the current style
https://matplotlib.org/api/_as_gen/matplotlib.pyplot.plot.html

If the first character of fmt is a '|' stem is used instead of plot.

Signals can be separated by a '-' to show them in different subplots.

Signals can be separated by a ',' to make them refer to different vertical axes in the same subplot.
I recommend using this with the option --auto-color-ylabels.

All signals (independent of the subplot and vertical axis) share the same horizontal axis.
'''

import argparse
import binascii
import datetime
import re
import struct
import sys

from argparse_addons import Integer  # type: ignore

try:
    from matplotlib import pyplot as plt
except ImportError:
    plt = None  # type: ignore[assignment,unused-ignore]

from .. import database, errors
from ..database.namedsignalvalue import NamedSignalValue

PYPLOT_BASE_COLORS = "bgrcmykwC"


class MatplotlibNotInstalledError(errors.Error):

    def __init__(self):
        super().__init__("The matplotlib package not installed and is required "
                         "for producing plots.")


if plt is not None:
    #TODO: I am not allowing "%H:%M" as input (for --start or --stop) because it could be misinterpreted as "%M:%S". Should this output format be changed?
    # I don't think the ambiguity is a problem for the output because if it is not obvious from the context it can be easily clarified with --xlabel.
    # However, it seems very unintuitive if the same format which is used for output is not allowed for input.
    # If you do change it, remember to uncomment the tests in test_plot_unittests.py.
    plt.rcParams["date.autoformatter.hour"] = "%H:%M"
    plt.rcParams["date.autoformatter.minute"] = "%H:%M"
    plt.rcParams["date.autoformatter.microsecond"] = "%H:%M:%S.%f"


# Matches 'candump' output, i.e. "vcan0  1F0   [8]  00 00 00 00 00 00 1B C1".
RE_CANDUMP = re.compile(r'^\s*(?:\((?P<time>.*?)\))?\s*\S+\s+(?P<frameid>[0-9A-F]+)\s*\[\d+\]\s*(?P<data>[0-9A-F ]*)(?:\s*::.*)?$')
# Matches 'cantools decode' output, i.e. ")" or "   voltage: 0 V,".
RE_DECODE = re.compile(r'\w+\(|\s+\w+:\s+[0-9.+-]+(\s+.*)?,?|\)')
# Matches 'candump -l' (or -L) output, i.e. "(1594172461.968006) vcan0 1F0#0000000000001BC1"
RE_CANDUMP_LOG = re.compile(r'^\((?P<time>\d+\.\d+)\)\s+\S+\s+(?P<frameid>[\dA-F]+)#(?P<data>[\dA-F]*)(\s+[RT])?$')


def _mo_unpack(mo):
    '''extract the data from a re match object'''
    timestamp = mo.group('time')
    frame_id = mo.group('frameid')
    frame_id = '0' * (8 - len(frame_id)) + frame_id
    frame_id = binascii.unhexlify(frame_id)
    frame_id = struct.unpack('>I', frame_id)[0]
    data = mo.group('data')
    data = data.replace(' ', '')
    data = binascii.unhexlify(data)

    return timestamp, frame_id, data

class TimestampParser:

    '''
    Parses the values for the horizontal axis
    and generates the corresponding axis label.
    Preferably timestamps are used but if none
    are given it falls back to line numbers.
    '''

    # candump -ta, -tz and -td have the same timestamp syntax: a floating number in seconds.
    # In case of -td using timestamps does not seem useful and a user should use --line-numbers.
    # The following constant shall distinguish between -ta and -tz.
    # If the first timestamp is bigger than THRESHOLD_ABSOLUTE_SECONDS I am assuming -ta is used
    # and convert timestamps to datetime objects which will print a date.
    # Otherwise I'll assume -tz is used and format them using timedelta objects.
    # I am not using zero to compare against in case the beginning of the log file is stripped.
    THRESHOLD_ABSOLUTE_SECONDS = 60*60*24*7

    FORMAT_ABSOLUTE_TIMESTAMP = "%Y-%m-%d %H:%M:%S.%f"

    def __init__(self, args):
        self.use_timestamp = None
        self.relative = None
        self._parse_timestamp = None
        self.first_timestamp = None
        self.args = args

    def init_start_stop(self, x0):
        if self.use_timestamp and self.relative:
            parse = self.parse_user_input_relative_time
        elif self.use_timestamp:
            parse = self.parse_user_input_absolute_time
        else:
            def parse(s, _x0):
                return int(s)

        if self.args.start is not None:
            self.args.start = parse(self.args.start, x0)
            x0 = self.args.start
            self.first_timestamp = x0
        if self.args.stop is not None:
            self.args.stop = parse(self.args.stop, x0)

    def parse_user_input_relative_time(self, user_input, first_timestamp):
        try:
            return float(user_input)
        except ValueError:
            pass

        patterns_hour = ['%H:%M:', '%H:%M:%S', '%H:%M:%S.%f']
        patterns_minute = [':%M:%S', '%M:%S.', '%M:%S.%f']
        patterns_day = ['%d day', '%d days']

        day_time_sep = ', '
        for pattern_day in tuple(patterns_day):
            for pattern_time in ['%H:%M', *patterns_hour]:
                patterns_day.append(pattern_day+day_time_sep+pattern_time)

        for pattern in patterns_minute + patterns_hour + patterns_day:
            t = self.strptimedelta_in_seconds(user_input, pattern)
            if t is not None:
                return t

        raise ValueError(f"Failed to parse relative time {user_input!r}.\n\nPlease note that an input like 'xx:xx' is ambiguous. It could be either 'HH:MM' or 'MM:SS'. Please specify what you want by adding a leading or trailing colon: 'HH:MM:' or ':MM:SS' (or 'MM:SS.').")

    def strptimedelta_in_seconds(self, user_input, pattern):
        '''
        Parse the string representation of a time delta object.
        Return value: int in seconds or None if parsing failed.
        '''
        # I cannot use `datetime.datetime.strptime(user_input, pattern) - datetime.datetime.strptime("", "")` because it treats no day as 1 day
        p = pattern
        p = p.replace('%H', '{hour}')
        p = p.replace('%M', '{min}')
        p = p.replace('%S', '{s}')
        p = p.replace('%f', '{ms}')
        p = p.replace('%d', '{day}')
        p = re.escape(p)
        p = p.replace(r'\{hour\}', '(?P<hour>[0-9][0-9]?)')
        p = p.replace(r'\{min\}', '(?P<min>[0-9][0-9]?)')
        p = p.replace(r'\{s\}', '(?P<s>[0-9][0-9]?)')
        p = p.replace(r'\{ms\}', '(?P<ms>[0-9]+)')
        p = p.replace(r'\{day\}', '(?P<day>[0-9][0-9]?)')
        p += '$'
        m = re.match(p, user_input)
        if m is None:
            return None

        d = m.groupdict('0')
        seconds = float(d.pop('s','0') + '.' + d.pop('ms','0'))
        d = {key:int(d[key]) for key in d}
        return ((d.pop('day',0)*24 + d.pop('hour',0))*60 + d.pop('min',0))*60 + seconds

    def parse_user_input_absolute_time(self, user_input, first_timestamp):
        patterns_year = ['%Y-%m-%d', '%d.%m.%Y']
        patterns_month = ['%m-%d', '%d.%m.']
        patterns_day = ['%d.']
        patterns_hour = ['%H:%M:', '%H:%M:%S', '%H:%M:%S.%f']
        patterns_minute = [':%M:%S', '%M:%S.', '%M:%S.%f']
        patterns_second = ['%S', '%S.%f']

        date_time_sep = ' '
        for patterns in (patterns_year, patterns_month, patterns_day):
            for pattern_date in tuple(patterns):
                for pattern_time in ['%H:%M', *patterns_hour]:
                    patterns.append(pattern_date+date_time_sep+pattern_time)

        patterns_year.append('%Y-%m')

        for attrs, patterns in [
            (['year', 'month', 'day', 'hour', 'minute'], patterns_second),
            (['year', 'month', 'day', 'hour'], patterns_minute),
            (['year', 'month', 'day'], patterns_hour),
            (['year', 'month'], patterns_day),
            (['year'], patterns_month),
            ([], patterns_year),
        ]:
            for p in patterns:
                try:
                    out = datetime.datetime.strptime(user_input, p)
                except ValueError:
                    pass
                else:
                    kw = {a:getattr(first_timestamp,a) for a in attrs}
                    out = out.replace(**kw)
                    return out

        raise ValueError(f"Failed to parse absolute time {user_input!r}.\n\nPlease note that an input like 'xx:xx' is ambiguous. It could be either 'HH:MM' or 'MM:SS'. Please specify what you want by adding a leading or trailing colon: 'HH:MM:' or ':MM:SS' (or 'MM:SS.').")

    def first_parse_timestamp(self, timestamp, linenumber):
        if timestamp is None:
            self.use_timestamp = False
            return linenumber

        try:
            out = self.parse_absolute_timestamp(timestamp)
            self.use_timestamp = True
            self.relative = False
            self.first_timestamp = out
            self._parse_timestamp = self.parse_absolute_timestamp
            return out
        except ValueError:
            pass

        try:
            if float(timestamp) > self.THRESHOLD_ABSOLUTE_SECONDS:
                out = self.parse_absolute_seconds(timestamp)
                self.relative = False
                self.first_timestamp = out
                self._parse_timestamp = self.parse_absolute_seconds
            else:
                out = self.parse_seconds(timestamp)
                self.relative = True
                self._parse_timestamp = self.parse_seconds

            self.use_timestamp = True
            return out
        except ValueError:
            pass

        self.use_timestamp = False
        return linenumber

    def parse_timestamp(self, timestamp, linenumber):
        if self.use_timestamp is None:
            x = self.first_parse_timestamp(timestamp, linenumber)
            self.init_start_stop(x)
            return x

        if self.use_timestamp:
            return self._parse_timestamp(timestamp)
        else:
            return linenumber

    def parse_absolute_timestamp(self, timestamp):
        return datetime.datetime.strptime(timestamp, self.FORMAT_ABSOLUTE_TIMESTAMP)

    @staticmethod
    def parse_absolute_seconds(timestamp):
        return datetime.datetime.fromtimestamp(float(timestamp))

    @staticmethod
    def parse_seconds(timestamp):
        return float(timestamp)

    def get_label(self):
        if self.use_timestamp:
            if self.relative:
                label = "relative time"
            else:
                label = "absolute time"
        else:
            label = "line number"

        if isinstance(self.first_timestamp, datetime.datetime):
            label += self.first_timestamp.strftime(" (start: %d.%m.%Y)")

        return label

def _do_decode(args):
    '''
    The entry point of the program.
    It iterates over all input lines, parses them
    and passes the data to a Plotter object.
    '''
    if plt is None:
        raise MatplotlibNotInstalledError()

    if args.list_styles:
        print("available matplotlib styles:")
        for style in plt.style.available:
            print(f"- {style}")
        return

    if args.show_errors:
        args.show_invalid_syntax = True
        args.show_unknown_frames = True
        args.show_invalid_data = True
    if args.quiet:
        args.ignore_invalid_syntax = True
        args.ignore_unknown_frames = True
        args.ignore_invalid_data = True

    dbase = database.load_file(args.database,
                               encoding=args.encoding,
                               frame_id_mask=args.frame_id_mask,
                               prune_choices=args.prune,
                               strict=not args.no_strict)
    re_format = None
    timestamp_parser = TimestampParser(args)
    if args.show_invalid_syntax:
        # we cannot use a timestamp if we have failed to parse the line
        timestamp_parser.use_timestamp = False
    if args.line_numbers:
        timestamp_parser.use_timestamp = False

    if args.style is not None:
        plt.style.use(args.style)

    plotter = Plotter(dbase, args)

    line_number = 1
    while True:
        line = sys.stdin.readline()

        # Break at EOF.
        if not line:
            break

        line = line.strip('\r\n')
        if not line:
            continue

        # Auto-detect on first valid line.
        if re_format is None:
            mo = RE_CANDUMP.match(line)

            if mo:
                re_format = RE_CANDUMP
            else:
                mo = RE_CANDUMP_LOG.match(line)

                if mo:
                    re_format = RE_CANDUMP_LOG
        else:
            mo = re_format.match(line)

        if mo:
            timestamp, frame_id, data = _mo_unpack(mo)
            timestamp = timestamp_parser.parse_timestamp(timestamp, line_number)
            if args.start is not None and timestamp < args.start:
                line_number += 1
                continue
            elif args.stop is not None and timestamp > args.stop:
                break
            plotter.add_msg(timestamp, frame_id, data)
        elif RE_DECODE.match(line):
            continue
        else:
            plotter.failed_to_parse_line(line_number, line)

        line_number += 1

    plotter.plot(timestamp_parser.get_label())


class Plotter:

    '''
    Decodes the data received from _do_decode further
    and stores them in a Signals object.
    Shows or exports the data plotted by Signals.
    '''

    # ------- initialization -------

    def __init__(self, dbase, args):
        self.dbase = dbase
        self.decode_choices = not args.no_decode_choices
        self.show_invalid_syntax = args.show_invalid_syntax
        self.show_unknown_frames = args.show_unknown_frames
        self.show_invalid_data = args.show_invalid_data
        self.ignore_invalid_syntax = args.ignore_invalid_syntax
        self.ignore_unknown_frames = args.ignore_unknown_frames
        self.ignore_invalid_data = args.ignore_invalid_data
        self.output_filename = args.output_file
        self.signals = Signals(args.signals, args.case_sensitive, args.break_time, args, args.auto_color_ylabels, dbase)

        self.x_invalid_syntax = []
        self.x_unknown_frames = []
        self.x_invalid_data = []

    # ------- while reading data -------

    def add_msg(self, timestamp, frame_id, data):
        try:
            message = self.dbase.get_message_by_frame_id(frame_id)
        except KeyError:
            if self.show_unknown_frames:
                self.x_unknown_frames.append(timestamp)
            if not self.ignore_unknown_frames:
                print(f'Unknown frame id {frame_id} (0x{frame_id:x})')
            return

        try:
            decoded_signals = message.decode(data, self.decode_choices)
        except Exception as e:
            if self.show_invalid_data:
                self.x_invalid_data.append(timestamp)
            if not self.ignore_invalid_data:
                print(f'Failed to parse data of frame id {frame_id} (0x{frame_id:x}): {e}')
            return

        for signal in decoded_signals:
            x = timestamp
            y = decoded_signals[signal]
            if isinstance(y, NamedSignalValue):
                y = str(y)
            signal = message.name + '.' + signal
            self.signals.add_value(signal, x, y)

    def failed_to_parse_line(self, timestamp, line):
        if self.show_invalid_syntax:
            self.x_invalid_syntax.append(timestamp)
        if not self.ignore_invalid_syntax:
            print(f"Failed to parse line: {line!r}")

    # ------- at end -------

    def plot(self, xlabel):
        self.signals.plot(xlabel, self.x_invalid_syntax, self.x_unknown_frames, self.x_invalid_data)
        if self.output_filename:
            plt.savefig(self.output_filename)
            print(f"Result written to {self.output_filename}")
        else:
            plt.show()

class Signals:

    '''
    Parses the command line options which signals should be plotted
    and saves the corresponding values in Graph objects.
    Automatically inserts None values as specified by break_time.
    Plots the values using matplotlib.pyplot.
    '''

    # added between signal names used as default ylabel
    YLABEL_SEP = ', '

    # before re.escape
    SEP_SUBPLOT = '-'
    SEP_AXES = ','

    SEP_FMT = ':'
    FMT_STEM = '|'

    # after re.escape
    SEP_SG = re.escape('.')

    WILDCARD_MANY = re.escape('*')
    WILDCARD_ONE  = re.escape('?')

    COLOR_INVALID_SYNTAX = '#ff0000'
    COLOR_UNKNOWN_FRAMES = '#ffab00'
    COLOR_INVALID_DATA   = '#ff00ff'
    ERROR_LINEWIDTH = 1

    FIRST_SUBPLOT = 1
    FIRST_AXIS = 0

    # ------- initialization -------

    def __init__(self, signals, case_sensitive, break_time, global_subplot_args, auto_color_ylabels, dbase):
        self.dbase = dbase
        self.args = signals
        self.global_subplot_args = global_subplot_args
        self.signals = []
        self.values = {}
        self.re_flags = 0 if case_sensitive else re.IGNORECASE
        self.break_time = break_time
        self.break_time_uninit = True
        self.subplot = self.FIRST_SUBPLOT
        self.subplot_axis = self.FIRST_AXIS
        self.subplot_args = {}
        self.subplot_argparser = argparse.ArgumentParser()
        self.subplot_argparser.add_argument('signals', nargs='*')
        add_subplot_options(self.subplot_argparser)

        i0 = 0
        while True:
            try:
                i1 = signals.index(self.SEP_SUBPLOT, i0)
            except ValueError:
                i1 = None

            try:
                i12 = signals.index(self.SEP_AXES, i0)
            except ValueError:
                i12 = None
            if i1 is None or (i12 is not None and i12 < i1):
                i1 = i12

            subplot_signals = signals[i0:i1]
            subplot_args = self.subplot_argparser.parse_args(subplot_signals)
            if auto_color_ylabels and subplot_args.color is None:
                subplot_args.color = f"C{self.subplot_axis}"
            self.subplot_args[(self.subplot, self.subplot_axis)] = subplot_args
            self._ylabel = ""
            for sg in subplot_args.signals:
                self.add_signal(sg)
            if subplot_args.ylabel is None and self._ylabel:
                subplot_args.ylabel = self._ylabel

            if i1 is None:
                break

            if signals[i1] == self.SEP_SUBPLOT:
                self.subplot += 1
                self.subplot_axis = self.FIRST_AXIS
            else:
                self.subplot_axis += 1
            i0 = i1 + 1

        if not self.signals:
            self.add_signal('*')

        self.compile_reo()

    def init_break_time(self, datatype):
        if self.break_time <= 0:
            self.break_time = None
        elif datatype == datetime.datetime:
            self.half_break_time = datetime.timedelta(seconds=self.break_time/2)
            self.break_time = datetime.timedelta(seconds=self.break_time)
        else:
            self.half_break_time = self.break_time / 2
        self.break_time_uninit = False

    def add_signal(self, signal):
        if self.SEP_FMT in signal:
            signal, fmt = signal.split(self.SEP_FMT, 1)
            if fmt.startswith(self.FMT_STEM):
                fmt = fmt[len(self.FMT_STEM):]
                plt_func = 'stem'
            else:
                plt_func = 'plot'
        else:
            fmt = ''
            plt_func = 'plot'

        if self._ylabel:
            self._ylabel += self.YLABEL_SEP
        self._ylabel += signal

        signal = re.escape(signal)
        if self.SEP_SG not in signal:
            signal = self.WILDCARD_MANY + self.SEP_SG + signal
        signal = signal.replace(self.WILDCARD_MANY, '.*')
        signal = signal.replace(self.WILDCARD_ONE, '.')
        signal += '$'
        reo = re.compile(signal, self.re_flags)

        sgo = Signal(reo, self.subplot, self.subplot_axis, plt_func, fmt)
        self.signals.append(sgo)

    def compile_reo(self):
        self.reo = re.compile('|'.join(sg.reo.pattern for sg in self.signals), re.IGNORECASE)

    # ------- while reading data -------

    def add_value(self, signal, x, y):
        if not self.is_displayed_signal(signal):
            return

        if signal not in self.values:
            graph = Graph()
            self.values[signal] = graph
        else:
            graph = self.values[signal]
            last_x = graph.x[-1]
            if self.break_time_uninit:
                self.init_break_time(type(x))
            if self.break_time and last_x + self.break_time < x:
                x_break = last_x + self.half_break_time
                graph.x.append(x_break)
                graph.y.append(None)
        graph.x.append(x)
        graph.y.append(y)

    def is_displayed_signal(self, signal):
        return self.reo.match(signal)

    # ------- at end -------

    SUBPLOT_DIRECT_NAMES = ('title', 'ylabel')
    def plot(self, xlabel, x_invalid_syntax, x_unknown_frames, x_invalid_data):
        self.default_xlabel = xlabel
        splot = None
        last_subplot = self.FIRST_SUBPLOT - 1
        last_axis = None
        axis_format_uninitialized = True
        sorted_signal_names = sorted(self.values.keys())
        self.legend_handles = []
        self.legend_labels = []
        for sgo in self.signals:
            if sgo.subplot > last_subplot:
                if splot is None:
                    axes = None
                else:
                    axes = splot.axes
                    self.finish_subplot(splot, self.subplot_args[(last_subplot, last_axis)])

                splot = plt.subplot(self.subplot, 1, sgo.subplot, sharex=axes)

                last_subplot = sgo.subplot
                last_axis = sgo.axis
            elif sgo.axis > last_axis:
                self.finish_axis(splot, self.subplot_args[(last_subplot, last_axis)])
                splot = splot.twinx()
                last_axis = sgo.axis

            plotted = False
            for signal_name in sorted_signal_names:
                graph = self.values[signal_name]
                if not sgo.match(signal_name):
                    continue
                if graph.plotted_signal:
                    if not self.is_replotting_desired(sgo, graph.plotted_signal):
                        continue
                else:
                    graph.plotted_signal = sgo

                x = graph.x
                y = graph.y
                if axis_format_uninitialized and x:
                    if isinstance(x[0], float):
                        splot.axes.xaxis.set_major_formatter(lambda x,pos: str(datetime.timedelta(seconds=x)))
                    axis_format_uninitialized = False
                plt_func = getattr(splot, sgo.plt_func)
                if self.global_subplot_args.show_units and self.subplot_args[(sgo.subplot, sgo.axis)].show_units:
                    unit = self.get_signal_unit(signal_name)
                    signal_name = f"{signal_name} [{unit}]"
                container = plt_func(x, y, sgo.fmt, label=signal_name)
                color = self.subplot_args[(sgo.subplot, sgo.axis)].color
                if color is not None and self.contains_no_color(sgo.fmt):
                    for line in container:
                        line.set_color(color)
                plotted = True

            if not plotted:
                print(f"WARNING: signal {sgo.reo.pattern!r} with format {sgo.fmt!r} was not plotted.")

        self.plot_error(splot, x_invalid_syntax, 'invalid syntax', self.COLOR_INVALID_SYNTAX)
        self.plot_error(splot, x_unknown_frames, 'unknown frames', self.COLOR_UNKNOWN_FRAMES)
        self.plot_error(splot, x_invalid_data, 'invalid data', self.COLOR_INVALID_DATA)
        self.finish_subplot(splot, self.subplot_args[(last_subplot, last_axis)])

    def finish_axis(self, splot, subplot_args):
        kw = {key:val for key,val in vars(subplot_args).items() if val is not None and key in self.SUBPLOT_DIRECT_NAMES}
        for key in self.SUBPLOT_DIRECT_NAMES:
            if key not in kw:
                val = getattr(self.global_subplot_args, key)
                if val is not None:
                    kw[key] = val
        if kw:
            splot.set(**kw)

        if subplot_args.xlabel is not None:
            xlabel = subplot_args.xlabel
        elif self.global_subplot_args.xlabel is not None:
            xlabel = self.global_subplot_args.xlabel
        else:
            xlabel = self.default_xlabel
        splot.set_xlabel(xlabel)

        if subplot_args.ymin is None:
            subplot_args.ymin = self.global_subplot_args.ymin
        if subplot_args.ymax is None:
            subplot_args.ymax = self.global_subplot_args.ymax
        if subplot_args.ymin is not None or subplot_args.ymax is not None:
            splot.axes.set_ylim(subplot_args.ymin, subplot_args.ymax)

        if subplot_args.color is not None:
            splot.yaxis.label.set_color(subplot_args.color)
            splot.tick_params(axis='y', which='both', colors=subplot_args.color)

        handles, labels = splot.get_legend_handles_labels()
        self.legend_handles.extend(handles)
        self.legend_labels.extend(labels)

    def finish_subplot(self, splot, subplot_args):
        self.finish_axis(splot, subplot_args)
        splot.legend(self.legend_handles, self.legend_labels)
        self.legend_handles = []
        self.legend_labels = []

    def contains_no_color(self, fmt):
        for c in fmt:
            if c in PYPLOT_BASE_COLORS:
                return False
        return True

    def plot_error(self, splot, xs, label, color):
        if xs:
            label += f" ({len(xs)})"
            xs = iter(xs)
            splot.axvline(next(xs), color=color, linewidth=self.ERROR_LINEWIDTH, label=label)
            for x in xs:
                splot.axvline(x, color=color, linewidth=self.ERROR_LINEWIDTH)

    def is_replotting_desired(self, current_signal, previously_plotted_signal):
        if current_signal.reo.pattern == previously_plotted_signal.reo.pattern:
            # if the user bothers to type out the same regex twice
            # it is probably intended to be plotted twice
            return True
        if '.' not in current_signal.reo.pattern:
            # if the user bothers to type out a complete signal name without wildcards
            # he/she probably means to plot this signal even if it has been plotted already
            return True

        return False

    def get_signal_unit(self, signal_name):
        msg, signal = re.split(self.SEP_SG, signal_name)
        return self.dbase.get_message_by_name(msg).get_signal_by_name(signal).unit

class Signal:

    '''
    Stores meta information about signals to be plotted:
    - a regex matching all signals it refers to
    - the format how it should be plotted
    - the subplot in which to display the signal

    It does *not* store the values to be plotted.
    They are stored in Graph.
    Signal and Graph have a one-to-many-relationship.
    '''

    # ------- initialization -------

    def __init__(
        self, reo: "re.Pattern[str]",
        subplot: int,
        axis: int,
        plt_func: str,
        fmt: str,
    ) -> None:
        self.reo = reo
        self.subplot = subplot
        self.axis = axis
        self.plt_func = plt_func
        self.fmt = fmt

    # ------- while reading data -------

    def match(self, signal):
        return self.reo.match(signal)

class Graph:

    '''
    A container for the values to be plotted.
    The corresponding signal names are the keys in Signals.values.
    The format how to plot this data is stored in Signals.signals (a list of Signal objects).

    plotted_signal stores a Signal object with which this graph has been plotted already
    to avoid undesired replotting of the same data in case the user gives two regex
    matching the same signal, one more specific to match a certain signal with a special format
    and one more generic matching the rest with another format.
    '''

    __slots__ = ('plotted_signal', 'x', 'y')

    def __init__(self):
        self.x = []
        self.y = []
        self.plotted_signal = None


class RawDescriptionArgumentDefaultsHelpFormatter(
    argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    pass


def add_subparser(subparsers):
    '''
    Is called from ../__init__.py.
    It adds the options for this subprogram to the argparse parser.
    It sets the entry point for this subprogram by setting a default values for func.
    '''
    plot_parser = subparsers.add_parser(
        'plot',
        description=__doc__,
        formatter_class=RawDescriptionArgumentDefaultsHelpFormatter)
    plot_parser.add_argument(
        '-c', '--no-decode-choices',
        action='store_true',
        help='Do not convert scaled values to choice strings.')
    plot_parser.add_argument(
        '-e', '--encoding',
        help='File encoding of dbc file.')
    plot_parser.add_argument(
        '-m', '--frame-id-mask',
        type=Integer(0),
        help=('Only compare selected frame id bits to find the message in the '
              'database. By default the candump and database frame ids must '
              'be equal for a match.'))
    plot_parser.add_argument(
        '-I', '--case-sensitive',
        action='store_true',
        help='Match the signal names case sensitive.')
    plot_parser.add_argument(
        '-l', '--line-numbers',
        action='store_true',
        help='Use line numbers instead of time stamps on the horizontal axis (useful with `candump -td`).')
    plot_parser.add_argument(
        '-t', '--break-time',
        default=100,
        type=float,
        help=('If the time distance between two consecutive signals is longer than this value '
              'the line in the plot will be interrupted. The value is given in seconds '
              '(if timestamps are used) or input lines (if line numbers are used). '
              '-1 means infinite. '))

    plot_parser.add_argument(
        '--show-invalid-syntax',
        action='store_true',
        help='Show a marker for lines which could not be parsed. This implies -l.')
    plot_parser.add_argument(
        '--show-unknown-frames',
        action='store_true',
        help='Show a marker for messages which are not contained in the database file.')
    plot_parser.add_argument(
        '--show-invalid-data',
        action='store_true',
        help='Show a marker for messages with data which could not be parsed.')
    plot_parser.add_argument(
        '-s', '--show-errors',
        action='store_true',
        help='Show all error messages in the plot. This is an abbreviation for all --show-* options. This implies -l.')

    plot_parser.add_argument(
        '--ignore-invalid-syntax',
        action='store_true',
        help='Don\'t print an error message for lines which could not be parsed.')
    plot_parser.add_argument(
        '--ignore-unknown-frames',
        action='store_true',
        help='Don\'t print an error message for messages which are not contained in the database file.')
    plot_parser.add_argument(
        '--ignore-invalid-data',
        action='store_true',
        help='Don\'t print an error message for messages with data which could not be parsed.')
    plot_parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Don\'t print any error messages. This is an abbreviation for all --ignore-* options.')

    plot_parser.add_argument(
        '-o', '--output-file',
        help='A file to write the plot to instead of displaying it in a window.')

    plot_parser.add_argument(
        '-ss', '--start',
        help='A start time or line number. Everything before is ignored. '
             'This filters the lines/messages to be processed. It does *not* set the minimum value of the x-axis.')
    plot_parser.add_argument(
        '-to', '--stop',
        help='An end time or line number. Everything after is ignored. '
             'This filters the lines/messages to be processed. It does *not* set the maximum value of the x-axis.')

    plot_parser.add_argument(
        '--style',
        help='The matplotlib style to be used.')
    plot_parser.add_argument(
        '--list-styles',
        action='store_true',
        help='Print all available matplotlib styles without drawing a plot.')
    plot_parser.add_argument(
        '-ac', '--auto-color-ylabels',
        action='store_true',
        help='This is equivalent to applying --color C0 to the first y-axis, --color C1 to the second and so on.')
    plot_parser.add_argument(
        '--prune',
        action='store_true',
        help='Try to shorten the names of named signal choices.')
    plot_parser.add_argument(
        '--no-strict',
        action='store_true',
        help='Skip database consistency checks.')

    plot_parser.add_argument(
        'database',
        help='Database file.')
    plot_parser.add_argument(
        'signals',
        nargs='*',
        help='The signals to be plotted.')
    plot_parser.set_defaults(func=_do_decode)

    subplot_arggroup = plot_parser.add_argument_group('subplot arguments',
        '''\
The following options can be used to configure the subplots/axes.
If they shall apply to a specific subplot/axis they must be placed among the signals for that subplot/axis and a -- must mark the end of the global optional arguments.
Otherwise they are used as default value for each subplot/axis.
''')
    add_subplot_options(subplot_arggroup)

def add_subplot_options(arg_group):
    arg_group.add_argument('--title')
    arg_group.add_argument('--color',
        help='The color to be used for the y-label and the signals (unless a different color is given for the signal). '
             'All string formats explained in the following link are allowed: https://matplotlib.org/tutorials/colors/colors.html')
    arg_group.add_argument('--xlabel')
    arg_group.add_argument('--ylabel')
    arg_group.add_argument('--ymin', type=float)
    arg_group.add_argument('--ymax', type=float)
    arg_group.add_argument(
        '--no-units',
        dest='show_units',
        action='store_false',
        help='Do not show units in legend labels.')
    return arg_group
