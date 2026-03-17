import argparse
import bisect
import curses
import queue
import re
import sys
import time
import warnings
from enum import Enum
from typing import Any

import can.cli
from argparse_addons import Integer  # type: ignore

from cantools.database.errors import DecodeError

from .. import database
from ..typechecking import SignalDictType
from .__utils__ import (
    format_multiplexed_name,
    format_signals,
    parse_additional_config,
)


class QuitError(Exception):
    pass

class MessageFormattingResult(Enum):
    Ok = 0
    UnknownMessage = 1
    DecodeError = 2

class Monitor(can.Listener):

    def __init__(self, stdscr: Any, args: argparse.Namespace):
        self._stdscr = stdscr
        print(f'Reading bus description file "{args.database}"...\r')
        self._dbase = database.load_file(args.database,
                                         encoding=args.encoding,
                                         frame_id_mask=args.frame_id_mask,
                                         prune_choices=args.prune,
                                         strict=not args.no_strict)
        self._single_line = args.single_line
        self._filtered_sorted_message_names: list[str] = []
        self._filter = ''
        self._filter_cursor_pos = 0
        self._compiled_filter = None
        self._formatted_messages: dict[str, list[str]] = {}
        self._raw_messages: dict[str, can.Message] = {}
        self._message_signals: dict[str, set[str]] = {}
        self._message_filtered_signals: dict[str, set[str]] = {}
        self._messages_with_error: set[str] = set()
        self._playing = True
        self._modified = True
        self._show_filter = False
        self._queue: queue.Queue = queue.Queue()
        self._nrows, self._ncols = stdscr.getmaxyx()
        self._received = 0
        self._discarded = 0
        self._errors = 0
        self._basetime: float | None = None
        self._page_first_row = 0

        stdscr.keypad(True)
        stdscr.nodelay(True)
        curses.use_default_colors()
        curses.curs_set(False)
        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_GREEN)
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_CYAN)
        curses.init_pair(3, curses.COLOR_CYAN, curses.COLOR_BLACK)

        if "bus_type" in args:
            # TODO: this branch can be removed once legacy args are removed
            bus = self.create_bus(args)
        else:
            bus = can.cli.create_bus_from_namespace(args)

        self._notifier = can.Notifier(bus, [self])

    def create_bus(self, args):
        kwargs = {}

        if args.bit_rate is not None:
            kwargs['bitrate'] = int(args.bit_rate)

        if args.fd:
            kwargs['fd'] = True

        if args.extra_args:
            kwargs.update(parse_additional_config(args.extra_args))

        try:
            return can.Bus(interface=args.bus_type,
                           channel=args.channel,
                           **kwargs)
        except Exception as exc:
            raise Exception(
                f"Failed to create CAN bus with bustype='{args.bus_type}' and "
                f"channel='{args.channel}'."
            ) from exc

    def run(self, max_num_keys_per_tick=-1):
        while True:
            try:
                self.tick(max_num_keys_per_tick)
            except QuitError:
                break

            time.sleep(0.05)

    def tick(self, max_num_keys=-1):
        modified = self.update()

        if modified:
            self.redraw()

        self.process_user_input(max_num_keys)

    def redraw(self):
        # Clear the screen.
        self._stdscr.erase()

        # Draw everything.
        self.draw_stats(0)
        self.draw_title(1)

        lines = []

        for name in self._filtered_sorted_message_names:
            for line in self._formatted_messages[name]:
                lines.append(line)

        # Only render the visible screen. We only have (self._nrows - 3)
        # available rows to draw on, due to the persistent TUI features that
        # are drawn:
        #
        # - line 0: stats
        # - line 1: title
        # - line (n - 1): menu
        num_actual_usable_rows = self._nrows - 2 - 1
        row = 2

        # make sure that we don't overshoot the last line of
        # content. this is a bit of a hack, because manipulation of
        # the controls is not supposed to happen within this method
        if len(lines) < self._page_first_row + num_actual_usable_rows:
            self._page_first_row = max(0, len(lines) - num_actual_usable_rows)

        for line in lines[self._page_first_row:self._page_first_row + num_actual_usable_rows]:
            self.addstr(row, 0, line)
            row += 1

        self.draw_menu(self._nrows - 1)

        # Refresh the screen.
        self._stdscr.refresh()

    def draw_stats(self, row):
        status_text = \
            f'Received: {self._received}, Discarded: {self._discarded}, Errors: {self._errors}'
        if self._filter:
            status_text += f', Filter: {self._filter}'
        self.addstr(row, 0, status_text)

    def draw_title(self, row):
        self.addstr_color(row,
                          0,
                          self.stretch('   TIMESTAMP  MESSAGE'),
                          curses.color_pair(1))

    def draw_menu(self, row):
        if self._show_filter:
            col = 0

            # text before cursor
            text = 'Filter regex: ' + self._filter[:self._filter_cursor_pos]
            self.addstr_color(row,
                              col,
                              text,
                              curses.color_pair(2))

            col = len(text)

            # cursor
            if self._filter_cursor_pos >= len(self._filter):
                c = " "
            else:
                c = self._filter[self._filter_cursor_pos]
            self.addstr_color(row,
                              col,
                              c,
                              curses.color_pair(3))
            col += 1

            # text after cursor
            text = self._filter[self._filter_cursor_pos + 1:]
            if len(text) > 0:
                self.addstr_color(row,
                                  col,
                                  text,
                                  curses.color_pair(2))
                col += len(text)

            # fill rest of line
            self.addstr_color(row,
                              col,
                              ' '*(self._ncols - col),
                              curses.color_pair(2))
        else:
            text = 'q: Quit, f: Filter, p: Play/Pause, r: Reset'

            self.addstr_color(row,
                              0,
                              self.stretch(text),
                              curses.color_pair(2))

    def addstr(self, row, col, text):
        try:
            self._stdscr.addstr(row, col, text)
        except curses.error:
            pass

    def addstr_color(self, row, col, text, color):
        try:
            self._stdscr.addstr(row, col, text, color)
        except curses.error:
            pass

    def stretch(self, text):
        return text + ' ' * (self._ncols - len(text))

    def process_user_input(self, max_num_keys=-1):
        while max_num_keys < 0 or max_num_keys > 0:
            max_num_keys -= 1
            try:
                key = self._stdscr.getkey()
            except curses.error:
                return

            if self._show_filter:
                self.process_user_input_filter(key)
            else:
                self.process_user_input_menu(key)

    def process_user_input_menu(self, key):
        if key == 'q':
            raise QuitError()
        elif key == 'p':
            self._playing = not self._playing
        elif key == 'r':
            self._playing = True
            self._filtered_sorted_message_names = []
            self._formatted_messages = {}
            self._received = 0
            self._discarded = 0
            self._basetime = None
            self._filter = ''
            self._compiled_filter = None
            self._modified = True
            self._page = 0

            while not self._queue.empty():
                self._queue.get()
        elif key in ['f', '/']:
            self._old_filter = self._filter
            self._show_filter = True
            self._filter_cursor_pos = len(self._filter)
            self._modified = True
            curses.curs_set(True)
        elif key == 'KEY_UP':
            self.line_up()
        elif key == 'KEY_DOWN':
            self.line_down()
        elif key == 'KEY_PPAGE':
            self.page_up()
        elif key == 'KEY_NPAGE':
            self.page_down()

    def line_down(self):
        # Increment line
        self._page_first_row += 1

        self._modified = True

    def line_up(self):
        # Decrement line
        if self._page_first_row > 0:
            self._page_first_row -= 1
        else:
            self._page_first_row = 0

        self._modified = True

    def page_up(self):
        num_actual_usable_rows = self._nrows - 2 - 1

        # Decrement page
        if self._page_first_row > num_actual_usable_rows:
            self._page_first_row -= num_actual_usable_rows
        else:
            self._page_first_row = 0

        self._modified = True

    def page_down(self):
        num_actual_usable_rows = self._nrows - 2 - 1

        # Increment page
        self._page_first_row += num_actual_usable_rows

        self._modified = True

    def compile_filter(self):
        try:
            self._compiled_filter = re.compile(self._filter, re.IGNORECASE)
        except (TypeError, re.error):
            self._compiled_filter = None

    def process_user_input_filter(self, key):
        if key == '\n':
            self._show_filter = False
            curses.curs_set(False)
        elif key == chr(27):
            # Escape
            self._show_filter = False
            self._filter = self._old_filter
            del self._old_filter
            curses.curs_set(False)
        elif key in ['KEY_BACKSPACE', '\b']:
            if self._filter_cursor_pos > 0:
                self._filter = \
                    self._filter[:self._filter_cursor_pos - 1] + \
                    self._filter[self._filter_cursor_pos:]
                self._filter_cursor_pos -= 1
        elif key == 'KEY_DC':
            # delete key
            if self._filter_cursor_pos < len(self._filter):
                self._filter = \
                    self._filter[:self._filter_cursor_pos] + \
                    self._filter[self._filter_cursor_pos + 1:]
        elif key == 'KEY_LEFT':
            if self._filter_cursor_pos > 0:
                self._filter_cursor_pos -= 1
        elif key == 'KEY_RIGHT':
            if self._filter_cursor_pos < len(self._filter):
                self._filter_cursor_pos += 1
        elif key == 'KEY_UP':
            self.line_up()
        elif key == 'KEY_DOWN':
            self.line_down()
        elif key == 'KEY_PPAGE':
            self.page_up()
        elif key == 'KEY_NPAGE':
            self.page_down()
        # we ignore keys with more than one character here. These
        # (mostly?) are control keys like KEY_UP, KEY_DOWN, etc.
        elif len(key) == 1:
            self._filter = \
                self._filter[:self._filter_cursor_pos] + \
                key + \
                self._filter[self._filter_cursor_pos:]
            self._filter_cursor_pos += 1

        self.compile_filter()

        # reformat all messages
        self._filtered_sorted_message_names.clear()
        self._message_signals.clear()
        self._formatted_messages.clear()
        for msg in self._raw_messages.values():
            self.try_update_message(msg)

        self._modified = True

    def try_update_message(self, raw_message: can.Message) -> MessageFormattingResult:
        if self._basetime is None:
            self._basetime = raw_message.timestamp

        data = raw_message.data
        timestamp = raw_message.timestamp - self._basetime

        try:
            message = self._dbase.get_message_by_frame_id(raw_message.arbitration_id, raw_message.is_extended_id) # type: ignore[union-attr]
        except KeyError:
            return MessageFormattingResult.UnknownMessage

        name = message.name
        try:
            if message.is_container:
                self._try_update_container(message, timestamp, data)
            else:
                if len(data) < message.length:
                    self._update_message_error(timestamp, name, data, f'{message.length - len(data)} bytes too short')
                    return MessageFormattingResult.DecodeError

                decoded_signals = message.decode_simple(data,
                    decode_choices=True,
                    allow_truncated=True,
                    allow_excess=True
                )

                name, formatted = self._format_message(timestamp, message, decoded_signals)
                self._update_formatted_message(name, formatted)
            self._raw_messages[name] = raw_message
            return MessageFormattingResult.Ok
        except DecodeError as e:
            # Discard the message in case of any decoding error, like we do when the
            # CAN message ID or length doesn't match what's specified in the DBC.
            self._update_message_error(timestamp, name, data, str(e))
            return MessageFormattingResult.DecodeError

    def _try_update_container(self, dbmsg, timestamp, data):
        decoded = dbmsg.decode(data, decode_containers=True)

        # handle the "table of contents" of the container message. To
        # avoid too much visual turmoil and the resulting usability issues,
        # we always put the contained messages on a single line
        contained_names = []
        for cmsg, _ in decoded:
            if isinstance(cmsg, int):
                tmp = dbmsg.get_contained_message_by_header_id(cmsg)
                cmsg_name = f'0x{cmsg:x}' if tmp is None else tmp.name
            else:
                cmsg_name = cmsg.name

            contained_names.append(cmsg_name)

        self._message_signals[dbmsg.name] = set(contained_names)
        self._update_formatted_message(dbmsg.name, self._format_lines(timestamp, dbmsg.name, contained_names))

        # handle the contained messages just as normal messages but
        # prefix their names with the name of the container followed
        # by '::'
        for cmsg, cdata in decoded:
            if isinstance(cmsg, int):
                tmp = dbmsg.get_contained_message_by_header_id(cmsg)
                cmsg_name = f'0x{cmsg:x}' if tmp is None else tmp.name
                full_name = f'{dbmsg.name} :: {cmsg_name}'

                if len(cdata) == 0:
                    cdata_str = f'<empty>'
                else:
                    cdata_str = f'0x{cdata.hex()}'

                formatted = self._format_lines(timestamp, full_name, [f'undecoded: {cdata_str}'])
            else:
                full_name, formatted = self._format_message(timestamp, cmsg, cdata, name_prefix=f'{dbmsg.name} :: ')
            self._update_formatted_message(full_name, formatted)

    def _format_message(self, timestamp: float, message: database.Message, decoded_signals: SignalDictType, name_prefix: str = '') -> tuple[str, list[str]]:
        name = message.name
        if message.is_multiplexed():
            name = format_multiplexed_name(message, decoded_signals)
        name = f'{name_prefix}{name}'

        filtered_signals = self._filter_signals(name, decoded_signals)
        formatted_signals = format_signals(message, filtered_signals)
        return name, self._format_lines(timestamp, name, formatted_signals)

    def _format_lines(self, timestamp: float, name: str, items: list[str], single_line: bool=False) -> list[str]:
        prefix = f'{timestamp:12.3f}  {name}('
        if self._single_line or single_line:
            formatted = [
                f'''{prefix}{', '.join(items)})'''
            ]
        else:
            formatted = [prefix]
            formatted += [f"{' ':<18}{line}{',' if index + 1 < len(items) else ''}" for index, line in enumerate(items)]
            formatted += [f"{' ':<14})"]
        return formatted


    def _filter_signals(self, name: str, signals: SignalDictType) -> SignalDictType:
        if name not in self._message_signals:
            self._message_signals[name] = self._message_filtered_signals[name] = set(signals.keys())
            self.insort_filtered(name)

        return {s: v for s, v in signals.items() if s in self._message_filtered_signals[name]}

    def _update_formatted_message(self, msg_name, formatted, is_error=False):
        old_formatted = self._formatted_messages.get(msg_name, [])

        # make sure never to decrease the number of lines occupied by
        # a message to avoid jittering
        if len(formatted) < len(old_formatted):
            formatted.extend(['']*(len(old_formatted) - len(formatted)))

        self._formatted_messages[msg_name] = formatted

        if is_error:
            self._messages_with_error.add(msg_name)
        else:
            self._messages_with_error.discard(msg_name)

        if msg_name not in self._filtered_sorted_message_names:
            self.insort_filtered(msg_name)

    def _update_message_error(self, timestamp, msg_name, data, error):
        formatted = self._format_lines(
            timestamp,
            msg_name,
            [f'undecoded, {error}: 0x{data.hex()}'],
            single_line=True
        )
        self._update_formatted_message(msg_name, formatted, is_error=True)

    def update_messages(self):
        modified = False

        try:
            while True:
                result = self.try_update_message(self._queue.get_nowait())
                self._received += 1
                if result == MessageFormattingResult.UnknownMessage:
                    self._discarded += 1
                elif result == MessageFormattingResult.DecodeError:
                    self._errors += 1
                modified = True
        except queue.Empty:
            pass

        return modified

    def update(self):
        if self._playing:
            modified = self.update_messages()
        else:
            modified = False

        if self._modified:
            self._modified = False
            modified = True

        if curses.is_term_resized(self._nrows, self._ncols):
            self._nrows, self._ncols = self._stdscr.getmaxyx()
            modified = True

        return modified

    def _signals_matching_filter(self, message_name: str) -> set[str]:
        all_signals = self._message_signals[message_name]

        # return all signals if:
        # - there is no filter
        # - message name matches the filter
        if self._compiled_filter is None or self._compiled_filter.search(message_name):
            return all_signals

        return {signal for signal in all_signals if self._compiled_filter.search(signal)}

    def _message_matches_filter(self, name: str) -> bool:
        # don't filter invalid messages as signals are unknown
        if name not in self._message_signals:
            return True

        matched_signals = self._signals_matching_filter(name)
        if matched_signals:
            self._message_filtered_signals[name] = matched_signals
        return bool(matched_signals)

    def insort_filtered(self, name):
        if name in self._messages_with_error or self._message_matches_filter(name):
            bisect.insort(self._filtered_sorted_message_names,
                          name)

    def on_message_received(self, msg):
        self._queue.put(msg)


def _do_monitor(args):
    def monitor(stdscr):
        Monitor(stdscr, args).run()

    try:
        curses.wrapper(monitor)
    except KeyboardInterrupt:
        pass


def _check_legacy_args() -> bool:
    """Return True if legacy CAN bus arguments were given."""
    args = sys.argv[1:]  # already a list of strings

    # New style explicitly supported -> no legacy mode
    if "-i" in args or "--interface" in args:
        return False

    # Special case: "-b" meaning change
    if "-b" in args:
        b_index = args.index("-b")
        # Legacy if the value after -b is non-numeric (old bus type behavior)
        if len(args) > b_index + 1 and not args[b_index + 1].isdigit():
            return True

    # Other explicit legacy indicators
    if any(arg in args for arg in ("--bus-type", "--bit-rate", "-B")):
        return True

    return False


def _warn_legacy_usage():
    """Emit a warning if legacy args were detected."""
    warnings.warn(
        "You are using legacy CAN bus arguments (-b, --bus-type, -B, etc.). "
        "These are deprecated and will be removed in a future release. "
        "Please use the new bus argument format via `--interface` and related options.",
        UserWarning,
        stacklevel=2
    )


def add_subparser(subparsers):
    monitor_parser = subparsers.add_parser(
        'monitor',
        description='Monitor CAN bus traffic in a text based user interface.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    monitor_parser.add_argument(
        '-s', '--single-line',
        action='store_true',
        help='Print the decoded message on a single line.')
    monitor_parser.add_argument(
        '-e', '--encoding',
        help='File encoding.')
    monitor_parser.add_argument(
        '-m', '--frame-id-mask',
        type=Integer(0),
        help=('Only compare selected frame id bits to find the message in the '
              'database. By default the received and database frame ids must '
              'be equal for a match.'))
    monitor_parser.add_argument(
        '--prune',
        action='store_true',
        help='Refrain from shortening the names of named signal values.')
    monitor_parser.add_argument(
        '--no-strict',
        action='store_true',
        help='Skip database consistency checks.')

    if _check_legacy_args():
        _warn_legacy_usage()
        monitor_parser.add_argument(
            '-b', '--bus-type',
            default='socketcan',
            help='(Deprecated) Python CAN bus type.')
        monitor_parser.add_argument(
            '-c', '--channel',
            default='vcan0',
            help='(Deprecated) Python CAN bus channel.')
        monitor_parser.add_argument(
            '-B', '--bit-rate',
            help='(Deprecated) Python CAN bus bit rate.')
        monitor_parser.add_argument(
            '-f', '--fd',
            action='store_true',
            help='(Deprecated) Python CAN CAN-FD bus.')
        monitor_parser.add_argument(
            'extra_args',
            nargs=argparse.REMAINDER,
            help="(Deprecated) Remaining arguments will be used for the interface. "
                 "Example: `-c can0 -b socketcand --host=192.168.0.10 --port=29536` "
                 "is equivalent to `Bus('can0', 'socketcand', host='192.168.0.10', port=29536)`")
    else:
        can.cli.add_bus_arguments(monitor_parser, filter_arg=True, group_title="bus arguments (python-can)")

    monitor_parser.add_argument(
        'database',
        help='Database file.')
    monitor_parser.set_defaults(func=_do_monitor)
