# Copyright (C) 2018 Kristian Sloth Lauszus.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
# Contact information
# -------------------
# Kristian Sloth Lauszus
# Web      :  http://www.lauszus.com
# e-mail   :  lauszus@gmail.com

import argparse
import errno
import logging
import os
import struct
import sys
import time

from can import __version__
from can.cli import (
    _set_logging_level_from_namespace,
    add_bus_arguments,
    create_bus_from_namespace,
)
from can.typechecking import TDataStructs

logger = logging.getLogger("can.viewer")

try:
    import curses
    from curses.ascii import ESC as KEY_ESC  # type: ignore[attr-defined,unused-ignore]
    from curses.ascii import SP as KEY_SPACE  # type: ignore[attr-defined,unused-ignore]
except ImportError:
    # Probably on Windows while windows-curses is not installed (e.g. in PyPy)
    logger.warning(
        "You won't be able to use the viewer program without curses installed!"
    )
    curses = None  # type: ignore[assignment]


class CanViewer:  # pylint: disable=too-many-instance-attributes
    def __init__(self, stdscr, bus, data_structs, testing=False):
        self.stdscr = stdscr
        self.bus = bus
        self.data_structs = data_structs

        # Initialise the ID dictionary, Previous values dict, start timestamp,
        # scroll and variables for pausing the viewer and enabling byte highlighting
        self.ids = {}
        self.start_time = None
        self.scroll = 0
        self.paused = False
        self.highlight_changed_bytes = False
        self.previous_values = {}

        # Get the window dimensions - used for resizing the window
        self.y, self.x = self.stdscr.getmaxyx()

        # Do not wait for key inputs, disable the cursor and choose the background color
        # automatically
        self.stdscr.nodelay(True)
        curses.curs_set(0)
        curses.use_default_colors()

        # Used to color error frames red
        curses.init_pair(1, curses.COLOR_RED, -1)
        # Used to color changed bytes
        curses.init_pair(2, curses.COLOR_CYAN, curses.COLOR_BLUE)

        if not testing:  # pragma: no cover
            self.run()

    def run(self):
        # Clear the terminal and draw the header
        self.draw_header()

        while True:
            # Do not read the CAN-Bus when in paused mode
            if not self.paused:
                # Read the CAN-Bus and draw it in the terminal window
                msg = self.bus.recv(timeout=1.0 / 1000.0)
                if msg is not None:
                    self.draw_can_bus_message(msg)
            else:
                # Sleep 1 ms, so the application does not use 100 % of the CPU resources
                time.sleep(1.0 / 1000.0)

            # Read the terminal input
            key = self.stdscr.getch()

            # Stop program if the user presses ESC or 'q'
            if key == KEY_ESC or key == ord("q"):
                break

            # Clear by pressing 'c'
            if key == ord("c"):
                self.ids = {}
                self.start_time = None
                self.scroll = 0
                self.draw_header()

            # Toggle byte change highlighting pressing 'h'
            elif key == ord("h"):
                self.highlight_changed_bytes = not self.highlight_changed_bytes
                if not self.highlight_changed_bytes:
                    # empty the previous values dict when leaving higlighting mode
                    self.previous_values.clear()
                self.draw_header()

            # Sort by pressing 's'
            elif key == ord("s"):
                # Sort frames based on the CAN-Bus ID
                self.draw_header()
                for i, key in enumerate(sorted(self.ids.keys())):
                    # Set the new row index, but skip the header
                    self.ids[key]["row"] = i + 1

                    # Do a recursive call, so the frames are repositioned
                    self.draw_can_bus_message(self.ids[key]["msg"], sorting=True)

            # Pause by pressing space
            elif key == KEY_SPACE:
                self.paused = not self.paused

            # Scroll by pressing up/down
            elif key == curses.KEY_UP:
                # Limit scrolling, so the user do not scroll passed the header
                if self.scroll > 0:
                    self.scroll -= 1
                    self.redraw_screen()
            elif key == curses.KEY_DOWN:
                # Limit scrolling, so the maximum scrolling position is one below the last line
                if self.scroll <= len(self.ids) - self.y + 1:
                    self.scroll += 1
                    self.redraw_screen()

            # Check if screen was resized
            resized = curses.is_term_resized(self.y, self.x)
            if resized is True:
                self.y, self.x = self.stdscr.getmaxyx()
                if hasattr(curses, "resizeterm"):  # pragma: no cover
                    curses.resizeterm(self.y, self.x)
                self.redraw_screen()

        # Shutdown the CAN-Bus interface
        self.bus.shutdown()

    # Unpack the data and then convert it into SI-units
    @staticmethod
    def unpack_data(cmd: int, cmd_to_struct: TDataStructs, data: bytes) -> list[float]:
        if not cmd_to_struct or not data:
            # These messages do not contain a data package
            return []

        for key, value in cmd_to_struct.items():
            if cmd == key if isinstance(key, int) else cmd in key:
                if isinstance(value, tuple):
                    # The struct is given as the fist argument
                    struct_t: struct.Struct = value[0]

                    # The conversion from raw values to SI-units are given in the rest of the tuple
                    values = [
                        d // val if isinstance(val, int) else float(d) / val
                        for d, val in zip(struct_t.unpack(data), value[1:])
                    ]
                else:
                    # No conversion from SI-units is needed
                    as_struct_t: struct.Struct = value
                    values = list(as_struct_t.unpack(data))

                return values

        raise ValueError(f"Unknown command: 0x{cmd:02X}")

    def draw_can_bus_message(self, msg, sorting=False):
        # Use the CAN-Bus ID as the key in the dict
        key = msg.arbitration_id

        # Sort the extended IDs at the bottom by setting the 32-bit high
        if msg.is_extended_id:
            key |= 1 << 32

        new_id_added, length_changed = False, False
        if not sorting:
            # Check if it is a new message or if the length is not the same
            if key not in self.ids:
                new_id_added = True
                # Set the start time when the first message has been received
                if not self.start_time:
                    self.start_time = msg.timestamp
            elif msg.dlc != self.ids[key]["msg"].dlc:
                length_changed = True

                # Clear the old data bytes when the length of the new message is shorter
                if msg.dlc < self.ids[key]["msg"].dlc:
                    self.draw_line(
                        self.ids[key]["row"],
                        # Start drawing at the last byte that is not in the new message
                        52 + msg.dlc * 3,
                        # Draw spaces where the old bytes were drawn
                        " " * ((self.ids[key]["msg"].dlc - msg.dlc) * 3 - 1),
                    )

            if new_id_added or length_changed:
                # Increment the index if it was just added, but keep it if the length just changed
                row = len(self.ids) + 1 if new_id_added else self.ids[key]["row"]

                # It's a new message ID or the length has changed, so add it to the dict
                # The first index is the row index, the second is the frame counter,
                # the third is a copy of the CAN-Bus frame
                # and the forth index is the time since the previous message
                self.ids[key] = {"row": row, "count": 0, "msg": msg, "dt": 0}
            else:
                # Calculate the time since the last message and save the timestamp
                self.ids[key]["dt"] = msg.timestamp - self.ids[key]["msg"].timestamp

                # Copy the CAN-Bus frame - this is used for sorting
                self.ids[key]["msg"] = msg

            # Increment frame counter
            self.ids[key]["count"] += 1

        # Format the CAN-Bus ID as a hex value
        arbitration_id_string = (
            "0x{0:0{1}X}".format(  # pylint: disable=consider-using-f-string
                msg.arbitration_id,
                8 if msg.is_extended_id else 3,
            )
        )

        # Use red for error frames
        if msg.is_error_frame:
            color = curses.color_pair(1)
        else:
            color = curses.color_pair(0)

        # Now draw the CAN-Bus message on the terminal window
        self.draw_line(self.ids[key]["row"], 0, str(self.ids[key]["count"]), color)
        self.draw_line(
            self.ids[key]["row"],
            8,
            f"{self.ids[key]['msg'].timestamp - self.start_time:.6f}",
            color,
        )
        self.draw_line(self.ids[key]["row"], 23, f"{self.ids[key]['dt']:.6f}", color)
        self.draw_line(self.ids[key]["row"], 35, arbitration_id_string, color)
        self.draw_line(self.ids[key]["row"], 47, str(msg.dlc), color)

        try:
            previous_byte_values = self.previous_values[key]
        except KeyError:  # no row of previous values exists for the current message ID
            # initialise a row to store the values for comparison next time
            self.previous_values[key] = {}
            previous_byte_values = self.previous_values[key]
        for i, b in enumerate(msg.data):
            col = 52 + i * 3
            if col > self.x - 2:
                # Data does not fit
                self.draw_line(self.ids[key]["row"], col - 4, "...", color)
                break
            if self.highlight_changed_bytes:
                try:
                    if b != previous_byte_values[i]:
                        # set colour to highlight a changed value
                        data_color = curses.color_pair(2)
                    else:
                        data_color = color
                except KeyError:
                    # previous entry for byte didn't exist - default to rest of line colour
                    data_color = color
                finally:
                    # write the new value to the previous values dict for next time
                    previous_byte_values[i] = b
            else:
                data_color = color
            text = f"{b:02X}"
            self.draw_line(self.ids[key]["row"], col, text, data_color)

        if self.data_structs:
            try:
                values_list = []
                for x in self.unpack_data(
                    msg.arbitration_id, self.data_structs, msg.data
                ):
                    if isinstance(x, float):
                        values_list.append(f"{x:.6f}")
                    else:
                        values_list.append(str(x))
                values_string = " ".join(values_list)
                self.ids[key]["values_string_length"] = len(values_string)
                values_string += " " * (self.x - len(values_string))

                self.draw_line(self.ids[key]["row"], 77, values_string, color)
            except (ValueError, struct.error):
                pass

        return self.ids[key]

    def draw_line(self, row, col, txt, *args):
        if row - self.scroll < 0:
            # Skip if we have scrolled past the line
            return
        try:
            self.stdscr.addstr(row - self.scroll, col, txt, *args)
        except curses.error:
            # Ignore if we are trying to write outside the window
            # This happens if the terminal window is too small
            pass

    def draw_header(self):
        self.stdscr.erase()
        self.draw_line(0, 0, "Count", curses.A_BOLD)
        self.draw_line(0, 8, "Time", curses.A_BOLD)
        self.draw_line(0, 23, "dt", curses.A_BOLD)
        self.draw_line(0, 35, "ID", curses.A_BOLD)
        self.draw_line(0, 47, "DLC", curses.A_BOLD)
        self.draw_line(0, 52, "Data", curses.A_BOLD)

        # Indicate that byte change highlighting is enabled
        if self.highlight_changed_bytes:
            self.draw_line(0, 57, "(changed)", curses.color_pair(2))
        # Only draw if the dictionary is not empty
        if self.data_structs:
            self.draw_line(0, 77, "Parsed values", curses.A_BOLD)

    def redraw_screen(self):
        # Trigger a complete redraw
        self.draw_header()
        for ids in self.ids.values():
            self.draw_can_bus_message(ids["msg"])


class SmartFormatter(argparse.HelpFormatter):
    def _get_default_metavar_for_optional(self, action):
        return action.dest.upper()

    def _format_usage(self, usage, actions, groups, prefix):
        # Use uppercase for "Usage:" text
        return super()._format_usage(usage, actions, groups, "Usage: ")

    def _format_args(self, action, default_metavar):
        if action.nargs not in (argparse.REMAINDER, argparse.ONE_OR_MORE):
            return super()._format_args(action, default_metavar)

        # Use the metavar if "REMAINDER" or "ONE_OR_MORE" is set
        get_metavar = self._metavar_formatter(action, default_metavar)
        return str(get_metavar(1))

    def _format_action_invocation(self, action):
        if not action.option_strings or action.nargs == 0:
            return super()._format_action_invocation(action)

        # Modified so "-s ARGS, --long ARGS" is replaced with "-s, --long ARGS"
        else:
            parts = []
            default = self._get_default_metavar_for_optional(action)
            args_string = self._format_args(action, default)
            for i, option_string in enumerate(action.option_strings):
                if i == len(action.option_strings) - 1:
                    parts.append(f"{option_string} {args_string}")
                else:
                    parts.append(str(option_string))
            return ", ".join(parts)

    def _split_lines(self, text, width):
        # Allow to manually split the lines
        if text.startswith("R|"):
            return text[2:].splitlines()
        return super()._split_lines(text, width)

    def _fill_text(self, text, width, indent):
        if text.startswith("R|"):
            return "".join(indent + line + "\n" for line in text[2:].splitlines())
        else:
            return super()._fill_text(text, width, indent)


def _parse_viewer_args(
    args: list[str],
) -> tuple[argparse.Namespace, TDataStructs]:
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        "python -m can.viewer",
        description="A simple CAN viewer terminal application written in Python",
        epilog="R|Shortcuts: "
        "\n        +---------+-------------------------------+"
        "\n        |   Key   |       Description             |"
        "\n        +---------+-------------------------------+"
        "\n        | ESQ/q   | Exit the viewer               |"
        "\n        | c       | Clear the stored frames       |"
        "\n        | s       | Sort the stored frames        |"
        "\n        | h       | Toggle highlight byte changes |"
        "\n        | SPACE   | Pause the viewer              |"
        "\n        | UP/DOWN | Scroll the viewer             |"
        "\n        +---------+-------------------------------+",
        formatter_class=SmartFormatter,
        add_help=False,
        allow_abbrev=False,
    )

    # add bus options group
    add_bus_arguments(parser, filter_arg=True, group_title="Bus arguments")

    optional = parser.add_argument_group("Optional arguments")

    optional.add_argument(
        "-h", "--help", action="help", help="Show this help message and exit"
    )

    optional.add_argument(
        "--version",
        action="version",
        help="Show program's version number and exit",
        version=f"%(prog)s (version {__version__})",
    )

    optional.add_argument(
        "-d",
        "--decode",
        dest="decode",
        help="R|Specify how to convert the raw bytes into real values."
        "\nThe ID of the frame is given as the first argument and the format as the second."
        "\nThe Python struct package is used to unpack the received data"
        "\nwhere the format characters have the following meaning:"
        "\n      < = little-endian, > = big-endian"
        "\n      x = pad byte"
        "\n      c = char"
        "\n      ? = bool"
        "\n      b = int8_t, B = uint8_t"
        "\n      h = int16, H = uint16"
        "\n      l = int32_t, L = uint32_t"
        "\n      q = int64_t, Q = uint64_t"
        "\n      f = float (32-bits), d = double (64-bits)"
        "\nFx to convert six bytes with ID 0x100 into uint8_t, uint16 and uint32_t:"
        '\n  $ python -m can.viewer -d "100:<BHL"'
        "\nNote that the IDs are always interpreted as hex values."
        "\nAn optional conversion from integers to real units can be given"
        "\nas additional arguments. In order to convert from raw integer"
        "\nvalues the values are divided with the corresponding scaling value,"
        "\nsimilarly the values are multiplied by the scaling value in order"
        "\nto convert from real units to raw integer values."
        "\nFx lets say the uint8_t needs no conversion, but the uint16 and the uint32_t"
        "\nneeds to be divided by 10 and 100 respectively:"
        '\n  $ python -m can.viewer -d "101:<BHL:1:10.0:100.0"'
        "\nBe aware that integer division is performed if the scaling value is an integer."
        "\nMultiple arguments are separated by spaces:"
        '\n  $ python -m can.viewer -d "100:<BHL" "101:<BHL:1:10.0:100.0"'
        "\nAlternatively a file containing the conversion strings separated by new lines"
        "\ncan be given as input:"
        "\n  $ cat file.txt"
        "\n      100:<BHL"
        "\n      101:<BHL:1:10.0:100.0"
        "\n  $ python -m can.viewer -d file.txt",
        metavar="{<id>:<format>,<id>:<format>:<scaling1>:...:<scalingN>,file.txt}",
        nargs=argparse.ONE_OR_MORE,
        default="",
    )

    optional.add_argument(
        "-v",
        action="count",
        dest="verbosity",
        help="""How much information do you want to see at the command line?
                        You can add several of these e.g., -vv is DEBUG""",
        default=2,
    )

    # Print help message when no arguments are given
    if not args:
        parser.print_help(sys.stderr)
        raise SystemExit(errno.EINVAL)

    parsed_args, unknown_args = parser.parse_known_args(args)
    if unknown_args:
        print("Unknown arguments:", unknown_args)

    # Dictionary used to convert between Python values and C structs represented as Python strings.
    # If the value is 'None' then the message does not contain any data package.
    #
    # The struct package is used to unpack the received data.
    # Note the data is assumed to be in little-endian byte order.
    # < = little-endian, > = big-endian
    # x = pad byte
    # c = char
    # ? = bool
    # b = int8_t, B = uint8_t
    # h = int16, H = uint16
    # l = int32_t, L = uint32_t
    # q = int64_t, Q = uint64_t
    # f = float (32-bits), d = double (64-bits)
    #
    # An optional conversion from real units to integers can be given as additional arguments.
    # In order to convert from raw integer value the real units are multiplied with the values and
    # similarly the values
    # are divided by the value in order to convert from real units to raw integer values.

    data_structs: TDataStructs = {}
    if parsed_args.decode:
        if os.path.isfile(parsed_args.decode[0]):
            with open(parsed_args.decode[0], encoding="utf-8") as f:
                structs = f.readlines()
        else:
            structs = parsed_args.decode

        for s in structs:
            tmp = s.rstrip("\n").split(":")

            # The ID is given as a hex value, the format needs no conversion
            key, fmt = int(tmp[0], base=16), tmp[1]

            # The scaling
            scaling: list[float] = []
            for t in tmp[2:]:
                # First try to convert to int, if that fails, then convert to a float
                try:
                    scaling.append(int(t))
                except ValueError:
                    scaling.append(float(t))

            if scaling:
                data_structs[key] = (struct.Struct(fmt), *scaling)
            else:
                data_structs[key] = struct.Struct(fmt)

    return parsed_args, data_structs


def main() -> None:
    parsed_args, data_structs = _parse_viewer_args(sys.argv[1:])
    bus = create_bus_from_namespace(parsed_args)
    _set_logging_level_from_namespace(parsed_args)
    curses.wrapper(CanViewer, bus, data_structs)  # type: ignore[attr-defined,unused-ignore]


if __name__ == "__main__":
    # Catch ctrl+c
    try:
        main()
    except KeyboardInterrupt:
        pass
