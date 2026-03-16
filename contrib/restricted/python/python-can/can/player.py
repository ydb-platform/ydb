"""
Replays CAN traffic saved with can.logger back
to a CAN bus.

Similar to canplayer in the can-utils package.
"""

import argparse
import errno
import sys
from datetime import datetime
from typing import TYPE_CHECKING, cast

from can import LogReader, MessageSync
from can.cli import (
    _add_extra_args,
    _parse_additional_config,
    _set_logging_level_from_namespace,
    add_bus_arguments,
    create_bus_from_namespace,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from can import Message


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay CAN traffic.")

    player_group = parser.add_argument_group("Player arguments")

    player_group.add_argument(
        "-f",
        "--file_name",
        dest="log_file",
        help="Path and base log filename, for supported types see can.LogReader.",
        default=None,
    )

    player_group.add_argument(
        "-v",
        action="count",
        dest="verbosity",
        help="""Also print can frames to stdout.
                        You can add several of these to enable debugging""",
        default=2,
    )

    player_group.add_argument(
        "--ignore-timestamps",
        dest="timestamps",
        help="""Ignore timestamps (send all frames immediately with minimum gap between frames)""",
        action="store_false",
    )

    player_group.add_argument(
        "--error-frames",
        help="Also send error frames to the interface.",
        action="store_true",
    )

    player_group.add_argument(
        "-g",
        "--gap",
        type=float,
        help="<s> minimum time between replayed frames",
        default=0.0001,
    )
    player_group.add_argument(
        "-s",
        "--skip",
        type=float,
        default=60 * 60 * 24,
        help="<s> skip gaps greater than 's' seconds",
    )

    player_group.add_argument(
        "infile",
        metavar="input-file",
        type=str,
        help="The file to replay. For supported types see can.LogReader.",
    )

    # handle remaining arguments
    _add_extra_args(player_group)

    # add bus options
    add_bus_arguments(parser)

    # print help message when no arguments were given
    if len(sys.argv) < 2:
        parser.print_help(sys.stderr)
        raise SystemExit(errno.EINVAL)

    results, unknown_args = parser.parse_known_args()
    additional_config = _parse_additional_config([*results.extra_args, *unknown_args])

    _set_logging_level_from_namespace(results)
    verbosity = results.verbosity

    error_frames = results.error_frames

    with create_bus_from_namespace(results) as bus:
        with LogReader(results.infile, **additional_config) as reader:
            in_sync = MessageSync(
                cast("Iterable[Message]", reader),
                timestamps=results.timestamps,
                gap=results.gap,
                skip=results.skip,
            )

            print(f"Can LogReader (Started on {datetime.now()})")

            try:
                for message in in_sync:
                    if message.is_error_frame and not error_frames:
                        continue
                    if verbosity >= 3:
                        print(message)
                    bus.send(message)
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
