"""
Creates a bridge between two CAN buses.

This will connect to two CAN buses. Messages received on one
bus will be sent to the other bus and vice versa.
"""

import argparse
import errno
import sys
import time
from datetime import datetime
from typing import Final

from can.cli import add_bus_arguments, create_bus_from_namespace
from can.listener import RedirectReader
from can.notifier import Notifier

BRIDGE_DESCRIPTION: Final = """\
Bridge two CAN buses.

Both can buses will be connected so that messages from bus1 will be sent on
bus2 and messages from bus2 will be sent to bus1.
"""
BUS_1_PREFIX: Final = "bus1"
BUS_2_PREFIX: Final = "bus2"


def _parse_bridge_args(args: list[str]) -> argparse.Namespace:
    """Parse command line arguments for bridge script."""

    parser = argparse.ArgumentParser(description=BRIDGE_DESCRIPTION)
    add_bus_arguments(parser, prefix=BUS_1_PREFIX, group_title="Bus 1 arguments")
    add_bus_arguments(parser, prefix=BUS_2_PREFIX, group_title="Bus 2 arguments")

    # print help message when no arguments were given
    if not args:
        parser.print_help(sys.stderr)
        raise SystemExit(errno.EINVAL)

    results, _unknown_args = parser.parse_known_args(args)
    return results


def main() -> None:
    results = _parse_bridge_args(sys.argv[1:])

    with (
        create_bus_from_namespace(results, prefix=BUS_1_PREFIX) as bus1,
        create_bus_from_namespace(results, prefix=BUS_2_PREFIX) as bus2,
    ):
        reader1_to_2 = RedirectReader(bus2)
        reader2_to_1 = RedirectReader(bus1)
        with Notifier(bus1, [reader1_to_2]), Notifier(bus2, [reader2_to_1]):
            print(f"CAN Bridge (Started on {datetime.now()})")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass

    print(f"CAN Bridge (Stopped on {datetime.now()})")


if __name__ == "__main__":
    main()
