import argparse
import errno
import sys
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Union,
)

from can import BusState, Logger, SizedRotatingLogger
from can.cli import (
    _add_extra_args,
    _parse_additional_config,
    _set_logging_level_from_namespace,
    add_bus_arguments,
    create_bus_from_namespace,
)
from can.typechecking import TAdditionalCliArgs

if TYPE_CHECKING:
    from can.io import BaseRotatingLogger
    from can.io.generic import MessageWriter


def _parse_logger_args(
    args: list[str],
) -> tuple[argparse.Namespace, TAdditionalCliArgs]:
    """Parse command line arguments for logger script."""

    parser = argparse.ArgumentParser(
        description="Log CAN traffic, printing messages to stdout or to a "
        "given file.",
    )

    logger_group = parser.add_argument_group("logger arguments")

    logger_group.add_argument(
        "-f",
        "--file_name",
        dest="log_file",
        help="Path and base log filename, for supported types see can.Logger.",
        default=None,
    )

    logger_group.add_argument(
        "-a",
        "--append",
        dest="append",
        help="Append to the log file if it already exists.",
        action="store_true",
    )

    logger_group.add_argument(
        "-s",
        "--file_size",
        dest="file_size",
        type=int,
        help="Maximum file size in bytes. Rotate log file when size threshold "
        "is reached. (The resulting file sizes will be consistent, but are not "
        "guaranteed to be exactly what is specified here due to the rollover "
        "conditions being logger implementation specific.)",
        default=None,
    )

    logger_group.add_argument(
        "-v",
        action="count",
        dest="verbosity",
        help="""How much information do you want to see at the command line?
                        You can add several of these e.g., -vv is DEBUG""",
        default=2,
    )

    state_group = logger_group.add_mutually_exclusive_group(required=False)
    state_group.add_argument(
        "--active",
        help="Start the bus as active, this is applied by default.",
        action="store_true",
    )
    state_group.add_argument(
        "--passive", help="Start the bus as passive.", action="store_true"
    )

    # handle remaining arguments
    _add_extra_args(logger_group)

    # add bus options
    add_bus_arguments(parser, filter_arg=True)

    # print help message when no arguments were given
    if not args:
        parser.print_help(sys.stderr)
        raise SystemExit(errno.EINVAL)

    results, unknown_args = parser.parse_known_args(args)
    additional_config = _parse_additional_config([*results.extra_args, *unknown_args])
    return results, additional_config


def main() -> None:
    results, additional_config = _parse_logger_args(sys.argv[1:])
    bus = create_bus_from_namespace(results)
    _set_logging_level_from_namespace(results)

    if results.active:
        bus.state = BusState.ACTIVE
    elif results.passive:
        bus.state = BusState.PASSIVE

    print(f"Connected to {bus.__class__.__name__}: {bus.channel_info}")
    print(f"Can Logger (Started on {datetime.now()})")

    logger: Union[MessageWriter, BaseRotatingLogger]
    if results.file_size:
        logger = SizedRotatingLogger(
            base_filename=results.log_file,
            max_bytes=results.file_size,
            append=results.append,
            **additional_config,
        )
    else:
        logger = Logger(
            filename=results.log_file,
            append=results.append,
            **additional_config,
        )

    try:
        while True:
            msg = bus.recv(1)
            if msg is not None:
                logger(msg)
    except KeyboardInterrupt:
        pass
    finally:
        bus.shutdown()
        logger.stop()


if __name__ == "__main__":
    main()
