import argparse
import re
from collections.abc import Sequence
from typing import Any, Optional, Union

import can
from can.typechecking import CanFilter, TAdditionalCliArgs
from can.util import _dict2timing, cast_from_string


def add_bus_arguments(
    parser: argparse.ArgumentParser,
    *,
    filter_arg: bool = False,
    prefix: Optional[str] = None,
    group_title: Optional[str] = None,
) -> None:
    """Adds CAN bus configuration options to an argument parser.

    :param parser:
        The argument parser to which the options will be added.
    :param filter_arg:
        Whether to include the filter argument.
    :param prefix:
        An optional prefix for the argument names, allowing configuration of multiple buses.
    :param group_title:
        The title of the argument group. If not provided, a default title will be generated
        based on the prefix. For example, "bus arguments (prefix)" if a prefix is specified,
        or "bus arguments" otherwise.
    """
    if group_title is None:
        group_title = f"bus arguments ({prefix})" if prefix else "bus arguments"

    group = parser.add_argument_group(group_title)

    flags = [f"--{prefix}-channel"] if prefix else ["-c", "--channel"]
    dest = f"{prefix}_channel" if prefix else "channel"
    group.add_argument(
        *flags,
        dest=dest,
        default=argparse.SUPPRESS,
        metavar="CHANNEL",
        help=r"Most backend interfaces require some sort of channel. For "
        r"example with the serial interface the channel might be a rfcomm"
        r' device: "/dev/rfcomm0". With the socketcan interface valid '
        r'channel examples include: "can0", "vcan0".',
    )

    flags = [f"--{prefix}-interface"] if prefix else ["-i", "--interface"]
    dest = f"{prefix}_interface" if prefix else "interface"
    group.add_argument(
        *flags,
        dest=dest,
        default=argparse.SUPPRESS,
        choices=sorted(can.VALID_INTERFACES),
        help="""Specify the backend CAN interface to use. If left blank,
                        fall back to reading from configuration files.""",
    )

    flags = [f"--{prefix}-bitrate"] if prefix else ["-b", "--bitrate"]
    dest = f"{prefix}_bitrate" if prefix else "bitrate"
    group.add_argument(
        *flags,
        dest=dest,
        type=int,
        default=argparse.SUPPRESS,
        metavar="BITRATE",
        help="Bitrate to use for the CAN bus.",
    )

    flags = [f"--{prefix}-fd"] if prefix else ["--fd"]
    dest = f"{prefix}_fd" if prefix else "fd"
    group.add_argument(
        *flags,
        dest=dest,
        default=argparse.SUPPRESS,
        action="store_true",
        help="Activate CAN-FD support",
    )

    flags = [f"--{prefix}-data-bitrate"] if prefix else ["--data-bitrate"]
    dest = f"{prefix}_data_bitrate" if prefix else "data_bitrate"
    group.add_argument(
        *flags,
        dest=dest,
        type=int,
        default=argparse.SUPPRESS,
        metavar="DATA_BITRATE",
        help="Bitrate to use for the data phase in case of CAN-FD.",
    )

    flags = [f"--{prefix}-timing"] if prefix else ["--timing"]
    dest = f"{prefix}_timing" if prefix else "timing"
    group.add_argument(
        *flags,
        dest=dest,
        action=_BitTimingAction,
        nargs=argparse.ONE_OR_MORE,
        default=argparse.SUPPRESS,
        metavar="TIMING_ARG",
        help="Configure bit rate and bit timing. For example, use "
        "`--timing f_clock=8_000_000 tseg1=5 tseg2=2 sjw=2 brp=2 nof_samples=1` for classical CAN "
        "or `--timing f_clock=80_000_000 nom_tseg1=119 nom_tseg2=40 nom_sjw=40 nom_brp=1 "
        "data_tseg1=29 data_tseg2=10 data_sjw=10 data_brp=1` for CAN FD. "
        "Check the python-can documentation to verify whether your "
        "CAN interface supports the `timing` argument.",
    )

    if filter_arg:
        flags = [f"--{prefix}-filter"] if prefix else ["--filter"]
        dest = f"{prefix}_can_filters" if prefix else "can_filters"
        group.add_argument(
            *flags,
            dest=dest,
            nargs=argparse.ONE_OR_MORE,
            action=_CanFilterAction,
            default=argparse.SUPPRESS,
            metavar="{<can_id>:<can_mask>,<can_id>~<can_mask>}",
            help="R|Space separated CAN filters for the given CAN interface:"
            "\n      <can_id>:<can_mask> (matches when <received_can_id> & mask =="
            " can_id & mask)"
            "\n      <can_id>~<can_mask> (matches when <received_can_id> & mask !="
            " can_id & mask)"
            "\nFx to show only frames with ID 0x100 to 0x103 and 0x200 to 0x20F:"
            "\n      python -m can.viewer --filter 100:7FC 200:7F0"
            "\nNote that the ID and mask are always interpreted as hex values",
        )

    flags = [f"--{prefix}-bus-kwargs"] if prefix else ["--bus-kwargs"]
    dest = f"{prefix}_bus_kwargs" if prefix else "bus_kwargs"
    group.add_argument(
        *flags,
        dest=dest,
        action=_BusKwargsAction,
        nargs=argparse.ONE_OR_MORE,
        default=argparse.SUPPRESS,
        metavar="BUS_KWARG",
        help="Pass keyword arguments down to the instantiation of the bus class. "
        "For example, `-i vector -c 1 --bus-kwargs app_name=MyCanApp serial=1234` is equivalent "
        "to opening the bus with `can.Bus('vector', channel=1, app_name='MyCanApp', serial=1234)",
    )


def create_bus_from_namespace(
    namespace: argparse.Namespace,
    *,
    prefix: Optional[str] = None,
    **kwargs: Any,
) -> can.BusABC:
    """Creates and returns a CAN bus instance based on the provided namespace and arguments.

    :param namespace:
        The namespace containing parsed arguments.
    :param prefix:
        An optional prefix for the argument names, enabling support for multiple buses.
    :param kwargs:
        Additional keyword arguments to configure the bus.
    :return:
        A CAN bus instance.
    """
    config: dict[str, Any] = {"single_handle": True, **kwargs}

    for keyword in (
        "channel",
        "interface",
        "bitrate",
        "fd",
        "data_bitrate",
        "can_filters",
        "timing",
        "bus_kwargs",
    ):
        prefixed_keyword = f"{prefix}_{keyword}" if prefix else keyword

        if prefixed_keyword in namespace:
            value = getattr(namespace, prefixed_keyword)

            if keyword == "bus_kwargs":
                config.update(value)
            else:
                config[keyword] = value

    try:
        return can.Bus(**config)
    except Exception as exc:
        err_msg = f"Unable to instantiate bus from arguments {vars(namespace)}."
        raise argparse.ArgumentError(None, err_msg) from exc


class _CanFilterAction(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Union[str, Sequence[Any], None],
        option_string: Optional[str] = None,
    ) -> None:
        if not isinstance(values, list):
            raise argparse.ArgumentError(self, "Invalid filter argument")

        print(f"Adding filter(s): {values}")
        can_filters: list[CanFilter] = []

        for filt in values:
            if ":" in filt:
                parts = filt.split(":")
                can_id = int(parts[0], base=16)
                can_mask = int(parts[1], base=16)
            elif "~" in filt:
                parts = filt.split("~")
                can_id = int(parts[0], base=16) | 0x20000000  # CAN_INV_FILTER
                can_mask = int(parts[1], base=16) & 0x20000000  # socket.CAN_ERR_FLAG
            else:
                raise argparse.ArgumentError(self, "Invalid filter argument")
            can_filters.append({"can_id": can_id, "can_mask": can_mask})

        setattr(namespace, self.dest, can_filters)


class _BitTimingAction(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Union[str, Sequence[Any], None],
        option_string: Optional[str] = None,
    ) -> None:
        if not isinstance(values, list):
            raise argparse.ArgumentError(self, "Invalid --timing argument")

        timing_dict: dict[str, int] = {}
        for arg in values:
            try:
                key, value_string = arg.split("=")
                value = int(value_string)
                timing_dict[key] = value
            except ValueError:
                raise argparse.ArgumentError(
                    self, f"Invalid timing argument: {arg}"
                ) from None

        if not (timing := _dict2timing(timing_dict)):
            err_msg = "Invalid --timing argument. Incomplete parameters."
            raise argparse.ArgumentError(self, err_msg)

        setattr(namespace, self.dest, timing)
        print(timing)


class _BusKwargsAction(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Union[str, Sequence[Any], None],
        option_string: Optional[str] = None,
    ) -> None:
        if not isinstance(values, list):
            raise argparse.ArgumentError(self, "Invalid --bus-kwargs argument")

        bus_kwargs: dict[str, Union[str, int, float, bool]] = {}

        for arg in values:
            try:
                match = re.match(
                    r"^(?P<name>[_a-zA-Z][_a-zA-Z0-9]*)=(?P<value>\S*?)$",
                    arg,
                )
                if not match:
                    raise ValueError
                key = match["name"].replace("-", "_")
                string_val = match["value"]
                bus_kwargs[key] = cast_from_string(string_val)
            except ValueError:
                raise argparse.ArgumentError(
                    self,
                    f"Unable to parse bus keyword argument '{arg}'",
                ) from None

        setattr(namespace, self.dest, bus_kwargs)


def _add_extra_args(
    parser: Union[argparse.ArgumentParser, argparse._ArgumentGroup],
) -> None:
    parser.add_argument(
        "extra_args",
        nargs=argparse.REMAINDER,
        help="The remaining arguments will be used for logger/player initialisation. "
        "For example, `can_logger -i virtual -c test -f logfile.blf --compression-level=9` "
        "passes the keyword argument `compression_level=9` to the BlfWriter.",
    )


def _parse_additional_config(unknown_args: Sequence[str]) -> TAdditionalCliArgs:
    for arg in unknown_args:
        if not re.match(r"^--[a-zA-Z][a-zA-Z0-9\-]*=\S*?$", arg):
            raise ValueError(f"Parsing argument {arg} failed")

    def _split_arg(_arg: str) -> tuple[str, str]:
        left, right = _arg.split("=", 1)
        return left.lstrip("-").replace("-", "_"), right

    args: dict[str, Union[str, int, float, bool]] = {}
    for key, string_val in map(_split_arg, unknown_args):
        args[key] = cast_from_string(string_val)
    return args


def _set_logging_level_from_namespace(namespace: argparse.Namespace) -> None:
    if "verbosity" in namespace:
        logging_level_names = [
            "critical",
            "error",
            "warning",
            "info",
            "debug",
            "subdebug",
        ]
        can.set_logging_level(logging_level_names[min(5, namespace.verbosity)])
