"""
Utilities and configuration file parsing.
"""

import contextlib
import copy
import functools
import json
import logging
import os
import os.path
import platform
import re
import warnings
from collections.abc import Iterable
from configparser import ConfigParser
from time import get_clock_info, perf_counter, time
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
    cast,
)

from typing_extensions import ParamSpec

import can

from . import typechecking
from .bit_timing import BitTiming, BitTimingFd
from .exceptions import CanInitializationError, CanInterfaceNotImplementedError
from .interfaces import VALID_INTERFACES

log = logging.getLogger("can.util")

# List of valid data lengths for a CAN FD message
CAN_FD_DLC = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64]

REQUIRED_KEYS = ["interface", "channel"]


CONFIG_FILES = ["~/can.conf"]

if platform.system() in ("Linux", "Darwin"):
    CONFIG_FILES.extend(["/etc/can.conf", "~/.can", "~/.canrc"])
elif platform.system() == "Windows" or platform.python_implementation() == "IronPython":
    CONFIG_FILES.extend(["can.ini", os.path.join(os.getenv("APPDATA", ""), "can.ini")])


def load_file_config(
    path: Optional[typechecking.StringPathLike] = None, section: str = "default"
) -> dict[str, str]:
    """
    Loads configuration from file with following content::

        [default]
        interface = socketcan
        channel = can0

    :param path:
        path to config file. If not specified, several sensible
        default locations are tried depending on platform.
    :param section:
        name of the section to read configuration from.
    """
    config = ConfigParser()

    # make sure to not transform the entries such that capitalization is preserved
    config.optionxform = lambda optionstr: optionstr  # type: ignore[method-assign]

    if path is None:
        config.read([os.path.expanduser(path) for path in CONFIG_FILES])
    else:
        config.read(path)

    _config: dict[str, str] = {}

    if config.has_section(section):
        _config.update(config.items(section))

    return _config


def load_environment_config(context: Optional[str] = None) -> dict[str, str]:
    """
    Loads config dict from environmental variables (if set):

    * CAN_INTERFACE
    * CAN_CHANNEL
    * CAN_BITRATE
    * CAN_CONFIG

    if context is supplied, "_{context}" is appended to the environment
    variable name we will look at. For example if context="ABC":

    * CAN_INTERFACE_ABC
    * CAN_CHANNEL_ABC
    * CAN_BITRATE_ABC
    * CAN_CONFIG_ABC

    """
    mapper = {
        "interface": "CAN_INTERFACE",
        "channel": "CAN_CHANNEL",
        "bitrate": "CAN_BITRATE",
    }

    context_suffix = f"_{context}" if context else ""
    can_config_key = f"CAN_CONFIG{context_suffix}"
    config: dict[str, str] = json.loads(os.environ.get(can_config_key, "{}"))

    for key, val in mapper.items():
        config_option = os.environ.get(val + context_suffix, None)
        if config_option:
            config[key] = config_option

    return config


def load_config(
    path: Optional[typechecking.StringPathLike] = None,
    config: Optional[dict[str, Any]] = None,
    context: Optional[str] = None,
) -> typechecking.BusConfig:
    """
    Returns a dict with configuration details which is loaded from (in this order):

    - config
    - can.rc
    - Environment variables CAN_INTERFACE, CAN_CHANNEL, CAN_BITRATE
    - Config files ``/etc/can.conf`` or ``~/.can`` or ``~/.canrc``
      where the latter may add or replace values of the former.

    Interface can be any of the strings from ``can.VALID_INTERFACES`` for example:
    kvaser, socketcan, pcan, usb2can, ixxat, nican, virtual.

    .. note::

            The key ``bustype`` is copied to ``interface`` if that one is missing
            and does never appear in the result.

    :param path:
        Optional path to config file.

    :param config:
        A dict which may set the 'interface', and/or the 'channel', or neither.
        It may set other values that are passed through.

    :param context:
        Extra 'context' pass to config sources. This can be used to section
        other than 'default' in the configuration file.

    :return:
        A config dictionary that should contain 'interface' & 'channel'::

            {
                'interface': 'python-can backend interface to use',
                'channel': 'default channel to use',
                # possibly more
            }

        Note ``None`` will be used if all the options are exhausted without
        finding a value.

        All unused values are passed from ``config`` over to this.

    :raises:
        CanInterfaceNotImplementedError if the ``interface`` name isn't recognized
    """

    # Start with an empty dict to apply filtering to all sources
    given_config = config or {}
    config = {}

    # Use the given dict for default values
    config_sources = cast(
        "Iterable[Union[dict[str, Any], Callable[[Any], dict[str, Any]]]]",
        [
            given_config,
            can.rc,
            lambda _context: load_environment_config(  # pylint: disable=unnecessary-lambda
                _context
            ),
            lambda _context: load_environment_config(),
            lambda _context: load_file_config(path, _context),
            lambda _context: load_file_config(path),
        ],
    )

    # Slightly complex here to only search for the file config if required
    for _cfg in config_sources:
        cfg = _cfg(context) if callable(_cfg) else _cfg
        # remove legacy operator (and copy to interface if not already present)
        if "bustype" in cfg:
            if "interface" not in cfg or not cfg["interface"]:
                cfg["interface"] = cfg["bustype"]
            del cfg["bustype"]
        # copy all new parameters
        for key, val in cfg.items():
            if key not in config:
                if isinstance(val, str):
                    config[key] = cast_from_string(val)
                else:
                    config[key] = cfg[key]

    bus_config = _create_bus_config(config)
    can.log.debug("can config: %s", bus_config)
    return bus_config


def _create_bus_config(config: dict[str, Any]) -> typechecking.BusConfig:
    """Validates some config values, performs compatibility mappings and creates specific
    structures (e.g. for bit timings).

    :param config: The raw config as specified by the user
    :return: A config that can be used by a :class:`~can.BusABC`
    :raises NotImplementedError: if the ``interface`` is unknown
    """
    # substitute None for all values not found
    for key in REQUIRED_KEYS:
        if key not in config:
            config[key] = None

    if config["interface"] not in VALID_INTERFACES:
        raise CanInterfaceNotImplementedError(
            f'Unknown interface type "{config["interface"]}"'
        )
    if "port" in config:
        # convert port to integer if necessary
        if isinstance(config["port"], int):
            port = config["port"]
        elif isinstance(config["port"], str):
            if config["port"].isnumeric():
                config["port"] = port = int(config["port"])
            else:
                raise ValueError("Port config must be a number!")
        else:
            raise TypeError("Port config must be string or integer!")

        if not 0 < port < 65535:
            raise ValueError("Port config must be inside 0-65535 range!")

    if "timing" not in config:
        if timing := _dict2timing(config):
            config["timing"] = timing

    if "fd" in config:
        config["fd"] = config["fd"] not in (0, False)

    if "state" in config and not isinstance(config["state"], can.BusState):
        try:
            config["state"] = can.BusState[config["state"]]
        except KeyError as e:
            raise ValueError("State config not valid!") from e

    return cast("typechecking.BusConfig", config)


def _dict2timing(data: dict[str, Any]) -> Union[BitTiming, BitTimingFd, None]:
    """Try to instantiate a :class:`~can.BitTiming` or :class:`~can.BitTimingFd` from
    a dictionary. Return `None` if not possible."""

    with contextlib.suppress(ValueError, TypeError):
        if set(typechecking.BitTimingFdDict.__annotations__).issubset(data):
            return BitTimingFd(
                **{
                    key: int(data[key])
                    for key in typechecking.BitTimingFdDict.__annotations__
                },
                strict=False,
            )
        elif set(typechecking.BitTimingDict.__annotations__).issubset(data):
            return BitTiming(
                **{
                    key: int(data[key])
                    for key in typechecking.BitTimingDict.__annotations__
                },
                strict=False,
            )

    return None


def set_logging_level(level_name: str) -> None:
    """Set the logging level for the `"can"` logger.

    :param level_name:
        One of: `'critical'`, `'error'`, `'warning'`, `'info'`,
        `'debug'`, `'subdebug'`, or the value :obj:`None` (=default).
        Defaults to `'debug'`.
    """
    can_logger = logging.getLogger("can")

    try:
        can_logger.setLevel(getattr(logging, level_name.upper()))
    except AttributeError:
        can_logger.setLevel(logging.DEBUG)
    log.debug("Logging set to %s", level_name)


def len2dlc(length: int) -> int:
    """Calculate the DLC from data length.

    :param length: Length in number of bytes (0-64)

    :returns: DLC (0-15)
    """
    if length <= 8:
        return length
    for dlc, nof_bytes in enumerate(CAN_FD_DLC):
        if nof_bytes >= length:
            return dlc
    return 15


def dlc2len(dlc: int) -> int:
    """Calculate the data length from DLC.

    :param dlc: DLC (0-15)

    :returns: Data length in number of bytes (0-64)
    """
    return CAN_FD_DLC[dlc] if dlc <= 15 else 64


def channel2int(channel: Optional[typechecking.Channel]) -> Optional[int]:
    """Try to convert the channel to an integer.

    :param channel:
        Channel string (e.g. `"can0"`, `"CAN1"`) or an integer

    :returns: Channel integer or ``None`` if unsuccessful
    """
    if isinstance(channel, int):
        return channel
    if isinstance(channel, str):
        match = re.match(r".*?(\d+)$", channel)
        if match:
            return int(match.group(1))
    return None


P1 = ParamSpec("P1")
T1 = TypeVar("T1")


def deprecated_args_alias(
    deprecation_start: str,
    deprecation_end: Optional[str] = None,
    **aliases: Optional[str],
) -> Callable[[Callable[P1, T1]], Callable[P1, T1]]:
    """Allows to rename/deprecate a function kwarg(s) and optionally
    have the deprecated kwarg(s) set as alias(es)

    Example::

        @deprecated_args_alias("1.2.0", oldArg="new_arg", anotherOldArg="another_new_arg")
        def library_function(new_arg, another_new_arg):
            pass

        @deprecated_args_alias(
            deprecation_start="1.2.0",
            deprecation_end="3.0.0",
            oldArg="new_arg",
            obsoleteOldArg=None,
        )
        def library_function(new_arg):
            pass

    :param deprecation_start:
        The *python-can* version, that introduced the :class:`DeprecationWarning`.
    :param deprecation_end:
        The *python-can* version, that marks the end of the deprecation period.
    :param aliases:
        keyword arguments, that map the deprecated argument names
        to the new argument names or ``None``.

    """

    def deco(f: Callable[P1, T1]) -> Callable[P1, T1]:
        @functools.wraps(f)
        def wrapper(*args: P1.args, **kwargs: P1.kwargs) -> T1:
            _rename_kwargs(
                func_name=f.__name__,
                start=deprecation_start,
                end=deprecation_end,
                kwargs=kwargs,
                aliases=aliases,
            )
            return f(*args, **kwargs)

        return wrapper

    return deco


def _rename_kwargs(
    func_name: str,
    start: str,
    end: Optional[str],
    kwargs: dict[str, Any],
    aliases: dict[str, Optional[str]],
) -> None:
    """Helper function for `deprecated_args_alias`"""
    for alias, new in aliases.items():
        if alias in kwargs:
            deprecation_notice = (
                f"The '{alias}' argument is deprecated since python-can v{start}"
            )
            if end:
                deprecation_notice += (
                    f", and scheduled for removal in python-can v{end}"
                )
            deprecation_notice += "."

            value = kwargs.pop(alias)
            if new is not None:
                deprecation_notice += f" Use '{new}' instead."

                if new in kwargs:
                    raise TypeError(
                        f"{func_name} received both '{alias}' (deprecated) and '{new}'."
                    )
                kwargs[new] = value

            warnings.warn(deprecation_notice, DeprecationWarning, stacklevel=3)


T2 = TypeVar("T2", BitTiming, BitTimingFd)


def check_or_adjust_timing_clock(timing: T2, valid_clocks: Iterable[int]) -> T2:
    """Adjusts the given timing instance to have an *f_clock* value that is within the
    allowed values specified by *valid_clocks*. If the *f_clock* value of timing is
    already within *valid_clocks*, then *timing* is returned unchanged.

    :param timing:
        The :class:`~can.BitTiming` or :class:`~can.BitTimingFd` instance to adjust.
    :param valid_clocks:
        An iterable of integers representing the valid *f_clock* values that the timing instance
        can be changed to. The order of the values in *valid_clocks* determines the priority in
        which they are tried, with earlier values being tried before later ones.
    :return:
        A new :class:`~can.BitTiming` or :class:`~can.BitTimingFd` instance with an
        *f_clock* value within *valid_clocks*.
    :raises ~can.exceptions.CanInitializationError:
        If no compatible *f_clock* value can be found within *valid_clocks*.
    """
    if timing.f_clock in valid_clocks:
        # create a copy so this function always returns a new instance
        return copy.deepcopy(timing)

    for clock in valid_clocks:
        try:
            # Try to use a different f_clock
            adjusted_timing = timing.recreate_with_f_clock(clock)
            warnings.warn(
                f"Adjusted f_clock in {timing.__class__.__name__} from "
                f"{timing.f_clock} to {adjusted_timing.f_clock}",
                stacklevel=2,
            )
            return adjusted_timing
        except ValueError:
            pass

    raise CanInitializationError(
        f"The specified timing.f_clock value {timing.f_clock} "
        f"doesn't match any of the allowed device f_clock values: "
        f"{', '.join([str(f) for f in valid_clocks])}"
    ) from None


def time_perfcounter_correlation() -> tuple[float, float]:
    """Get the `perf_counter` value nearest to when time.time() is updated

    Computed if the default timer used by `time.time` on this platform has a resolution
    higher than 10μs, otherwise the current time and perf_counter is directly returned.
    This was chosen as typical timer resolution on Linux/macOS is ~1μs, and the Windows
    platform can vary from ~500μs to 10ms.

    Note this value is based on when `time.time()` is observed to update from Python,
    it is not directly returned by the operating system.

    :returns:
        (t, performance_counter) time.time value and time.perf_counter value when the time.time
        is updated

    """

    # use this if the resolution is higher than 10us
    if get_clock_info("time").resolution > 1e-5:
        t0 = time()
        while True:
            t1, performance_counter = time(), perf_counter()
            if t1 != t0:
                break
    else:
        return time(), perf_counter()
    return t1, performance_counter


def cast_from_string(string_val: str) -> Union[str, int, float, bool]:
    """Perform trivial type conversion from :class:`str` values.

    :param string_val:
        the string, that shall be converted
    """
    if re.match(r"^[-+]?\d+$", string_val):
        # value is integer
        return int(string_val)

    if re.match(r"^[-+]?\d*\.\d+(?:e[-+]?\d+)?$", string_val):
        # value is float
        return float(string_val)

    if re.match(r"^(?:True|False)$", string_val, re.IGNORECASE):
        # value is bool
        return string_val.lower() == "true"

    # value is string
    return string_val
