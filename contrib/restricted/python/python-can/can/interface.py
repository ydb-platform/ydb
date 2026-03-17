"""
This module contains the base implementation of :class:`can.BusABC` as well
as a list of all available backends and some implemented
CyclicSendTasks.
"""

import concurrent.futures.thread
import importlib
import logging
from collections.abc import Callable, Iterable, Sequence
from typing import Any, Optional, Union, cast

from . import util
from .bus import BusABC
from .exceptions import CanInterfaceNotImplementedError
from .interfaces import BACKENDS
from .typechecking import AutoDetectedConfig, Channel

log = logging.getLogger("can.interface")
log_autodetect = log.getChild("detect_available_configs")


def _get_class_for_interface(interface: str) -> type[BusABC]:
    """
    Returns the main bus class for the given interface.

    :raises:
        NotImplementedError if the interface is not known
    :raises CanInterfaceNotImplementedError:
         if there was a problem while importing the interface or the bus class within that
    """
    # Find the correct backend
    try:
        module_name, class_name = BACKENDS[interface]
    except KeyError:
        raise NotImplementedError(
            f"CAN interface '{interface}' not supported"
        ) from None

    # Import the correct interface module
    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        raise CanInterfaceNotImplementedError(
            f"Cannot import module {module_name} for CAN interface '{interface}': {e}"
        ) from None

    # Get the correct class
    try:
        bus_class = getattr(module, class_name)
    except Exception as e:
        raise CanInterfaceNotImplementedError(
            f"Cannot import class {class_name} from module {module_name} for CAN interface "
            f"'{interface}': {e}"
        ) from None

    return cast("type[BusABC]", bus_class)


@util.deprecated_args_alias(
    deprecation_start="4.2.0",
    deprecation_end="5.0.0",
    bustype="interface",
    context="config_context",
)
def Bus(  # noqa: N802
    channel: Optional[Channel] = None,
    interface: Optional[str] = None,
    config_context: Optional[str] = None,
    ignore_config: bool = False,
    **kwargs: Any,
) -> BusABC:
    """Create a new bus instance with configuration loading.

    Instantiates a CAN Bus of the given ``interface``, falls back to reading a
    configuration file from default locations.

    .. note::
        Please note that while the arguments provided to this class take precedence
        over any existing values from configuration, it is possible that other parameters
        from the configuration may be added to the bus instantiation.
        This could potentially have unintended consequences. To prevent this,
        you may use the *ignore_config* parameter to ignore any existing configurations.

    :param channel:
        Channel identification. Expected type is backend dependent.
        Set to ``None`` to let it be resolved automatically from the default
        :ref:`configuration`.

    :param interface:
        See :ref:`interface names` for a list of supported interfaces.
        Set to ``None`` to let it be resolved automatically from the default
        :ref:`configuration`.

    :param config_context:
        Extra 'context', that is passed to config sources.
        This can be used to select a section other than 'default' in the configuration file.

    :param ignore_config:
        If ``True``, only the given arguments will be used for the bus instantiation. Existing
        configuration sources will be ignored.

    :param kwargs:
        ``interface`` specific keyword arguments.

    :raises ~can.exceptions.CanInterfaceNotImplementedError:
        if the ``interface`` isn't recognized or cannot be loaded

    :raises ~can.exceptions.CanInitializationError:
        if the bus cannot be instantiated

    :raises ValueError:
        if the ``channel`` could not be determined
    """

    # figure out the rest of the configuration; this might raise an error
    if interface is not None:
        kwargs["interface"] = interface
    if channel is not None:
        kwargs["channel"] = channel

    if not ignore_config:
        kwargs = util.load_config(config=kwargs, context=config_context)

    # resolve the bus class to use for that interface
    cls = _get_class_for_interface(kwargs["interface"])

    # remove the "interface" key, so it doesn't get passed to the backend
    del kwargs["interface"]

    # make sure the bus can handle this config format
    channel = kwargs.pop("channel", channel)
    if channel is None:
        # Use the default channel for the backend
        bus = cls(**kwargs)
    else:
        bus = cls(channel, **kwargs)

    return bus


def detect_available_configs(
    interfaces: Union[None, str, Iterable[str]] = None,
    timeout: float = 5.0,
) -> Sequence[AutoDetectedConfig]:
    """Detect all configurations/channels that the interfaces could
    currently connect with.

    This might be quite time-consuming.

    Automated configuration detection may not be implemented by
    every interface on every platform. This method will not raise
    an error in that case, but will rather return an empty list
    for that interface.

    :param interfaces: either
        - the name of an interface to be searched in as a string,
        - an iterable of interface names to search in, or
        - `None` to search in all known interfaces.
    :param timeout: maximum number of seconds to wait for all interface
        detection tasks to complete. If exceeded, any pending tasks
        will be cancelled, a warning will be logged, and the method
        will return results gathered so far.
    :rtype: list[dict]
    :return: an iterable of dicts, each suitable for usage in
             the constructor of :class:`can.BusABC`. Interfaces that
             timed out will be logged as warnings and excluded.
    """

    # Determine which interfaces to search
    if interfaces is None:
        interfaces = BACKENDS
    elif isinstance(interfaces, str):
        interfaces = (interfaces,)
    # otherwise assume iterable of strings

    # Collect detection callbacks
    callbacks: dict[str, Callable[[], Sequence[AutoDetectedConfig]]] = {}
    for interface_keyword in interfaces:
        try:
            bus_class = _get_class_for_interface(interface_keyword)
            callbacks[interface_keyword] = (
                bus_class._detect_available_configs  # pylint: disable=protected-access
            )
        except CanInterfaceNotImplementedError:
            log_autodetect.debug(
                'interface "%s" cannot be loaded for detection of available configurations',
                interface_keyword,
            )

    result: list[AutoDetectedConfig] = []

    # Use manual executor to allow shutdown without waiting
    executor = concurrent.futures.ThreadPoolExecutor()
    try:
        futures_to_keyword = {
            executor.submit(func): kw for kw, func in callbacks.items()
        }
        done, not_done = concurrent.futures.wait(
            futures_to_keyword,
            timeout=timeout,
            return_when=concurrent.futures.ALL_COMPLETED,
        )
        # Log timed-out tasks
        if not_done:
            log_autodetect.warning(
                "Timeout (%.2fs) reached for interfaces: %s",
                timeout,
                ", ".join(sorted(futures_to_keyword[fut] for fut in not_done)),
            )
        # Process completed futures
        for future in done:
            keyword = futures_to_keyword[future]
            try:
                available = future.result()
            except NotImplementedError:
                log_autodetect.debug(
                    'interface "%s" does not support detection of available configurations',
                    keyword,
                )
            else:
                log_autodetect.debug(
                    'interface "%s" detected %i available configurations',
                    keyword,
                    len(available),
                )
                for config in available:
                    config.setdefault("interface", keyword)
                result.extend(available)
    finally:
        # shutdown immediately, do not wait for pending threads
        executor.shutdown(wait=False, cancel_futures=True)
    return result
