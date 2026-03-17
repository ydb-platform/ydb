"""
Ctypes wrapper module for Vector CAN Interface on win32/win64 systems.

Authors: Julien Grave <grave.jul@gmail.com>, Christian Sandberg
"""

import contextlib
import ctypes
import logging
import os
import time
import warnings
from collections.abc import Iterator, Sequence
from types import ModuleType
from typing import (
    Any,
    Callable,
    NamedTuple,
    Optional,
    Union,
    cast,
)

from can import (
    BitTiming,
    BitTimingFd,
    BusABC,
    CanInitializationError,
    CanInterfaceNotImplementedError,
    CanProtocol,
    Message,
)
from can.typechecking import AutoDetectedConfig, CanFilters
from can.util import (
    check_or_adjust_timing_clock,
    deprecated_args_alias,
    dlc2len,
    len2dlc,
    time_perfcounter_correlation,
)

from . import xlclass, xldefine
from .exceptions import VectorError, VectorInitializationError, VectorOperationError

LOG = logging.getLogger(__name__)

# Import safely Vector API module for Travis tests
xldriver: Optional[ModuleType] = None
try:
    from . import xldriver
except FileNotFoundError as exc:
    LOG.warning("Could not import vxlapi: %s", exc)

WaitForSingleObject: Optional[Callable[[int, int], int]]
INFINITE: Optional[int]
try:
    # Try builtin Python 3 Windows API
    from _winapi import (  # type: ignore[attr-defined,no-redef,unused-ignore]
        INFINITE,
        WaitForSingleObject,
    )

    HAS_EVENTS = True
except ImportError:
    WaitForSingleObject, INFINITE = None, None
    HAS_EVENTS = False


class VectorBus(BusABC):
    """The CAN Bus implemented for the Vector interface."""

    @deprecated_args_alias(
        deprecation_start="4.0.0",
        deprecation_end="5.0.0",
        **{
            "sjwAbr": "sjw_abr",
            "tseg1Abr": "tseg1_abr",
            "tseg2Abr": "tseg2_abr",
            "sjwDbr": "sjw_dbr",
            "tseg1Dbr": "tseg1_dbr",
            "tseg2Dbr": "tseg2_dbr",
        },
    )
    def __init__(
        self,
        channel: Union[int, Sequence[int], str],
        can_filters: Optional[CanFilters] = None,
        poll_interval: float = 0.01,
        receive_own_messages: bool = False,
        timing: Optional[Union[BitTiming, BitTimingFd]] = None,
        bitrate: Optional[int] = None,
        rx_queue_size: int = 2**14,
        app_name: Optional[str] = "CANalyzer",
        serial: Optional[int] = None,
        fd: bool = False,
        data_bitrate: Optional[int] = None,
        sjw_abr: int = 2,
        tseg1_abr: int = 6,
        tseg2_abr: int = 3,
        sjw_dbr: int = 2,
        tseg1_dbr: int = 6,
        tseg2_dbr: int = 3,
        listen_only: Optional[bool] = False,
        **kwargs: Any,
    ) -> None:
        """
        :param channel:
            The channel indexes to create this bus with.
            Can also be a single integer or a comma separated string.
        :param can_filters:
            See :class:`can.BusABC`.
        :param receive_own_messages:
            See :class:`can.BusABC`.
        :param timing:
            An instance of :class:`~can.BitTiming` or :class:`~can.BitTimingFd`
            to specify the bit timing parameters for the VectorBus interface. The
            `f_clock` value of the timing instance must be set to 8_000_000 (8MHz)
            or 16_000_000 (16MHz) for CAN 2.0 or 80_000_000 (80MHz) for CAN FD.
            If this parameter is provided, it takes precedence over all other
            timing-related parameters.
            Otherwise, the bit timing can be specified using the following parameters:
            `bitrate` for standard CAN or `fd`, `data_bitrate`, `sjw_abr`, `tseg1_abr`,
            `tseg2_abr`, `sjw_dbr`, `tseg1_dbr`, and `tseg2_dbr` for CAN FD.
        :param poll_interval:
            Poll interval in seconds.
        :param bitrate:
            Bitrate in bits/s.
        :param rx_queue_size:
            Number of messages in receive queue (power of 2).
            CAN: range `16…32768`
            CAN-FD: range `8192…524288`
        :param app_name:
            Name of application in *Vector Hardware Config*.
            If set to `None`, the channel should be a global channel index.
        :param serial:
            Serial number of the hardware to be used.
            If set, the channel parameter refers to the channels ONLY on the specified hardware.
            If set, the `app_name` does not have to be previously defined in
            *Vector Hardware Config*.
        :param fd:
            If CAN-FD frames should be supported.
        :param data_bitrate:
            Which bitrate to use for data phase in CAN FD.
            Defaults to arbitration bitrate.
        :param sjw_abr:
            Bus timing value sample jump width (arbitration).
        :param tseg1_abr:
            Bus timing value tseg1 (arbitration)
        :param tseg2_abr:
            Bus timing value tseg2 (arbitration)
        :param sjw_dbr:
            Bus timing value sample jump width (data)
        :param tseg1_dbr:
            Bus timing value tseg1 (data)
        :param tseg2_dbr:
            Bus timing value tseg2 (data)
        :param listen_only:
            if the bus should be set to listen only mode.

        :raise ~can.exceptions.CanInterfaceNotImplementedError:
            If the current operating system is not supported or the driver could not be loaded.
        :raise ~can.exceptions.CanInitializationError:
            If the bus could not be set up.
            This may or may not be a :class:`~can.interfaces.vector.VectorInitializationError`.
        """
        self.__testing = kwargs.get("_testing", False)
        if os.name != "nt" and not self.__testing:
            raise CanInterfaceNotImplementedError(
                f"The Vector interface is only supported on Windows, "
                f'but you are running "{os.name}"'
            )

        if xldriver is None:
            raise CanInterfaceNotImplementedError("The Vector API has not been loaded")
        self.xldriver = xldriver  # keep reference so mypy knows it is not None
        self.xldriver.xlOpenDriver()

        self.poll_interval = poll_interval

        self.channels: Sequence[int]
        if isinstance(channel, int):
            self.channels = [channel]
        elif isinstance(channel, str):  # must be checked before generic Sequence
            # Assume comma separated string of channels
            self.channels = [int(ch.strip()) for ch in channel.split(",")]
        elif isinstance(channel, Sequence):
            self.channels = [int(ch) for ch in channel]
        else:
            raise TypeError(
                f"Invalid type for parameter 'channel': {type(channel).__name__}"
            )

        self._app_name = app_name.encode() if app_name is not None else b""
        self.channel_info = "Application {}: {}".format(
            app_name,
            ", ".join(f"CAN {ch + 1}" for ch in self.channels),
        )

        channel_configs = get_channel_configs()
        is_fd = isinstance(timing, BitTimingFd) if timing else fd

        self.mask = 0
        self.channel_masks: dict[int, int] = {}
        self.index_to_channel: dict[int, int] = {}
        self._can_protocol = CanProtocol.CAN_FD if is_fd else CanProtocol.CAN_20

        self._listen_only = listen_only

        for channel in self.channels:
            if (
                len(self.channels) == 1
                and (_channel_index := kwargs.get("channel_index", None)) is not None
            ):
                # VectorBus._detect_available_configs() might return multiple
                # devices with the same serial number, e.g. if a VN8900 is connected via both USB and Ethernet
                # at the same time. If the VectorBus is instantiated with a config, that was returned from
                # VectorBus._detect_available_configs(), then use the contained global channel_index
                # to avoid any ambiguities.
                channel_index = cast("int", _channel_index)
            else:
                channel_index = self._find_global_channel_idx(
                    channel=channel,
                    serial=serial,
                    app_name=app_name,
                    channel_configs=channel_configs,
                )
            LOG.debug("Channel index %d found", channel)

            channel_mask = 1 << channel_index
            self.channel_masks[channel] = channel_mask
            self.index_to_channel[channel_index] = channel
            self.mask |= channel_mask

        permission_mask = xlclass.XLaccess()
        # Set mask to request channel init permission if needed
        if bitrate or fd or timing or self._listen_only:
            permission_mask.value = self.mask

        interface_version = (
            xldefine.XL_InterfaceVersion.XL_INTERFACE_VERSION_V4
            if is_fd
            else xldefine.XL_InterfaceVersion.XL_INTERFACE_VERSION
        )

        self.port_handle = xlclass.XLportHandle(xldefine.XL_INVALID_PORTHANDLE)
        self.xldriver.xlOpenPort(
            self.port_handle,
            self._app_name,
            self.mask,
            permission_mask,
            rx_queue_size,
            interface_version,
            xldefine.XL_BusTypes.XL_BUS_TYPE_CAN,
        )
        self.permission_mask = permission_mask.value

        LOG.debug(
            "Open Port: PortHandle: %d, ChannelMask: 0x%X, PermissionMask: 0x%X",
            self.port_handle.value,
            self.mask,
            self.permission_mask,
        )

        assert_timing = (bitrate or timing) and not self.__testing

        # set CAN settings
        if isinstance(timing, BitTiming):
            timing = check_or_adjust_timing_clock(timing, [16_000_000, 8_000_000])
            self._set_bit_timing(channel_mask=self.mask, timing=timing)
            if assert_timing:
                self._check_can_settings(
                    channel_mask=self.mask,
                    bitrate=timing.bitrate,
                    sample_point=timing.sample_point,
                )
        elif isinstance(timing, BitTimingFd):
            timing = check_or_adjust_timing_clock(timing, [80_000_000])
            self._set_bit_timing_fd(channel_mask=self.mask, timing=timing)
            if assert_timing:
                self._check_can_settings(
                    channel_mask=self.mask,
                    bitrate=timing.nom_bitrate,
                    sample_point=timing.nom_sample_point,
                    fd=True,
                    data_bitrate=timing.data_bitrate,
                    data_sample_point=timing.data_sample_point,
                )
        elif fd:
            timing = BitTimingFd.from_bitrate_and_segments(
                f_clock=80_000_000,
                nom_bitrate=bitrate or 500_000,
                nom_tseg1=tseg1_abr,
                nom_tseg2=tseg2_abr,
                nom_sjw=sjw_abr,
                data_bitrate=data_bitrate or bitrate or 500_000,
                data_tseg1=tseg1_dbr,
                data_tseg2=tseg2_dbr,
                data_sjw=sjw_dbr,
            )
            self._set_bit_timing_fd(channel_mask=self.mask, timing=timing)
            if assert_timing:
                self._check_can_settings(
                    channel_mask=self.mask,
                    bitrate=timing.nom_bitrate,
                    sample_point=timing.nom_sample_point,
                    fd=True,
                    data_bitrate=timing.data_bitrate,
                    data_sample_point=timing.data_sample_point,
                )
        elif bitrate:
            self._set_bitrate(channel_mask=self.mask, bitrate=bitrate)
            if assert_timing:
                self._check_can_settings(channel_mask=self.mask, bitrate=bitrate)

        if self._listen_only:
            self._set_output_mode(channel_mask=self.mask, listen_only=True)

        # Enable/disable TX receipts
        tx_receipts = 1 if receive_own_messages else 0
        self.xldriver.xlCanSetChannelMode(self.port_handle, self.mask, tx_receipts, 0)

        if HAS_EVENTS:
            self.event_handle = xlclass.XLhandle()
            self.xldriver.xlSetNotification(self.port_handle, self.event_handle, 1)
        else:
            LOG.info("Install pywin32 to avoid polling")

        # Calculate time offset for absolute timestamps
        offset = xlclass.XLuint64()
        try:
            if time.get_clock_info("time").resolution > 1e-5:
                ts, perfcounter = time_perfcounter_correlation()
                try:
                    self.xldriver.xlGetSyncTime(self.port_handle, offset)
                except VectorInitializationError:
                    self.xldriver.xlGetChannelTime(self.port_handle, self.mask, offset)
                current_perfcounter = time.perf_counter()
                now = ts + (current_perfcounter - perfcounter)
                self._time_offset = now - offset.value * 1e-9
            else:
                try:
                    self.xldriver.xlGetSyncTime(self.port_handle, offset)
                except VectorInitializationError:
                    self.xldriver.xlGetChannelTime(self.port_handle, self.mask, offset)
                self._time_offset = time.time() - offset.value * 1e-9

        except VectorInitializationError:
            self._time_offset = 0.0

        self._is_filtered = False
        super().__init__(
            channel=channel,
            can_filters=can_filters,
            **kwargs,
        )

        # activate channels after CAN filters were applied
        try:
            self.xldriver.xlActivateChannel(
                self.port_handle, self.mask, xldefine.XL_BusTypes.XL_BUS_TYPE_CAN, 0
            )
        except VectorOperationError as error:
            self.shutdown()
            raise VectorInitializationError.from_generic(error) from None

    @property
    def fd(self) -> bool:
        class_name = self.__class__.__name__
        warnings.warn(
            f"The {class_name}.fd property is deprecated and superseded by "
            f"{class_name}.protocol. It is scheduled for removal in python-can version 5.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._can_protocol is CanProtocol.CAN_FD

    def _find_global_channel_idx(
        self,
        channel: int,
        serial: Optional[int],
        app_name: Optional[str],
        channel_configs: list["VectorChannelConfig"],
    ) -> int:
        if serial is not None:
            serial_found = False
            for channel_config in channel_configs:
                if channel_config.serial_number != serial:
                    continue

                serial_found = True
                if channel_config.hw_channel == channel:
                    return channel_config.channel_index

            if not serial_found:
                err_msg = f"No interface with serial {serial} found."
            else:
                err_msg = (
                    f"Channel {channel} not found on interface with serial {serial}."
                )
            raise CanInitializationError(
                err_msg, error_code=xldefine.XL_Status.XL_ERR_HW_NOT_PRESENT
            )

        if app_name:
            hw_type, hw_index, hw_channel = self.get_application_config(
                app_name, channel
            )
            idx = cast(
                "int", self.xldriver.xlGetChannelIndex(hw_type, hw_index, hw_channel)
            )
            if idx < 0:
                # Undocumented behavior! See issue #353.
                # If hardware is unavailable, this function returns -1.
                # Raise an exception as if the driver
                # would have signalled XL_ERR_HW_NOT_PRESENT.
                raise VectorInitializationError(
                    xldefine.XL_Status.XL_ERR_HW_NOT_PRESENT,
                    xldefine.XL_Status.XL_ERR_HW_NOT_PRESENT.name,
                    "xlGetChannelIndex",
                )
            return idx

        # check if channel is a valid global channel index
        for channel_config in channel_configs:
            if channel == channel_config.channel_index:
                return channel

        raise CanInitializationError(
            f"Channel {channel} not found. The 'channel' parameter must be "
            f"a valid global channel index if neither 'app_name' nor 'serial' were given.",
            error_code=xldefine.XL_Status.XL_ERR_HW_NOT_PRESENT,
        )

    def _has_init_access(self, channel: int) -> bool:
        return bool(self.permission_mask & self.channel_masks[channel])

    def _read_bus_params(
        self, channel_index: int, vcc_list: list["VectorChannelConfig"]
    ) -> "VectorBusParams":
        for vcc in vcc_list:
            if vcc.channel_index == channel_index:
                bus_params = vcc.bus_params
                if bus_params is None:
                    # for CAN channels, this should never be `None`
                    raise ValueError("Invalid bus parameters.")
                return bus_params

        channel = self.index_to_channel[channel_index]
        raise CanInitializationError(
            f"Channel configuration for channel {channel} not found."
        )

    def _set_output_mode(self, channel_mask: int, listen_only: bool) -> None:
        # set parameters for channels with init access
        channel_mask = channel_mask & self.permission_mask

        if channel_mask:
            if listen_only:
                self.xldriver.xlCanSetChannelOutput(
                    self.port_handle,
                    channel_mask,
                    xldefine.XL_OutputMode.XL_OUTPUT_MODE_SILENT,
                )
            else:
                self.xldriver.xlCanSetChannelOutput(
                    self.port_handle,
                    channel_mask,
                    xldefine.XL_OutputMode.XL_OUTPUT_MODE_NORMAL,
                )

            LOG.info("xlCanSetChannelOutput: listen_only=%u", listen_only)
        else:
            LOG.warning("No channels with init access to set listen only mode")

    def _set_bitrate(self, channel_mask: int, bitrate: int) -> None:
        # set parameters for channels with init access
        channel_mask = channel_mask & self.permission_mask
        if channel_mask:
            self.xldriver.xlCanSetChannelBitrate(
                self.port_handle,
                channel_mask,
                bitrate,
            )
            LOG.info("xlCanSetChannelBitrate: baudr.=%u", bitrate)

    def _set_bit_timing(self, channel_mask: int, timing: BitTiming) -> None:
        # set parameters for channels with init access
        channel_mask = channel_mask & self.permission_mask
        if channel_mask:
            if timing.f_clock == 8_000_000:
                self.xldriver.xlCanSetChannelParamsC200(
                    self.port_handle,
                    channel_mask,
                    timing.btr0,
                    timing.btr1,
                )
                LOG.info(
                    "xlCanSetChannelParamsC200: BTR0=%#02x, BTR1=%#02x",
                    timing.btr0,
                    timing.btr1,
                )
            elif timing.f_clock == 16_000_000:
                chip_params = xlclass.XLchipParams()
                chip_params.bitRate = timing.bitrate
                chip_params.sjw = timing.sjw
                chip_params.tseg1 = timing.tseg1
                chip_params.tseg2 = timing.tseg2
                chip_params.sam = timing.nof_samples
                self.xldriver.xlCanSetChannelParams(
                    self.port_handle,
                    channel_mask,
                    chip_params,
                )
                LOG.info(
                    "xlCanSetChannelParams: baudr.=%u, sjwAbr=%u, tseg1Abr=%u, tseg2Abr=%u",
                    chip_params.bitRate,
                    chip_params.sjw,
                    chip_params.tseg1,
                    chip_params.tseg2,
                )
            else:
                raise CanInitializationError(
                    f"timing.f_clock must be 8_000_000 or 16_000_000 (is {timing.f_clock})"
                )

    def _set_bit_timing_fd(
        self,
        channel_mask: int,
        timing: BitTimingFd,
    ) -> None:
        # set parameters for channels with init access
        channel_mask = channel_mask & self.permission_mask
        if channel_mask:
            canfd_conf = xlclass.XLcanFdConf()
            canfd_conf.arbitrationBitRate = timing.nom_bitrate
            canfd_conf.sjwAbr = timing.nom_sjw
            canfd_conf.tseg1Abr = timing.nom_tseg1
            canfd_conf.tseg2Abr = timing.nom_tseg2
            canfd_conf.dataBitRate = timing.data_bitrate
            canfd_conf.sjwDbr = timing.data_sjw
            canfd_conf.tseg1Dbr = timing.data_tseg1
            canfd_conf.tseg2Dbr = timing.data_tseg2
            self.xldriver.xlCanFdSetConfiguration(
                self.port_handle, channel_mask, canfd_conf
            )
            LOG.info(
                "xlCanFdSetConfiguration.: ABaudr.=%u, DBaudr.=%u",
                canfd_conf.arbitrationBitRate,
                canfd_conf.dataBitRate,
            )
            LOG.info(
                "xlCanFdSetConfiguration.: sjwAbr=%u, tseg1Abr=%u, tseg2Abr=%u",
                canfd_conf.sjwAbr,
                canfd_conf.tseg1Abr,
                canfd_conf.tseg2Abr,
            )
            LOG.info(
                "xlCanFdSetConfiguration.: sjwDbr=%u, tseg1Dbr=%u, tseg2Dbr=%u",
                canfd_conf.sjwDbr,
                canfd_conf.tseg1Dbr,
                canfd_conf.tseg2Dbr,
            )

    def _check_can_settings(
        self,
        channel_mask: int,
        bitrate: int,
        sample_point: Optional[float] = None,
        fd: bool = False,
        data_bitrate: Optional[int] = None,
        data_sample_point: Optional[float] = None,
    ) -> None:
        """Compare requested CAN settings to active settings in driver."""
        vcc_list = get_channel_configs()
        for channel_index in _iterate_channel_index(channel_mask):
            bus_params = self._read_bus_params(
                channel_index=channel_index, vcc_list=vcc_list
            )
            # use bus_params.canfd even if fd==False, bus_params.can and bus_params.canfd are a C union
            bus_params_data = bus_params.canfd
            settings_acceptable = True

            # check bus type
            settings_acceptable &= (
                bus_params.bus_type is xldefine.XL_BusTypes.XL_BUS_TYPE_CAN
            )

            # check CAN operation mode
            # skip the check if can_op_mode is 0
            # as it happens for cancaseXL, VN7600 and sometimes on other hardware (VN1640)
            if bus_params_data.can_op_mode:
                if fd:
                    settings_acceptable &= bool(
                        bus_params_data.can_op_mode
                        & xldefine.XL_CANFD_BusParams_CanOpMode.XL_BUS_PARAMS_CANOPMODE_CANFD
                    )
                else:
                    settings_acceptable &= bool(
                        bus_params_data.can_op_mode
                        & xldefine.XL_CANFD_BusParams_CanOpMode.XL_BUS_PARAMS_CANOPMODE_CAN20
                    )

            # check bitrates
            if bitrate:
                settings_acceptable &= (
                    abs(bus_params_data.bitrate - bitrate) < bitrate / 256
                )
            if fd and data_bitrate:
                settings_acceptable &= (
                    abs(bus_params_data.data_bitrate - data_bitrate)
                    < data_bitrate / 256
                )

            # check sample points
            if sample_point:
                nom_sample_point_act = (
                    100
                    * (1 + bus_params_data.tseg1_abr)
                    / (1 + bus_params_data.tseg1_abr + bus_params_data.tseg2_abr)
                )
                settings_acceptable &= (
                    abs(nom_sample_point_act - sample_point)
                    < 2.0  # 2 percent tolerance
                )
            if fd and data_sample_point:
                data_sample_point_act = (
                    100
                    * (1 + bus_params_data.tseg1_dbr)
                    / (1 + bus_params_data.tseg1_dbr + bus_params_data.tseg2_dbr)
                )
                settings_acceptable &= (
                    abs(data_sample_point_act - data_sample_point)
                    < 2.0  # 2 percent tolerance
                )

            if not settings_acceptable:
                # The error message depends on the currently active CAN settings.
                # If the active operation mode is CAN FD, show the active CAN FD timings,
                # otherwise show CAN 2.0 timings.
                if bool(
                    bus_params_data.can_op_mode
                    & xldefine.XL_CANFD_BusParams_CanOpMode.XL_BUS_PARAMS_CANOPMODE_CANFD
                ):
                    active_settings = bus_params.canfd._asdict()
                    active_settings["can_op_mode"] = "CAN FD"
                else:
                    active_settings = bus_params.can._asdict()
                    active_settings["can_op_mode"] = "CAN 2.0"
                settings_string = ", ".join(
                    [f"{key}: {val}" for key, val in active_settings.items()]
                )
                channel = self.index_to_channel[channel_index]
                raise CanInitializationError(
                    f"The requested settings could not be set for channel {channel}. "
                    f"Another application might have set incompatible settings. "
                    f"These are the currently active settings: {settings_string}."
                )

    def _apply_filters(self, filters: Optional[CanFilters]) -> None:
        if filters:
            # Only up to one filter per ID type allowed
            if len(filters) == 1 or (
                len(filters) == 2
                and filters[0].get("extended") != filters[1].get("extended")
            ):
                try:
                    for can_filter in filters:
                        self.xldriver.xlCanSetChannelAcceptance(
                            self.port_handle,
                            self.mask,
                            can_filter["can_id"],
                            can_filter["can_mask"],
                            (
                                xldefine.XL_AcceptanceFilter.XL_CAN_EXT
                                if can_filter.get("extended")
                                else xldefine.XL_AcceptanceFilter.XL_CAN_STD
                            ),
                        )
                except VectorOperationError as exception:
                    LOG.warning("Could not set filters: %s", exception)
                    # go to fallback
                else:
                    self._is_filtered = True
                    return
            else:
                LOG.warning("Only up to one filter per extended or standard ID allowed")
                # go to fallback

        # fallback: reset filters
        self._is_filtered = False
        try:
            self.xldriver.xlCanSetChannelAcceptance(
                self.port_handle,
                self.mask,
                0x0,
                0x0,
                xldefine.XL_AcceptanceFilter.XL_CAN_EXT,
            )
            self.xldriver.xlCanSetChannelAcceptance(
                self.port_handle,
                self.mask,
                0x0,
                0x0,
                xldefine.XL_AcceptanceFilter.XL_CAN_STD,
            )
        except VectorOperationError as exc:
            LOG.warning("Could not reset filters: %s", exc)

    def _recv_internal(
        self, timeout: Optional[float]
    ) -> tuple[Optional[Message], bool]:
        end_time = time.time() + timeout if timeout is not None else None

        while True:
            try:
                if self._can_protocol is CanProtocol.CAN_FD:
                    msg = self._recv_canfd()
                else:
                    msg = self._recv_can()

            except VectorOperationError as exception:
                if exception.error_code != xldefine.XL_Status.XL_ERR_QUEUE_IS_EMPTY:
                    raise
            else:
                if msg:
                    return msg, self._is_filtered

            # if no message was received, wait or return on timeout
            if end_time is not None and time.time() > end_time:
                return None, self._is_filtered

            if HAS_EVENTS:
                # Wait for receive event to occur
                if end_time is None:
                    time_left_ms = INFINITE
                else:
                    time_left = end_time - time.time()
                    time_left_ms = max(0, int(time_left * 1000))
                WaitForSingleObject(self.event_handle.value, time_left_ms)  # type: ignore
            else:
                # Wait a short time until we try again
                time.sleep(self.poll_interval)

    def _recv_canfd(self) -> Optional[Message]:
        xl_can_rx_event = xlclass.XLcanRxEvent()
        self.xldriver.xlCanReceive(self.port_handle, xl_can_rx_event)

        if xl_can_rx_event.tag == xldefine.XL_CANFD_RX_EventTags.XL_CAN_EV_TAG_RX_OK:
            is_rx = True
            data_struct = xl_can_rx_event.tagData.canRxOkMsg
        elif xl_can_rx_event.tag == xldefine.XL_CANFD_RX_EventTags.XL_CAN_EV_TAG_TX_OK:
            is_rx = False
            data_struct = xl_can_rx_event.tagData.canTxOkMsg
        else:
            self.handle_canfd_event(xl_can_rx_event)
            return None

        msg_id = data_struct.canId
        dlc = dlc2len(data_struct.dlc)
        flags = data_struct.msgFlags
        timestamp = xl_can_rx_event.timeStamp * 1e-9
        channel = self.index_to_channel.get(xl_can_rx_event.chanIndex)

        return Message(
            timestamp=timestamp + self._time_offset,
            arbitration_id=msg_id & 0x1FFFFFFF,
            is_extended_id=bool(
                msg_id & xldefine.XL_MessageFlagsExtended.XL_CAN_EXT_MSG_ID
            ),
            is_remote_frame=bool(
                flags & xldefine.XL_CANFD_RX_MessageFlags.XL_CAN_RXMSG_FLAG_RTR
            ),
            is_error_frame=bool(
                flags & xldefine.XL_CANFD_RX_MessageFlags.XL_CAN_RXMSG_FLAG_EF
            ),
            is_fd=bool(flags & xldefine.XL_CANFD_RX_MessageFlags.XL_CAN_RXMSG_FLAG_EDL),
            bitrate_switch=bool(
                flags & xldefine.XL_CANFD_RX_MessageFlags.XL_CAN_RXMSG_FLAG_BRS
            ),
            error_state_indicator=bool(
                flags & xldefine.XL_CANFD_RX_MessageFlags.XL_CAN_RXMSG_FLAG_ESI
            ),
            is_rx=is_rx,
            channel=channel,
            dlc=dlc,
            data=data_struct.data[:dlc],
        )

    def _recv_can(self) -> Optional[Message]:
        xl_event = xlclass.XLevent()
        event_count = ctypes.c_uint(1)
        self.xldriver.xlReceive(self.port_handle, event_count, xl_event)

        if xl_event.tag != xldefine.XL_EventTags.XL_RECEIVE_MSG:
            self.handle_can_event(xl_event)
            return None

        msg_id = xl_event.tagData.msg.id
        dlc = xl_event.tagData.msg.dlc
        flags = xl_event.tagData.msg.flags
        timestamp = xl_event.timeStamp * 1e-9
        channel = self.index_to_channel.get(xl_event.chanIndex)

        return Message(
            timestamp=timestamp + self._time_offset,
            arbitration_id=msg_id & 0x1FFFFFFF,
            is_extended_id=bool(
                msg_id & xldefine.XL_MessageFlagsExtended.XL_CAN_EXT_MSG_ID
            ),
            is_remote_frame=bool(
                flags & xldefine.XL_MessageFlags.XL_CAN_MSG_FLAG_REMOTE_FRAME
            ),
            is_error_frame=bool(
                flags & xldefine.XL_MessageFlags.XL_CAN_MSG_FLAG_ERROR_FRAME
            ),
            is_rx=not bool(
                flags & xldefine.XL_MessageFlags.XL_CAN_MSG_FLAG_TX_COMPLETED
            ),
            is_fd=False,
            dlc=dlc,
            data=xl_event.tagData.msg.data[:dlc],
            channel=channel,
        )

    def handle_can_event(self, event: xlclass.XLevent) -> None:
        """Handle non-message CAN events.

        Method is called by :meth:`~can.interfaces.vector.VectorBus._recv_internal`
        when `event.tag` is not `XL_RECEIVE_MSG`. Subclasses can implement this method.

        :param event: XLevent that could have a `XL_CHIP_STATE`, `XL_TIMER` or `XL_SYNC_PULSE` tag.
        """

    def handle_canfd_event(self, event: xlclass.XLcanRxEvent) -> None:
        """Handle non-message CAN FD events.

        Method is called by :meth:`~can.interfaces.vector.VectorBus._recv_internal`
        when `event.tag` is not `XL_CAN_EV_TAG_RX_OK` or `XL_CAN_EV_TAG_TX_OK`.
        Subclasses can implement this method.

        :param event: `XLcanRxEvent` that could have a `XL_CAN_EV_TAG_RX_ERROR`,
            `XL_CAN_EV_TAG_TX_ERROR`, `XL_TIMER` or `XL_CAN_EV_TAG_CHIP_STATE` tag.
        """

    def send(self, msg: Message, timeout: Optional[float] = None) -> None:
        self._send_sequence([msg])

    def _send_sequence(self, msgs: Sequence[Message]) -> int:
        """Send messages and return number of successful transmissions."""
        if self._can_protocol is CanProtocol.CAN_FD:
            return self._send_can_fd_msg_sequence(msgs)
        else:
            return self._send_can_msg_sequence(msgs)

    def _get_tx_channel_mask(self, msgs: Sequence[Message]) -> int:
        if len(msgs) == 1:
            return self.channel_masks.get(msgs[0].channel, self.mask)  # type: ignore[arg-type]
        else:
            return self.mask

    def _send_can_msg_sequence(self, msgs: Sequence[Message]) -> int:
        """Send CAN messages and return number of successful transmissions."""
        mask = self._get_tx_channel_mask(msgs)
        message_count = ctypes.c_uint(len(msgs))

        xl_event_array = (xlclass.XLevent * message_count.value)(
            *map(self._build_xl_event, msgs)
        )

        self.xldriver.xlCanTransmit(
            self.port_handle, mask, message_count, xl_event_array
        )
        return message_count.value

    @staticmethod
    def _build_xl_event(msg: Message) -> xlclass.XLevent:
        msg_id = msg.arbitration_id
        if msg.is_extended_id:
            msg_id |= xldefine.XL_MessageFlagsExtended.XL_CAN_EXT_MSG_ID

        flags = 0
        if msg.is_remote_frame:
            flags |= xldefine.XL_MessageFlags.XL_CAN_MSG_FLAG_REMOTE_FRAME

        xl_event = xlclass.XLevent()
        xl_event.tag = xldefine.XL_EventTags.XL_TRANSMIT_MSG
        xl_event.tagData.msg.id = msg_id
        xl_event.tagData.msg.dlc = msg.dlc
        xl_event.tagData.msg.flags = flags
        xl_event.tagData.msg.data = tuple(msg.data)

        return xl_event

    def _send_can_fd_msg_sequence(self, msgs: Sequence[Message]) -> int:
        """Send CAN FD messages and return number of successful transmissions."""
        mask = self._get_tx_channel_mask(msgs)
        message_count = len(msgs)

        xl_can_tx_event_array = (xlclass.XLcanTxEvent * message_count)(
            *map(self._build_xl_can_tx_event, msgs)
        )

        msg_count_sent = ctypes.c_uint(0)
        self.xldriver.xlCanTransmitEx(
            self.port_handle, mask, message_count, msg_count_sent, xl_can_tx_event_array
        )
        return msg_count_sent.value

    @staticmethod
    def _build_xl_can_tx_event(msg: Message) -> xlclass.XLcanTxEvent:
        msg_id = msg.arbitration_id
        if msg.is_extended_id:
            msg_id |= xldefine.XL_MessageFlagsExtended.XL_CAN_EXT_MSG_ID

        flags = 0
        if msg.is_fd:
            flags |= xldefine.XL_CANFD_TX_MessageFlags.XL_CAN_TXMSG_FLAG_EDL
        if msg.bitrate_switch:
            flags |= xldefine.XL_CANFD_TX_MessageFlags.XL_CAN_TXMSG_FLAG_BRS
        if msg.is_remote_frame:
            flags |= xldefine.XL_CANFD_TX_MessageFlags.XL_CAN_TXMSG_FLAG_RTR

        xl_can_tx_event = xlclass.XLcanTxEvent()
        xl_can_tx_event.tag = xldefine.XL_CANFD_TX_EventTags.XL_CAN_EV_TAG_TX_MSG
        xl_can_tx_event.transId = 0xFFFF

        xl_can_tx_event.tagData.canMsg.canId = msg_id
        xl_can_tx_event.tagData.canMsg.msgFlags = flags
        xl_can_tx_event.tagData.canMsg.dlc = len2dlc(msg.dlc)
        xl_can_tx_event.tagData.canMsg.data = tuple(msg.data)

        return xl_can_tx_event

    def flush_tx_buffer(self) -> None:
        """
        Flush the TX buffer of the bus.

        Implementation does not use function ``xlCanFlushTransmitQueue`` of the XL driver, as it works only
        for XL family devices.

        .. warning::
            Using this function will flush the queue and send a high voltage message (ID = 0, DLC = 0, no data).
        """
        if self._can_protocol is CanProtocol.CAN_FD:
            xl_can_tx_event = xlclass.XLcanTxEvent()
            xl_can_tx_event.tag = xldefine.XL_CANFD_TX_EventTags.XL_CAN_EV_TAG_TX_MSG
            xl_can_tx_event.tagData.canMsg.msgFlags |= (
                xldefine.XL_CANFD_TX_MessageFlags.XL_CAN_TXMSG_FLAG_HIGHPRIO
            )

            self.xldriver.xlCanTransmitEx(
                self.port_handle,
                self.mask,
                ctypes.c_uint(1),
                ctypes.c_uint(0),
                xl_can_tx_event,
            )
        else:
            xl_event = xlclass.XLevent()
            xl_event.tag = xldefine.XL_EventTags.XL_TRANSMIT_MSG
            xl_event.tagData.msg.flags |= (
                xldefine.XL_MessageFlags.XL_CAN_MSG_FLAG_OVERRUN
                | xldefine.XL_MessageFlags.XL_CAN_MSG_FLAG_WAKEUP
            )

            self.xldriver.xlCanTransmit(
                self.port_handle, self.mask, ctypes.c_uint(1), xl_event
            )

    def shutdown(self) -> None:
        super().shutdown()

        with contextlib.suppress(VectorError):
            self.xldriver.xlDeactivateChannel(self.port_handle, self.mask)
            self.xldriver.xlClosePort(self.port_handle)
            self.xldriver.xlCloseDriver()

    def reset(self) -> None:
        self.xldriver.xlDeactivateChannel(self.port_handle, self.mask)
        self.xldriver.xlActivateChannel(
            self.port_handle, self.mask, xldefine.XL_BusTypes.XL_BUS_TYPE_CAN, 0
        )

    @staticmethod
    def _detect_available_configs() -> Sequence["AutoDetectedVectorConfig"]:
        configs: list[AutoDetectedVectorConfig] = []
        channel_configs = get_channel_configs()
        LOG.info("Found %d channels", len(channel_configs))
        for channel_config in channel_configs:
            if (
                not channel_config.channel_bus_capabilities
                & xldefine.XL_BusCapabilities.XL_BUS_ACTIVE_CAP_CAN
            ):
                continue
            LOG.info(
                "Channel index %d: %s",
                channel_config.channel_index,
                channel_config.name,
            )
            configs.append(
                {
                    "interface": "vector",
                    "channel": channel_config.hw_channel,
                    "serial": channel_config.serial_number,
                    "channel_index": channel_config.channel_index,
                    "hw_type": channel_config.hw_type,
                    "hw_index": channel_config.hw_index,
                    "hw_channel": channel_config.hw_channel,
                    "supports_fd": bool(
                        channel_config.channel_capabilities
                        & xldefine.XL_ChannelCapabilities.XL_CHANNEL_FLAG_CANFD_ISO_SUPPORT
                    ),
                    "vector_channel_config": channel_config,
                }
            )
        return configs

    @staticmethod
    def popup_vector_hw_configuration(wait_for_finish: int = 0) -> None:
        """Open vector hardware configuration window.

        :param wait_for_finish:
            Time to wait for user input in milliseconds.
        """
        if xldriver is None:
            raise CanInterfaceNotImplementedError("The Vector API has not been loaded")

        xldriver.xlPopupHwConfig(ctypes.c_char_p(), ctypes.c_uint(wait_for_finish))

    @staticmethod
    def get_application_config(
        app_name: str, app_channel: int
    ) -> tuple[Union[int, xldefine.XL_HardwareType], int, int]:
        """Retrieve information for an application in Vector Hardware Configuration.

        :param app_name:
            The name of the application.
        :param app_channel:
            The channel of the application.
        :return:
            Returns a tuple of the hardware type, the hardware index and the
            hardware channel.

        :raises can.interfaces.vector.VectorInitializationError:
            If the application name does not exist in the Vector hardware configuration.
        """
        if xldriver is None:
            raise CanInterfaceNotImplementedError("The Vector API has not been loaded")

        hw_type = ctypes.c_uint()
        hw_index = ctypes.c_uint()
        hw_channel = ctypes.c_uint()
        _app_channel = ctypes.c_uint(app_channel)

        try:
            xldriver.xlGetApplConfig(
                app_name.encode(),
                _app_channel,
                hw_type,
                hw_index,
                hw_channel,
                xldefine.XL_BusTypes.XL_BUS_TYPE_CAN,
            )
        except VectorError as e:
            raise VectorInitializationError(
                error_code=e.error_code,
                error_string=(
                    f"Vector HW Config: Channel '{app_channel}' of "
                    f"application '{app_name}' is not assigned to any interface"
                ),
                function="xlGetApplConfig",
            ) from None
        return _hw_type(hw_type.value), hw_index.value, hw_channel.value

    @staticmethod
    def set_application_config(
        app_name: str,
        app_channel: int,
        hw_type: Union[int, xldefine.XL_HardwareType],
        hw_index: int,
        hw_channel: int,
        **kwargs: Any,
    ) -> None:
        """Modify the application settings in Vector Hardware Configuration.

        This method can also be used with a channel config dictionary::

            import can
            from can.interfaces.vector import VectorBus

            configs = can.detect_available_configs(interfaces=['vector'])
            cfg = configs[0]
            VectorBus.set_application_config(app_name="MyApplication", app_channel=0, **cfg)

        :param app_name:
            The name of the application. Creates a new application if it does
            not exist yet.
        :param app_channel:
            The channel of the application.
        :param hw_type:
            The hardware type of the interface.
            E.g XL_HardwareType.XL_HWTYPE_VIRTUAL
        :param hw_index:
            The index of the interface if multiple interface with the same
            hardware type are present.
        :param hw_channel:
            The channel index of the interface.

        :raises can.interfaces.vector.VectorInitializationError:
            If the application name does not exist in the Vector hardware configuration.
        """
        if xldriver is None:
            raise CanInterfaceNotImplementedError("The Vector API has not been loaded")

        xldriver.xlSetApplConfig(
            app_name.encode(),
            app_channel,
            hw_type,
            hw_index,
            hw_channel,
            xldefine.XL_BusTypes.XL_BUS_TYPE_CAN,
        )

    def set_timer_rate(self, timer_rate_ms: int) -> None:
        """Set the cyclic event rate of the port.

        Once set, the port will generate a cyclic event with the tag XL_EventTags.XL_TIMER.
        This timer can be used to keep an application alive. See XL Driver Library Description
        for more information

        :param timer_rate_ms:
            The timer rate in ms. The minimal timer rate is 1ms, a value of 0 deactivates
            the timer events.
        """
        timer_rate_10us = timer_rate_ms * 100
        self.xldriver.xlSetTimerRate(self.port_handle, timer_rate_10us)


class VectorCanParams(NamedTuple):
    bitrate: int
    sjw: int
    tseg1: int
    tseg2: int
    sam: int
    output_mode: xldefine.XL_OutputMode
    can_op_mode: xldefine.XL_CANFD_BusParams_CanOpMode


class VectorCanFdParams(NamedTuple):
    bitrate: int
    data_bitrate: int
    sjw_abr: int
    tseg1_abr: int
    tseg2_abr: int
    sam_abr: int
    sjw_dbr: int
    tseg1_dbr: int
    tseg2_dbr: int
    output_mode: xldefine.XL_OutputMode
    can_op_mode: xldefine.XL_CANFD_BusParams_CanOpMode


class VectorBusParams(NamedTuple):
    bus_type: xldefine.XL_BusTypes
    can: VectorCanParams
    canfd: VectorCanFdParams


class VectorChannelConfig(NamedTuple):
    """NamedTuple which contains the channel properties from Vector XL API."""

    name: str
    hw_type: Union[int, xldefine.XL_HardwareType]
    hw_index: int
    hw_channel: int
    channel_index: int
    channel_mask: int
    channel_capabilities: xldefine.XL_ChannelCapabilities
    channel_bus_capabilities: xldefine.XL_BusCapabilities
    is_on_bus: bool
    connected_bus_type: xldefine.XL_BusTypes
    bus_params: Optional[VectorBusParams]
    serial_number: int
    article_number: int
    transceiver_name: str


class AutoDetectedVectorConfig(AutoDetectedConfig):
    # data for use in VectorBus.__init__():
    serial: int
    channel_index: int
    # data for use in VectorBus.set_application_config():
    hw_type: int
    hw_index: int
    hw_channel: int
    # additional information:
    supports_fd: bool
    vector_channel_config: VectorChannelConfig


def _get_xl_driver_config() -> xlclass.XLdriverConfig:
    if xldriver is None:
        raise VectorError(
            error_code=xldefine.XL_Status.XL_ERR_DLL_NOT_FOUND,
            error_string="xldriver is unavailable",
            function="_get_xl_driver_config",
        )
    driver_config = xlclass.XLdriverConfig()
    xldriver.xlOpenDriver()
    xldriver.xlGetDriverConfig(driver_config)
    xldriver.xlCloseDriver()
    return driver_config


def _read_bus_params_from_c_struct(
    bus_params: xlclass.XLbusParams,
) -> Optional[VectorBusParams]:
    bus_type = xldefine.XL_BusTypes(bus_params.busType)
    if bus_type is not xldefine.XL_BusTypes.XL_BUS_TYPE_CAN:
        return None
    return VectorBusParams(
        bus_type=bus_type,
        can=VectorCanParams(
            bitrate=bus_params.data.can.bitRate,
            sjw=bus_params.data.can.sjw,
            tseg1=bus_params.data.can.tseg1,
            tseg2=bus_params.data.can.tseg2,
            sam=bus_params.data.can.sam,
            output_mode=xldefine.XL_OutputMode(bus_params.data.can.outputMode),
            can_op_mode=xldefine.XL_CANFD_BusParams_CanOpMode(
                bus_params.data.can.canOpMode
            ),
        ),
        canfd=VectorCanFdParams(
            bitrate=bus_params.data.canFD.arbitrationBitRate,
            data_bitrate=bus_params.data.canFD.dataBitRate,
            sjw_abr=bus_params.data.canFD.sjwAbr,
            tseg1_abr=bus_params.data.canFD.tseg1Abr,
            tseg2_abr=bus_params.data.canFD.tseg2Abr,
            sam_abr=bus_params.data.canFD.samAbr,
            sjw_dbr=bus_params.data.canFD.sjwDbr,
            tseg1_dbr=bus_params.data.canFD.tseg1Dbr,
            tseg2_dbr=bus_params.data.canFD.tseg2Dbr,
            output_mode=xldefine.XL_OutputMode(bus_params.data.canFD.outputMode),
            can_op_mode=xldefine.XL_CANFD_BusParams_CanOpMode(
                bus_params.data.canFD.canOpMode
            ),
        ),
    )


def get_channel_configs() -> list[VectorChannelConfig]:
    """Read channel properties from Vector XL API."""
    try:
        driver_config = _get_xl_driver_config()
    except VectorError:
        return []

    channel_list: list[VectorChannelConfig] = []
    for i in range(driver_config.channelCount):
        xlcc: xlclass.XLchannelConfig = driver_config.channel[i]
        vcc = VectorChannelConfig(
            name=xlcc.name.decode(),
            hw_type=_hw_type(xlcc.hwType),
            hw_index=xlcc.hwIndex,
            hw_channel=xlcc.hwChannel,
            channel_index=xlcc.channelIndex,
            channel_mask=xlcc.channelMask,
            channel_capabilities=xldefine.XL_ChannelCapabilities(
                xlcc.channelCapabilities
            ),
            channel_bus_capabilities=xldefine.XL_BusCapabilities(
                xlcc.channelBusCapabilities
            ),
            is_on_bus=bool(xlcc.isOnBus),
            bus_params=_read_bus_params_from_c_struct(xlcc.busParams),
            connected_bus_type=xldefine.XL_BusTypes(xlcc.connectedBusType),
            serial_number=xlcc.serialNumber,
            article_number=xlcc.articleNumber,
            transceiver_name=xlcc.transceiverName.decode(),
        )
        channel_list.append(vcc)
    return channel_list


def _hw_type(hw_type: int) -> Union[int, xldefine.XL_HardwareType]:
    try:
        return xldefine.XL_HardwareType(hw_type)
    except ValueError:
        LOG.warning(f'Unknown XL_HardwareType value "{hw_type}"')
        return hw_type


def _iterate_channel_index(channel_mask: int) -> Iterator[int]:
    """Iterate over channel indexes in channel mask."""
    for channel_index, bit in enumerate(reversed(bin(channel_mask)[2:])):
        if bit == "1":
            yield channel_index
