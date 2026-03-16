"""
Ctypes wrapper module for IXXAT Virtual CAN Interface V3 on win32 systems

TODO: We could implement this interface such that setting other filters
      could work when the initial filters were set to zero using the
      software fallback. Or could the software filters even be changed
      after the connection was opened? We need to document that bahaviour!
      See also the NICAN interface.

"""

import ctypes
import functools
import logging
import sys
import time
import warnings
from collections.abc import Sequence
from typing import Callable, Optional, Union

from can import (
    BusABC,
    CanProtocol,
    CyclicSendTaskABC,
    LimitedDurationCyclicSendTaskABC,
    Message,
    RestartableCyclicTaskABC,
)
from can.ctypesutil import HANDLE, PHANDLE, CLibrary
from can.ctypesutil import HRESULT as ctypes_HRESULT
from can.exceptions import CanInitializationError, CanInterfaceNotImplementedError
from can.util import deprecated_args_alias, dlc2len, len2dlc

from . import constants, structures
from .exceptions import *

__all__ = [
    "IXXATBus",
    "VCIBusOffError",
    "VCIDeviceNotFoundError",
    "VCIError",
    "VCITimeout",
    "vciFormatError",
]

log = logging.getLogger("can.ixxat")

# Hack to have vciFormatError as a free function, see below
vciFormatError = None

# main ctypes instance
_canlib = None
# TODO: Use ECI driver for linux
if sys.platform == "win32" or sys.platform == "cygwin":
    try:
        _canlib = CLibrary("vcinpl2.dll")
    except Exception as e:
        log.warning("Cannot load IXXAT vcinpl library: %s", e)
else:
    # Will not work on other systems, but have it importable anyway for
    # tests/sphinx
    log.warning("IXXAT VCI library does not work on %s platform", sys.platform)


def __vciFormatErrorExtended(
    library_instance: CLibrary, function: Callable, vret: int, args: tuple
):
    """Format a VCI error and attach failed function, decoded HRESULT and arguments
    :param CLibrary library_instance:
        Mapped instance of IXXAT vcinpl library
    :param callable function:
        Failed function
    :param HRESULT vret:
        HRESULT returned by vcinpl call
    :param args:
        Arbitrary arguments tuple
    :return:
        Formatted string
    """
    # TODO: make sure we don't generate another exception
    return (
        f"{__vciFormatError(library_instance, function, vret)} - arguments were {args}"
    )


def __vciFormatError(library_instance: CLibrary, function: Callable, vret: int):
    """Format a VCI error and attach failed function and decoded HRESULT
    :param CLibrary library_instance:
        Mapped instance of IXXAT vcinpl library
    :param callable function:
        Failed function
    :param HRESULT vret:
        HRESULT returned by vcinpl call
    :return:
        Formatted string
    """
    buf = ctypes.create_string_buffer(constants.VCI_MAX_ERRSTRLEN)
    ctypes.memset(buf, 0, constants.VCI_MAX_ERRSTRLEN)
    library_instance.vciFormatError(vret, buf, constants.VCI_MAX_ERRSTRLEN)
    return "function {} failed ({})".format(
        function._name, buf.value.decode("utf-8", "replace")
    )


def __check_status(result, function, args):
    """
    Check the result of a vcinpl function call and raise appropriate exception
    in case of an error. Used as errcheck function when mapping C functions
    with ctypes.
        :param result:
            Function call numeric result
        :param callable function:
            Called function
        :param args:
            Arbitrary arguments tuple
        :raise:
            :class:VCITimeout
            :class:VCIRxQueueEmptyError
            :class:StopIteration
            :class:VCIError
    """
    if result == constants.VCI_E_TIMEOUT:
        raise VCITimeout(f"Function {function._name} timed out")
    elif result == constants.VCI_E_RXQUEUE_EMPTY:
        raise VCIRxQueueEmptyError()
    elif result == constants.VCI_E_NO_MORE_ITEMS:
        raise StopIteration()
    elif result == constants.VCI_E_ACCESSDENIED:
        pass  # not a real error, might happen if another program has initialized the bus
    elif result != constants.VCI_OK:
        raise VCIError(vciFormatError(function, result))

    return result


try:
    hresult_type = ctypes.c_ulong
    # Map all required symbols and initialize library ---------------------------
    # HRESULT VCIAPI vciInitialize ( void );
    _canlib.map_symbol("vciInitialize", hresult_type, (), __check_status)

    # void VCIAPI vciFormatError (HRESULT hrError, PCHAR pszText, UINT32 dwsize);
    try:
        _canlib.map_symbol(
            "vciFormatError", None, (ctypes_HRESULT, ctypes.c_char_p, ctypes.c_uint32)
        )
    except ImportError:
        _canlib.map_symbol(
            "vciFormatErrorA", None, (ctypes_HRESULT, ctypes.c_char_p, ctypes.c_uint32)
        )
        _canlib.vciFormatError = _canlib.vciFormatErrorA
    # Hack to have vciFormatError as a free function
    vciFormatError = functools.partial(__vciFormatError, _canlib)

    # HRESULT VCIAPI vciEnumDeviceOpen( OUT PHANDLE hEnum );
    _canlib.map_symbol("vciEnumDeviceOpen", hresult_type, (PHANDLE,), __check_status)
    # HRESULT VCIAPI vciEnumDeviceClose ( IN HANDLE hEnum );
    _canlib.map_symbol("vciEnumDeviceClose", hresult_type, (HANDLE,), __check_status)
    # HRESULT VCIAPI vciEnumDeviceNext( IN  HANDLE hEnum, OUT PVCIDEVICEINFO pInfo );
    _canlib.map_symbol(
        "vciEnumDeviceNext",
        hresult_type,
        (HANDLE, structures.PVCIDEVICEINFO),
        __check_status,
    )

    # HRESULT VCIAPI vciDeviceOpen( IN  REFVCIID rVciid, OUT PHANDLE  phDevice );
    _canlib.map_symbol(
        "vciDeviceOpen", hresult_type, (structures.PVCIID, PHANDLE), __check_status
    )
    # HRESULT vciDeviceClose( HANDLE hDevice )
    _canlib.map_symbol("vciDeviceClose", hresult_type, (HANDLE,), __check_status)

    # HRESULT VCIAPI canChannelOpen( IN  HANDLE  hDevice, IN  UINT32  dwCanNo, IN  BOOL    fExclusive, OUT PHANDLE phCanChn );
    _canlib.map_symbol(
        "canChannelOpen",
        hresult_type,
        (HANDLE, ctypes.c_uint32, ctypes.c_long, PHANDLE),
        __check_status,
    )
    # EXTERN_C HRESULT VCIAPI
    #   canChannelInitialize( IN HANDLE hCanChn,
    #                         IN UINT16 wRxFifoSize,
    #                         IN UINT16 wRxThreshold,
    #                         IN UINT16 wTxFifoSize,
    #                         IN UINT16 wTxThreshold,
    #                         IN UINT32 dwFilterSize,
    #                         IN UINT8  bFilterMode );
    _canlib.map_symbol(
        "canChannelInitialize",
        hresult_type,
        (
            HANDLE,
            ctypes.c_uint16,
            ctypes.c_uint16,
            ctypes.c_uint16,
            ctypes.c_uint16,
            ctypes.c_uint32,
            ctypes.c_uint8,
        ),
        __check_status,
    )
    # EXTERN_C HRESULT VCIAPI canChannelActivate( IN HANDLE hCanChn, IN BOOL   fEnable );
    _canlib.map_symbol(
        "canChannelActivate", hresult_type, (HANDLE, ctypes.c_long), __check_status
    )
    # HRESULT canChannelClose( HANDLE hChannel )
    _canlib.map_symbol("canChannelClose", hresult_type, (HANDLE,), __check_status)
    # EXTERN_C HRESULT VCIAPI canChannelReadMessage( IN  HANDLE  hCanChn, IN  UINT32  dwMsTimeout, OUT PCANMSG2 pCanMsg );
    _canlib.map_symbol(
        "canChannelReadMessage",
        hresult_type,
        (HANDLE, ctypes.c_uint32, structures.PCANMSG2),
        __check_status,
    )
    # HRESULT canChannelPeekMessage(HANDLE hChannel,PCANMSG2 pCanMsg );
    _canlib.map_symbol(
        "canChannelPeekMessage",
        hresult_type,
        (HANDLE, structures.PCANMSG2),
        __check_status,
    )
    # HRESULT canChannelWaitTxEvent (HANDLE hChannel UINT32 dwMsTimeout );
    _canlib.map_symbol(
        "canChannelWaitTxEvent",
        hresult_type,
        (HANDLE, ctypes.c_uint32),
        __check_status,
    )
    # HRESULT canChannelWaitRxEvent (HANDLE hChannel, UINT32 dwMsTimeout );
    _canlib.map_symbol(
        "canChannelWaitRxEvent",
        hresult_type,
        (HANDLE, ctypes.c_uint32),
        __check_status,
    )
    # HRESULT canChannelPostMessage (HANDLE hChannel, PCANMSG2 pCanMsg );
    _canlib.map_symbol(
        "canChannelPostMessage",
        hresult_type,
        (HANDLE, structures.PCANMSG2),
        __check_status,
    )
    # HRESULT canChannelSendMessage (HANDLE hChannel, UINT32 dwMsTimeout, PCANMSG2 pCanMsg );
    _canlib.map_symbol(
        "canChannelSendMessage",
        hresult_type,
        (HANDLE, ctypes.c_uint32, structures.PCANMSG2),
        __check_status,
    )

    # EXTERN_C HRESULT VCIAPI canControlOpen( IN  HANDLE  hDevice, IN  UINT32  dwCanNo, OUT PHANDLE phCanCtl );
    _canlib.map_symbol(
        "canControlOpen",
        hresult_type,
        (HANDLE, ctypes.c_uint32, PHANDLE),
        __check_status,
    )
    # EXTERN_C HRESULT VCIAPI
    #   canControlInitialize( IN HANDLE  hCanCtl,
    #                         IN UINT8   bOpMode,
    #                         IN UINT8   bExMode,
    #                         IN UINT8   bSFMode,
    #                         IN UINT8   bEFMode,
    #                         IN UINT32  dwSFIds,
    #                         IN UINT32  dwEFIds,
    #                         IN PCANBTP pBtpSDR,
    #                         IN PCANBTP pBtpFDR );
    _canlib.map_symbol(
        "canControlInitialize",
        hresult_type,
        (
            HANDLE,
            ctypes.c_uint8,
            ctypes.c_uint8,
            ctypes.c_uint8,
            ctypes.c_uint8,
            ctypes.c_uint32,
            ctypes.c_uint32,
            structures.PCANBTP,
            structures.PCANBTP,
        ),
        __check_status,
    )
    # EXTERN_C HRESULT VCIAPI canControlClose( IN HANDLE hCanCtl );
    _canlib.map_symbol("canControlClose", hresult_type, (HANDLE,), __check_status)
    # EXTERN_C HRESULT VCIAPI canControlReset( IN HANDLE hCanCtl );
    _canlib.map_symbol("canControlReset", hresult_type, (HANDLE,), __check_status)
    # EXTERN_C HRESULT VCIAPI canControlStart( IN HANDLE hCanCtl, IN BOOL   fStart );
    _canlib.map_symbol(
        "canControlStart", hresult_type, (HANDLE, ctypes.c_long), __check_status
    )
    # EXTERN_C HRESULT VCIAPI canControlGetStatus( IN  HANDLE hCanCtl, OUT PCANLINESTATUS2 pStatus );
    _canlib.map_symbol(
        "canControlGetStatus",
        hresult_type,
        (HANDLE, structures.PCANLINESTATUS2),
        __check_status,
    )
    # EXTERN_C HRESULT VCIAPI canControlGetCaps( IN  HANDLE hCanCtl, OUT PCANCAPABILITIES2 pCanCaps );
    _canlib.map_symbol(
        "canControlGetCaps",
        hresult_type,
        (HANDLE, structures.PCANCAPABILITIES2),
        __check_status,
    )
    # EXTERN_C HRESULT VCIAPI canControlSetAccFilter( IN HANDLE hCanCtl, IN BOOL   fExtend, IN UINT32 dwCode, IN UINT32 dwMask );
    _canlib.map_symbol(
        "canControlSetAccFilter",
        hresult_type,
        (HANDLE, ctypes.c_int, ctypes.c_uint32, ctypes.c_uint32),
        __check_status,
    )
    # EXTERN_C HRESULT canControlAddFilterIds (HANDLE hControl, BOOL fExtended, UINT32 dwCode, UINT32 dwMask);
    _canlib.map_symbol(
        "canControlAddFilterIds",
        hresult_type,
        (HANDLE, ctypes.c_int, ctypes.c_uint32, ctypes.c_uint32),
        __check_status,
    )
    # EXTERN_C HRESULT canControlRemFilterIds (HANDLE hControl, BOOL fExtendend, UINT32 dwCode, UINT32 dwMask );
    _canlib.map_symbol(
        "canControlRemFilterIds",
        hresult_type,
        (HANDLE, ctypes.c_int, ctypes.c_uint32, ctypes.c_uint32),
        __check_status,
    )
    # EXTERN_C HRESULT canSchedulerOpen (HANDLE hDevice, UINT32 dwCanNo, PHANDLE phScheduler );
    _canlib.map_symbol(
        "canSchedulerOpen",
        hresult_type,
        (HANDLE, ctypes.c_uint32, PHANDLE),
        __check_status,
    )
    # EXTERN_C HRESULT canSchedulerClose (HANDLE hScheduler );
    _canlib.map_symbol("canSchedulerClose", hresult_type, (HANDLE,), __check_status)
    # EXTERN_C HRESULT canSchedulerGetCaps (HANDLE hScheduler, PCANCAPABILITIES2 pCaps );
    _canlib.map_symbol(
        "canSchedulerGetCaps",
        hresult_type,
        (HANDLE, structures.PCANCAPABILITIES2),
        __check_status,
    )
    # EXTERN_C HRESULT canSchedulerActivate ( HANDLE hScheduler, BOOL fEnable );
    _canlib.map_symbol(
        "canSchedulerActivate", hresult_type, (HANDLE, ctypes.c_int), __check_status
    )
    # EXTERN_C HRESULT canSchedulerAddMessage (HANDLE hScheduler, PCANCYCLICTXMSG2 pMessage, PUINT32 pdwIndex );
    _canlib.map_symbol(
        "canSchedulerAddMessage",
        hresult_type,
        (HANDLE, structures.PCANCYCLICTXMSG2, ctypes.POINTER(ctypes.c_uint32)),
        __check_status,
    )
    # EXTERN_C HRESULT canSchedulerRemMessage (HANDLE hScheduler, UINT32 dwIndex );
    _canlib.map_symbol(
        "canSchedulerRemMessage",
        hresult_type,
        (HANDLE, ctypes.c_uint32),
        __check_status,
    )
    # EXTERN_C HRESULT canSchedulerStartMessage (HANDLE hScheduler, UINT32 dwIndex, UINT16 dwCount );
    _canlib.map_symbol(
        "canSchedulerStartMessage",
        hresult_type,
        (HANDLE, ctypes.c_uint32, ctypes.c_uint16),
        __check_status,
    )
    # EXTERN_C HRESULT canSchedulerStopMessage (HANDLE hScheduler, UINT32 dwIndex );
    _canlib.map_symbol(
        "canSchedulerStopMessage",
        hresult_type,
        (HANDLE, ctypes.c_uint32),
        __check_status,
    )
    _canlib.vciInitialize()
except AttributeError:
    # In case _canlib == None meaning we're not on win32/no lib found
    pass
except Exception as e:
    log.warning("Could not initialize IXXAT VCI library: %s", e)
# ---------------------------------------------------------------------------


CAN_INFO_MESSAGES = {
    constants.CAN_INFO_START: "CAN started",
    constants.CAN_INFO_STOP: "CAN stopped",
    constants.CAN_INFO_RESET: "CAN reset",
}

CAN_ERROR_MESSAGES = {
    constants.CAN_ERROR_STUFF: "CAN bit stuff error",
    constants.CAN_ERROR_FORM: "CAN form error",
    constants.CAN_ERROR_ACK: "CAN acknowledgment error",
    constants.CAN_ERROR_BIT: "CAN bit error",
    constants.CAN_ERROR_CRC: "CAN CRC error",
    constants.CAN_ERROR_OTHER: "Other (unknown) CAN error",
}

CAN_STATUS_FLAGS = {
    constants.CAN_STATUS_TXPEND: "transmission pending",
    constants.CAN_STATUS_OVRRUN: "data overrun occurred",
    constants.CAN_STATUS_ERRLIM: "error warning limit exceeded",
    constants.CAN_STATUS_BUSOFF: "bus off",
    constants.CAN_STATUS_ININIT: "init mode active",
    constants.CAN_STATUS_BUSCERR: "bus coupling error",
}
# ----------------------------------------------------------------------------


class IXXATBus(BusABC):
    """The CAN Bus implemented for the IXXAT interface.

    .. warning::

        This interface does implement efficient filtering of messages, but
        the filters have to be set in ``__init__`` using the ``can_filters`` parameter.
        Using :meth:`~can.BusABC.set_filters` does not work.

    """

    @deprecated_args_alias(
        deprecation_start="4.0.0",
        deprecation_end="5.0.0",
        UniqueHardwareId="unique_hardware_id",
        rxFifoSize="rx_fifo_size",
        txFifoSize="tx_fifo_size",
    )
    def __init__(
        self,
        channel: int,
        can_filters=None,
        receive_own_messages: int = False,
        unique_hardware_id: Optional[int] = None,
        extended: bool = True,
        rx_fifo_size: int = 1024,
        tx_fifo_size: int = 128,
        bitrate: int = 500000,
        data_bitrate: int = 2000000,
        sjw_abr: Optional[int] = None,
        tseg1_abr: Optional[int] = None,
        tseg2_abr: Optional[int] = None,
        sjw_dbr: Optional[int] = None,
        tseg1_dbr: Optional[int] = None,
        tseg2_dbr: Optional[int] = None,
        ssp_dbr: Optional[int] = None,
        **kwargs,
    ):
        """
        :param channel:
            The Channel id to create this bus with.

        :param can_filters:
            See :meth:`can.BusABC.set_filters`.

        :param receive_own_messages:
            Enable self-reception of sent messages.

        :param unique_hardware_id:
            unique_hardware_id to connect (optional, will use the first found if not supplied)

        :param extended:
            Default True, enables the capability to use extended IDs.

        :param rx_fifo_size:
            Receive fifo size (default 1024)

        :param tx_fifo_size:
            Transmit fifo size (default 128)

        :param bitrate:
            Channel bitrate in bit/s

        :param data_bitrate:
            Channel bitrate in bit/s (only in CAN-Fd if baudrate switch enabled).

        :param sjw_abr:
            Bus timing value sample jump width (arbitration).

        :param tseg1_abr:
            Bus timing value tseg1 (arbitration)

        :param tseg2_abr:
            Bus timing value tseg2 (arbitration)

        :param sjw_dbr:
            Bus timing value sample jump width (data)

        :param tseg1_dbr:
            Bus timing value tseg1 (data). Only takes effect with fd and bitrate switch enabled.

        :param tseg2_dbr:
            Bus timing value tseg2 (data). Only takes effect with fd and bitrate switch enabled.

        :param ssp_dbr:
            Secondary sample point (data). Only takes effect with fd and bitrate switch enabled.

        """
        if _canlib is None:
            raise CanInterfaceNotImplementedError(
                "The IXXAT VCI library has not been initialized. Check the logs for more details."
            )
        log.info("CAN Filters: %s", can_filters)
        # Configuration options
        self._receive_own_messages = receive_own_messages
        # Usually comes as a string from the config file
        channel = int(channel)

        if bitrate not in constants.CAN_BITRATE_PRESETS and (
            tseg1_abr is None or tseg2_abr is None or sjw_abr is None
        ):
            raise ValueError(
                f"To use bitrate {bitrate} (that has not predefined preset) is mandatory "
                f"to use also parameters tseg1_abr, tseg2_abr and swj_abr"
            )
        if data_bitrate not in constants.CAN_DATABITRATE_PRESETS and (
            tseg1_dbr is None or tseg2_dbr is None or sjw_dbr is None
        ):
            raise ValueError(
                f"To use data_bitrate {data_bitrate} (that has not predefined preset) is mandatory "
                f"to use also parameters tseg1_dbr, tseg2_dbr and swj_dbr"
            )

        if rx_fifo_size <= 0:
            raise ValueError("rx_fifo_size must be > 0")

        if tx_fifo_size <= 0:
            raise ValueError("tx_fifo_size must be > 0")

        if channel < 0:
            raise ValueError("channel number must be >= 0")

        self._device_handle = HANDLE()
        self._device_info = structures.VCIDEVICEINFO()
        self._control_handle = HANDLE()
        self._channel_handle = HANDLE()
        self._channel_capabilities = structures.CANCAPABILITIES2()
        self._message = structures.CANMSG2()
        self._payload = (ctypes.c_byte * 64)()
        self._can_protocol = CanProtocol.CAN_FD

        # Search for supplied device
        if unique_hardware_id is None:
            log.info("Searching for first available device")
        else:
            log.info("Searching for unique HW ID %s", unique_hardware_id)
        _canlib.vciEnumDeviceOpen(ctypes.byref(self._device_handle))
        while True:
            try:
                _canlib.vciEnumDeviceNext(
                    self._device_handle, ctypes.byref(self._device_info)
                )
            except StopIteration:
                if unique_hardware_id is None:
                    raise VCIDeviceNotFoundError(
                        "No IXXAT device(s) connected or device(s) in use by other process(es)."
                    ) from None
                else:
                    raise VCIDeviceNotFoundError(
                        f"Unique HW ID {unique_hardware_id} not connected or not available."
                    ) from None
            else:
                if (unique_hardware_id is None) or (
                    self._device_info.UniqueHardwareId.AsChar
                    == bytes(unique_hardware_id, "ascii")
                ):
                    break
                else:
                    log.debug(
                        "Ignoring IXXAT with hardware id '%s'.",
                        self._device_info.UniqueHardwareId.AsChar.decode("ascii"),
                    )
        _canlib.vciEnumDeviceClose(self._device_handle)

        try:
            _canlib.vciDeviceOpen(
                ctypes.byref(self._device_info.VciObjectId),
                ctypes.byref(self._device_handle),
            )
        except Exception as exception:
            raise CanInitializationError(
                f"Could not open device: {exception}"
            ) from exception

        log.info("Using unique HW ID %s", self._device_info.UniqueHardwareId.AsChar)

        log.info(
            "Initializing channel %d in shared mode, %d rx buffers, %d tx buffers",
            channel,
            rx_fifo_size,
            tx_fifo_size,
        )

        try:
            _canlib.canChannelOpen(
                self._device_handle,
                channel,
                constants.FALSE,
                ctypes.byref(self._channel_handle),
            )
        except Exception as exception:
            raise CanInitializationError(
                f"Could not open and initialize channel: {exception}"
            ) from exception

        # Signal TX/RX events when at least one frame has been handled
        _canlib.canChannelInitialize(
            self._channel_handle,
            rx_fifo_size,
            1,
            tx_fifo_size,
            1,
            0,
            constants.CAN_FILTER_PASS,
        )
        _canlib.canChannelActivate(self._channel_handle, constants.TRUE)

        pBtpSDR = IXXATBus._canptb_build(
            defaults=constants.CAN_BITRATE_PRESETS,
            bitrate=bitrate,
            tseg1=tseg1_abr,
            tseg2=tseg2_abr,
            sjw=sjw_abr,
            ssp=0,
        )
        pBtpFDR = IXXATBus._canptb_build(
            defaults=constants.CAN_DATABITRATE_PRESETS,
            bitrate=data_bitrate,
            tseg1=tseg1_dbr,
            tseg2=tseg2_dbr,
            sjw=sjw_dbr,
            ssp=ssp_dbr if ssp_dbr is not None else tseg1_dbr,
        )

        log.info(
            "Initializing control %d with SDR={%s}, FDR={%s}",
            channel,
            pBtpSDR,
            pBtpFDR,
        )
        _canlib.canControlOpen(
            self._device_handle, channel, ctypes.byref(self._control_handle)
        )

        _canlib.canControlGetCaps(
            self._control_handle, ctypes.byref(self._channel_capabilities)
        )

        # check capabilities
        bOpMode = constants.CAN_OPMODE_UNDEFINED
        if (
            self._channel_capabilities.dwFeatures & constants.CAN_FEATURE_STDANDEXT
        ) != 0:
            # controller supportes CAN_OPMODE_STANDARD and CAN_OPMODE_EXTENDED at the same time
            bOpMode |= constants.CAN_OPMODE_STANDARD  # enable both 11 bits reception
            if extended:  # parameter from configuration
                bOpMode |= constants.CAN_OPMODE_EXTENDED  # enable 29 bits reception
        elif (
            self._channel_capabilities.dwFeatures & constants.CAN_FEATURE_STDANDEXT
        ) != 0:
            log.warning(
                "Channel %d capabilities allow either basic or extended IDs, but not both. using %s according to parameter [extended=%s]",
                channel,
                "extended" if extended else "basic",
                "True" if extended else "False",
            )
            bOpMode |= (
                constants.CAN_OPMODE_EXTENDED
                if extended
                else constants.CAN_OPMODE_STANDARD
            )

        if (
            self._channel_capabilities.dwFeatures & constants.CAN_FEATURE_ERRFRAME
        ) != 0:
            bOpMode |= constants.CAN_OPMODE_ERRFRAME

        bExMode = constants.CAN_EXMODE_DISABLED
        if (self._channel_capabilities.dwFeatures & constants.CAN_FEATURE_EXTDATA) != 0:
            bExMode |= constants.CAN_EXMODE_EXTDATALEN

        if (
            self._channel_capabilities.dwFeatures & constants.CAN_FEATURE_FASTDATA
        ) != 0:
            bExMode |= constants.CAN_EXMODE_FASTDATA

        _canlib.canControlInitialize(
            self._control_handle,
            bOpMode,
            bExMode,
            constants.CAN_FILTER_PASS,
            constants.CAN_FILTER_PASS,
            0,
            0,
            ctypes.byref(pBtpSDR),
            ctypes.byref(pBtpFDR),
        )

        # With receive messages, this field contains the relative reception time of
        # the message in ticks. The resolution of a tick can be calculated from the fields
        # dwClockFreq and dwTscDivisor of the structure  CANCAPABILITIES in accordance with the following formula:
        # frequency [1/s] = dwClockFreq / dwTscDivisor
        self._tick_resolution = (
            self._channel_capabilities.dwTscClkFreq
            / self._channel_capabilities.dwTscDivisor
        )

        # Setup filters before starting the channel
        if can_filters:
            log.info("The IXXAT VCI backend is filtering messages")
            # Disable every message coming in
            for extended in (0, 1):
                _canlib.canControlSetAccFilter(
                    self._control_handle,
                    extended,
                    constants.CAN_ACC_CODE_NONE,
                    constants.CAN_ACC_MASK_NONE,
                )
            for can_filter in can_filters:
                # Filters define what messages are accepted
                code = int(can_filter["can_id"])
                mask = int(can_filter["can_mask"])
                extended = can_filter.get("extended", False)
                _canlib.canControlAddFilterIds(
                    self._control_handle, 1 if extended else 0, code << 1, mask << 1
                )
                log.info("Accepting ID: 0x%X MASK: 0x%X", code, mask)

        # Start the CAN controller. Messages will be forwarded to the channel
        start_begin = time.time()
        _canlib.canControlStart(self._control_handle, constants.TRUE)
        start_end = time.time()

        # Calculate an offset to make them relative to epoch
        # Assume that the time offset is in the middle of the start command
        self._timeoffset = start_begin + (start_end - start_begin / 2)
        self._overrunticks = 0
        self._starttickoffset = 0

        # For cyclic transmit list. Set when .send_periodic() is first called
        self._scheduler = None
        self._scheduler_resolution = None
        self.channel = channel

        # Usually you get back 3 messages like "CAN initialized" ecc...
        # Clear the FIFO by filter them out with low timeout
        for _ in range(rx_fifo_size):
            try:
                _canlib.canChannelReadMessage(
                    self._channel_handle, 0, ctypes.byref(self._message)
                )
            except (VCITimeout, VCIRxQueueEmptyError):
                break

        super().__init__(channel=channel, can_filters=None, **kwargs)

    @staticmethod
    def _canptb_build(defaults, bitrate, tseg1, tseg2, sjw, ssp):
        if bitrate in defaults:
            d = defaults[bitrate]
            if tseg1 is None:
                tseg1 = d.wTS1
            if tseg2 is None:
                tseg2 = d.wTS2
            if sjw is None:
                sjw = d.wSJW
            if ssp is None:
                ssp = d.wTDO
            dw_mode = d.dwMode
        else:
            dw_mode = 0

        return structures.CANBTP(
            dwMode=dw_mode,
            dwBPS=bitrate,
            wTS1=tseg1,
            wTS2=tseg2,
            wSJW=sjw,
            wTDO=ssp,
        )

    def _inWaiting(self):
        try:
            _canlib.canChannelWaitRxEvent(self._channel_handle, 0)
        except VCITimeout:
            return 0
        else:
            return 1

    def flush_tx_buffer(self):
        """Flushes the transmit buffer on the IXXAT"""
        # TODO #64: no timeout?
        _canlib.canChannelWaitTxEvent(self._channel_handle, constants.INFINITE)

    def _recv_internal(self, timeout):
        """Read a message from IXXAT device."""

        # TODO: handling CAN error messages?
        data_received = False

        if timeout == 0:
            # Peek without waiting
            try:
                _canlib.canChannelPeekMessage(
                    self._channel_handle, ctypes.byref(self._message)
                )
            except (VCITimeout, VCIRxQueueEmptyError, VCIError):
                # VCIError means no frame available (canChannelPeekMessage returned different from zero)
                return None, True
            else:
                if self._message.uMsgInfo.Bits.type == constants.CAN_MSGTYPE_DATA:
                    data_received = True
        else:
            # Wait if no message available
            if timeout is None or timeout < 0:
                remaining_ms = constants.INFINITE
                t0 = None
            else:
                timeout_ms = int(timeout * 1000)
                remaining_ms = timeout_ms
                t0 = time.perf_counter()

            while True:
                try:
                    _canlib.canChannelReadMessage(
                        self._channel_handle, remaining_ms, ctypes.byref(self._message)
                    )
                except (VCITimeout, VCIRxQueueEmptyError):
                    # Ignore the 2 errors, the timeout is handled manually with the perf_counter()
                    pass
                else:
                    # See if we got a data or info/error messages
                    if self._message.uMsgInfo.Bits.type == constants.CAN_MSGTYPE_DATA:
                        data_received = True
                        break
                    elif self._message.uMsgInfo.Bits.type == constants.CAN_MSGTYPE_INFO:
                        log.info(
                            CAN_INFO_MESSAGES.get(
                                self._message.abData[0],
                                f"Unknown CAN info message code {self._message.abData[0]}",
                            )
                        )
                    # Handle CAN start info message
                    elif self._message.abData[0] == constants.CAN_INFO_START:
                        self._starttickoffset = self._message.dwTime
                    elif (
                        self._message.uMsgInfo.Bits.type == constants.CAN_MSGTYPE_ERROR
                    ):
                        log.warning(
                            CAN_ERROR_MESSAGES.get(
                                self._message.abData[0],
                                f"Unknown CAN error message code {self._message.abData[0]}",
                            )
                        )

                    elif (
                        self._message.uMsgInfo.Bits.type == constants.CAN_MSGTYPE_STATUS
                    ):
                        log.info(_format_can_status(self._message.abData[0]))
                        if self._message.abData[0] & constants.CAN_STATUS_BUSOFF:
                            raise VCIBusOffError()

                    elif (
                        self._message.uMsgInfo.Bits.type
                        == constants.CAN_MSGTYPE_TIMEOVR
                    ):
                        # Add the number of timestamp overruns to the high word
                        self._overrunticks += self._message.dwMsgId << 32
                    else:
                        log.warning("Unexpected message info type")

                if t0 is not None:
                    remaining_ms = timeout_ms - int((time.perf_counter() - t0) * 1000)
                    if remaining_ms < 0:
                        break

        if not data_received:
            # Timed out / can message type is not DATA
            return None, True

        data_len = dlc2len(self._message.uMsgInfo.Bits.dlc)
        rx_msg = Message(
            timestamp=(
                (self._message.dwTime + self._overrunticks - self._starttickoffset)
                / self._tick_resolution
            )
            + self._timeoffset,
            is_remote_frame=bool(self._message.uMsgInfo.Bits.rtr),
            is_fd=bool(self._message.uMsgInfo.Bits.edl),
            is_rx=True,
            is_error_frame=bool(
                self._message.uMsgInfo.Bits.type == constants.CAN_MSGTYPE_ERROR
            ),
            bitrate_switch=bool(self._message.uMsgInfo.Bits.fdr),
            error_state_indicator=bool(self._message.uMsgInfo.Bits.esi),
            is_extended_id=bool(self._message.uMsgInfo.Bits.ext),
            arbitration_id=self._message.dwMsgId,
            dlc=data_len,
            data=self._message.abData[:data_len],
            channel=self.channel,
        )

        return rx_msg, True

    def send(self, msg: Message, timeout: Optional[float] = None) -> None:
        """
        Sends a message on the bus. The interface may buffer the message.

        :param msg:
            The message to send.
        :param timeout:
            Timeout after some time.
        :raise:
            :class:CanTimeoutError
            :class:CanOperationError
        """
        # This system is not designed to be very efficient
        message = structures.CANMSG2()
        message.uMsgInfo.Bits.type = (
            constants.CAN_MSGTYPE_ERROR
            if msg.is_error_frame
            else constants.CAN_MSGTYPE_DATA
        )
        message.uMsgInfo.Bits.rtr = 1 if msg.is_remote_frame else 0
        message.uMsgInfo.Bits.ext = 1 if msg.is_extended_id else 0
        message.uMsgInfo.Bits.srr = 1 if self._receive_own_messages else 0
        message.uMsgInfo.Bits.fdr = 1 if msg.bitrate_switch else 0
        message.uMsgInfo.Bits.esi = 1 if msg.error_state_indicator else 0
        message.uMsgInfo.Bits.edl = 1 if msg.is_fd else 0
        message.dwMsgId = msg.arbitration_id
        if msg.dlc:  # this dlc means number of bytes of payload
            message.uMsgInfo.Bits.dlc = len2dlc(msg.dlc)
            data_len_dif = msg.dlc - len(msg.data)
            data = msg.data + bytearray(
                [0] * data_len_dif
            )  # pad with zeros until required length
            adapter = (ctypes.c_uint8 * msg.dlc).from_buffer(data)
            ctypes.memmove(message.abData, adapter, msg.dlc)

        if timeout:
            _canlib.canChannelSendMessage(
                self._channel_handle, int(timeout * 1000), message
            )

        else:
            _canlib.canChannelPostMessage(self._channel_handle, message)

    def _send_periodic_internal(
        self,
        msgs: Union[Sequence[Message], Message],
        period: float,
        duration: Optional[float] = None,
        autostart: bool = True,
        modifier_callback: Optional[Callable[[Message], None]] = None,
    ) -> CyclicSendTaskABC:
        """Send a message using built-in cyclic transmit list functionality."""
        if modifier_callback is None:
            if self._scheduler is None:
                self._scheduler = HANDLE()
                _canlib.canSchedulerOpen(
                    self._device_handle, self.channel, self._scheduler
                )
                caps = structures.CANCAPABILITIES2()
                _canlib.canSchedulerGetCaps(self._scheduler, caps)
                self._scheduler_resolution = (
                    caps.dwCmsClkFreq / caps.dwCmsDivisor
                )  # TODO: confirm
                _canlib.canSchedulerActivate(self._scheduler, constants.TRUE)
            return CyclicSendTask(
                self._scheduler,
                msgs,
                period,
                duration,
                self._scheduler_resolution,
                autostart=autostart,
            )

        # fallback to thread based cyclic task
        warnings.warn(
            f"{self.__class__.__name__} falls back to a thread-based cyclic task, "
            "when the `modifier_callback` argument is given.",
            stacklevel=3,
        )
        return BusABC._send_periodic_internal(
            self,
            msgs=msgs,
            period=period,
            duration=duration,
            autostart=autostart,
            modifier_callback=modifier_callback,
        )

    def shutdown(self):
        super().shutdown()
        if self._scheduler is not None:
            _canlib.canSchedulerClose(self._scheduler)
        _canlib.canChannelClose(self._channel_handle)
        _canlib.canControlStart(self._control_handle, constants.FALSE)
        _canlib.canControlClose(self._control_handle)
        _canlib.vciDeviceClose(self._device_handle)


class CyclicSendTask(LimitedDurationCyclicSendTaskABC, RestartableCyclicTaskABC):
    """A message in the cyclic transmit list."""

    def __init__(
        self,
        scheduler,
        msgs,
        period,
        duration,
        resolution,
        autostart: bool = True,
    ):
        super().__init__(msgs, period, duration)
        if len(self.messages) != 1:
            raise ValueError(
                "IXXAT Interface only supports periodic transmission of 1 element"
            )

        self._scheduler = scheduler
        self._index = None
        self._count = int(duration / period) if duration else 0

        self._msg = structures.CANCYCLICTXMSG2()
        self._msg.wCycleTime = round(period * resolution)
        self._msg.dwMsgId = self.messages[0].arbitration_id
        self._msg.uMsgInfo.Bits.type = constants.CAN_MSGTYPE_DATA
        self._msg.uMsgInfo.Bits.ext = 1 if self.messages[0].is_extended_id else 0
        self._msg.uMsgInfo.Bits.rtr = 1 if self.messages[0].is_remote_frame else 0
        self._msg.uMsgInfo.Bits.dlc = self.messages[0].dlc
        for i, b in enumerate(self.messages[0].data):
            self._msg.abData[i] = b
        if autostart:
            self.start()

    def start(self):
        """Start transmitting message (add to list if needed)."""
        if self._index is None:
            self._index = ctypes.c_uint32()
            _canlib.canSchedulerAddMessage(self._scheduler, self._msg, self._index)
        _canlib.canSchedulerStartMessage(self._scheduler, self._index, self._count)

    def pause(self):
        """Pause transmitting message (keep it in the list)."""
        _canlib.canSchedulerStopMessage(self._scheduler, self._index)

    def stop(self):
        """Stop transmitting message (remove from list)."""
        # Remove it completely instead of just stopping it to avoid filling up
        # the list with permanently stopped messages
        _canlib.canSchedulerRemMessage(self._scheduler, self._index)
        self._index = None


def _format_can_status(status_flags: int):
    """
    Format a status bitfield found in CAN_MSGTYPE_STATUS messages or in dwStatus
    field in CANLINESTATUS.

    Valid states are defined in the CAN_STATUS_* constants in cantype.h
    """
    states = []
    for flag, description in CAN_STATUS_FLAGS.items():
        if status_flags & flag:
            states.append(description)
            status_flags &= ~flag

    if status_flags:
        states.append(f"unknown state 0x{status_flags:02x}")

    if states:
        return "CAN status message: {}".format(", ".join(states))
    else:
        return "Empty CAN status message"


def get_ixxat_hwids():
    """Get a list of hardware ids of all available IXXAT devices."""
    hwids = []
    device_handle = HANDLE()
    device_info = structures.VCIDEVICEINFO()

    _canlib.vciEnumDeviceOpen(ctypes.byref(device_handle))
    while True:
        try:
            _canlib.vciEnumDeviceNext(device_handle, ctypes.byref(device_info))
        except StopIteration:
            break
        else:
            hwids.append(device_info.UniqueHardwareId.AsChar.decode("ascii"))
    _canlib.vciEnumDeviceClose(device_handle)

    return hwids
