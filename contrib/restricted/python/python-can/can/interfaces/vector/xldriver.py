# type: ignore
"""
Ctypes wrapper module for Vector CAN Interface on win32/win64 systems.

Authors: Julien Grave <grave.jul@gmail.com>, Christian Sandberg
"""

import ctypes
import logging
import platform
from ctypes.util import find_library

from . import xlclass
from .exceptions import VectorInitializationError, VectorOperationError

LOG = logging.getLogger(__name__)

# Load Windows DLL
DLL_NAME = "vxlapi64" if platform.architecture()[0] == "64bit" else "vxlapi"
if dll_path := find_library(DLL_NAME):
    _xlapi_dll = ctypes.windll.LoadLibrary(dll_path)
else:
    raise FileNotFoundError(f"Vector XL library not found: {DLL_NAME}")

# ctypes wrapping for API functions
xlGetErrorString = _xlapi_dll.xlGetErrorString
xlGetErrorString.argtypes = [xlclass.XLstatus]
xlGetErrorString.restype = xlclass.XLstringType


def check_status_operation(result, function, arguments):
    """Check the status and raise a :class:`VectorOperationError` on error."""
    if result > 0:
        raise VectorOperationError(
            result, xlGetErrorString(result).decode(), function.__name__
        )
    return result


def check_status_initialization(result, function, arguments):
    """Check the status and raise a :class:`VectorInitializationError` on error."""
    if result > 0:
        raise VectorInitializationError(
            result, xlGetErrorString(result).decode(), function.__name__
        )
    return result


xlGetDriverConfig = _xlapi_dll.xlGetDriverConfig
xlGetDriverConfig.argtypes = [ctypes.POINTER(xlclass.XLdriverConfig)]
xlGetDriverConfig.restype = xlclass.XLstatus
xlGetDriverConfig.errcheck = check_status_operation

xlOpenDriver = _xlapi_dll.xlOpenDriver
xlOpenDriver.argtypes = []
xlOpenDriver.restype = xlclass.XLstatus
xlOpenDriver.errcheck = check_status_initialization

xlCloseDriver = _xlapi_dll.xlCloseDriver
xlCloseDriver.argtypes = []
xlCloseDriver.restype = xlclass.XLstatus
xlCloseDriver.errcheck = check_status_operation

xlGetApplConfig = _xlapi_dll.xlGetApplConfig
xlGetApplConfig.argtypes = [
    ctypes.c_char_p,
    ctypes.c_uint,
    ctypes.POINTER(ctypes.c_uint),
    ctypes.POINTER(ctypes.c_uint),
    ctypes.POINTER(ctypes.c_uint),
    ctypes.c_uint,
]
xlGetApplConfig.restype = xlclass.XLstatus
xlGetApplConfig.errcheck = check_status_initialization

xlSetApplConfig = _xlapi_dll.xlSetApplConfig
xlSetApplConfig.argtypes = [
    ctypes.c_char_p,
    ctypes.c_uint,
    ctypes.c_uint,
    ctypes.c_uint,
    ctypes.c_uint,
    ctypes.c_uint,
]
xlSetApplConfig.restype = xlclass.XLstatus
xlSetApplConfig.errcheck = check_status_initialization

xlGetChannelIndex = _xlapi_dll.xlGetChannelIndex
xlGetChannelIndex.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int]
xlGetChannelIndex.restype = ctypes.c_int

xlGetChannelMask = _xlapi_dll.xlGetChannelMask
xlGetChannelMask.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int]
xlGetChannelMask.restype = xlclass.XLaccess

xlOpenPort = _xlapi_dll.xlOpenPort
xlOpenPort.argtypes = [
    ctypes.POINTER(xlclass.XLportHandle),
    ctypes.c_char_p,
    xlclass.XLaccess,
    ctypes.POINTER(xlclass.XLaccess),
    ctypes.c_uint,
    ctypes.c_uint,
    ctypes.c_uint,
]
xlOpenPort.restype = xlclass.XLstatus
xlOpenPort.errcheck = check_status_initialization

xlGetSyncTime = _xlapi_dll.xlGetSyncTime
xlGetSyncTime.argtypes = [xlclass.XLportHandle, ctypes.POINTER(xlclass.XLuint64)]
xlGetSyncTime.restype = xlclass.XLstatus
xlGetSyncTime.errcheck = check_status_initialization

xlGetChannelTime = _xlapi_dll.xlGetChannelTime
xlGetChannelTime.argtypes = [
    xlclass.XLportHandle,
    xlclass.XLaccess,
    ctypes.POINTER(xlclass.XLuint64),
]
xlGetChannelTime.restype = xlclass.XLstatus
xlGetChannelTime.errcheck = check_status_initialization

xlClosePort = _xlapi_dll.xlClosePort
xlClosePort.argtypes = [xlclass.XLportHandle]
xlClosePort.restype = xlclass.XLstatus
xlClosePort.errcheck = check_status_operation

xlSetNotification = _xlapi_dll.xlSetNotification
xlSetNotification.argtypes = [
    xlclass.XLportHandle,
    ctypes.POINTER(xlclass.XLhandle),
    ctypes.c_int,
]
xlSetNotification.restype = xlclass.XLstatus
xlSetNotification.errcheck = check_status_initialization

xlCanSetChannelMode = _xlapi_dll.xlCanSetChannelMode
xlCanSetChannelMode.argtypes = [
    xlclass.XLportHandle,
    xlclass.XLaccess,
    ctypes.c_int,
    ctypes.c_int,
]
xlCanSetChannelMode.restype = xlclass.XLstatus
xlCanSetChannelMode.errcheck = check_status_initialization

xlActivateChannel = _xlapi_dll.xlActivateChannel
xlActivateChannel.argtypes = [
    xlclass.XLportHandle,
    xlclass.XLaccess,
    ctypes.c_uint,
    ctypes.c_uint,
]
xlActivateChannel.restype = xlclass.XLstatus
xlActivateChannel.errcheck = check_status_operation

xlDeactivateChannel = _xlapi_dll.xlDeactivateChannel
xlDeactivateChannel.argtypes = [xlclass.XLportHandle, xlclass.XLaccess]
xlDeactivateChannel.restype = xlclass.XLstatus
xlDeactivateChannel.errcheck = check_status_operation

xlCanFdSetConfiguration = _xlapi_dll.xlCanFdSetConfiguration
xlCanFdSetConfiguration.argtypes = [
    xlclass.XLportHandle,
    xlclass.XLaccess,
    ctypes.POINTER(xlclass.XLcanFdConf),
]
xlCanFdSetConfiguration.restype = xlclass.XLstatus
xlCanFdSetConfiguration.errcheck = check_status_initialization

xlReceive = _xlapi_dll.xlReceive
xlReceive.argtypes = [
    xlclass.XLportHandle,
    ctypes.POINTER(ctypes.c_uint),
    ctypes.POINTER(xlclass.XLevent),
]
xlReceive.restype = xlclass.XLstatus
xlReceive.errcheck = check_status_operation

xlCanReceive = _xlapi_dll.xlCanReceive
xlCanReceive.argtypes = [xlclass.XLportHandle, ctypes.POINTER(xlclass.XLcanRxEvent)]
xlCanReceive.restype = xlclass.XLstatus
xlCanReceive.errcheck = check_status_operation

xlCanSetChannelBitrate = _xlapi_dll.xlCanSetChannelBitrate
xlCanSetChannelBitrate.argtypes = [
    xlclass.XLportHandle,
    xlclass.XLaccess,
    ctypes.c_ulong,
]
xlCanSetChannelBitrate.restype = xlclass.XLstatus
xlCanSetChannelBitrate.errcheck = check_status_initialization

xlCanSetChannelParams = _xlapi_dll.xlCanSetChannelParams
xlCanSetChannelParams.argtypes = [
    xlclass.XLportHandle,
    xlclass.XLaccess,
    ctypes.POINTER(xlclass.XLchipParams),
]
xlCanSetChannelParams.restype = xlclass.XLstatus
xlCanSetChannelParams.errcheck = check_status_initialization

xlCanSetChannelParamsC200 = _xlapi_dll.xlCanSetChannelParamsC200
xlCanSetChannelParamsC200.argtypes = [
    xlclass.XLportHandle,
    xlclass.XLaccess,
    ctypes.c_ubyte,
    ctypes.c_ubyte,
]
xlCanSetChannelParams.restype = xlclass.XLstatus
xlCanSetChannelParams.errcheck = check_status_initialization

xlCanTransmit = _xlapi_dll.xlCanTransmit
xlCanTransmit.argtypes = [
    xlclass.XLportHandle,
    xlclass.XLaccess,
    ctypes.POINTER(ctypes.c_uint),
    ctypes.POINTER(xlclass.XLevent),
]
xlCanTransmit.restype = xlclass.XLstatus
xlCanTransmit.errcheck = check_status_operation

xlCanTransmitEx = _xlapi_dll.xlCanTransmitEx
xlCanTransmitEx.argtypes = [
    xlclass.XLportHandle,
    xlclass.XLaccess,
    ctypes.c_uint,
    ctypes.POINTER(ctypes.c_uint),
    ctypes.POINTER(xlclass.XLcanTxEvent),
]
xlCanTransmitEx.restype = xlclass.XLstatus
xlCanTransmitEx.errcheck = check_status_operation

xlCanFlushTransmitQueue = _xlapi_dll.xlCanFlushTransmitQueue
xlCanFlushTransmitQueue.argtypes = [xlclass.XLportHandle, xlclass.XLaccess]
xlCanFlushTransmitQueue.restype = xlclass.XLstatus
xlCanFlushTransmitQueue.errcheck = check_status_operation

xlCanSetChannelAcceptance = _xlapi_dll.xlCanSetChannelAcceptance
xlCanSetChannelAcceptance.argtypes = [
    xlclass.XLportHandle,
    xlclass.XLaccess,
    ctypes.c_ulong,
    ctypes.c_ulong,
    ctypes.c_uint,
]
xlCanSetChannelAcceptance.restype = xlclass.XLstatus
xlCanSetChannelAcceptance.errcheck = check_status_operation

xlCanResetAcceptance = _xlapi_dll.xlCanResetAcceptance
xlCanResetAcceptance.argtypes = [xlclass.XLportHandle, xlclass.XLaccess, ctypes.c_uint]
xlCanResetAcceptance.restype = xlclass.XLstatus
xlCanResetAcceptance.errcheck = check_status_operation

xlCanRequestChipState = _xlapi_dll.xlCanRequestChipState
xlCanRequestChipState.argtypes = [xlclass.XLportHandle, xlclass.XLaccess]
xlCanRequestChipState.restype = xlclass.XLstatus
xlCanRequestChipState.errcheck = check_status_operation

xlCanSetChannelOutput = _xlapi_dll.xlCanSetChannelOutput
xlCanSetChannelOutput.argtypes = [xlclass.XLportHandle, xlclass.XLaccess, ctypes.c_char]
xlCanSetChannelOutput.restype = xlclass.XLstatus
xlCanSetChannelOutput.errcheck = check_status_operation

xlPopupHwConfig = _xlapi_dll.xlPopupHwConfig
xlPopupHwConfig.argtypes = [ctypes.c_char_p, ctypes.c_uint]
xlPopupHwConfig.restype = xlclass.XLstatus
xlPopupHwConfig.errcheck = check_status_operation

xlSetTimerRate = _xlapi_dll.xlSetTimerRate
xlSetTimerRate.argtypes = [xlclass.XLportHandle, ctypes.c_ulong]
xlSetTimerRate.restype = xlclass.XLstatus
xlSetTimerRate.errcheck = check_status_operation

xlGetEventString = _xlapi_dll.xlGetEventString
xlGetEventString.argtypes = [ctypes.POINTER(xlclass.XLevent)]
xlGetEventString.restype = xlclass.XLstringType

xlCanGetEventString = _xlapi_dll.xlCanGetEventString
xlCanGetEventString.argtypes = [ctypes.POINTER(xlclass.XLcanRxEvent)]
xlCanGetEventString.restype = xlclass.XLstringType

xlGetReceiveQueueLevel = _xlapi_dll.xlGetReceiveQueueLevel
xlGetReceiveQueueLevel.argtypes = [xlclass.XLportHandle, ctypes.POINTER(ctypes.c_int)]
xlGetReceiveQueueLevel.restype = xlclass.XLstatus
xlGetReceiveQueueLevel.errcheck = check_status_operation

xlGenerateSyncPulse = _xlapi_dll.xlGenerateSyncPulse
xlGenerateSyncPulse.argtypes = [xlclass.XLportHandle, xlclass.XLaccess]
xlGenerateSyncPulse.restype = xlclass.XLstatus
xlGenerateSyncPulse.errcheck = check_status_operation

xlFlushReceiveQueue = _xlapi_dll.xlFlushReceiveQueue
xlFlushReceiveQueue.argtypes = [xlclass.XLportHandle]
xlFlushReceiveQueue.restype = xlclass.XLstatus
xlFlushReceiveQueue.errcheck = check_status_operation
