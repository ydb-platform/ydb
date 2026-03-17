"""
Definition of data types and structures for vxlapi.

Authors: Julien Grave <grave.jul@gmail.com>, Christian Sandberg
"""

# Import Standard Python Modules
# ==============================
import ctypes

# Vector XL API Definitions
# =========================
from . import xldefine

XLuint64 = ctypes.c_int64
XLaccess = XLuint64
XLhandle = ctypes.c_void_p
XLstatus = ctypes.c_short
XLportHandle = ctypes.c_long
XLeventTag = ctypes.c_ubyte
XLstringType = ctypes.c_char_p


# structure for XL_RECEIVE_MSG, XL_TRANSMIT_MSG
class s_xl_can_msg(ctypes.Structure):
    _fields_ = [
        ("id", ctypes.c_ulong),
        ("flags", ctypes.c_ushort),
        ("dlc", ctypes.c_ushort),
        ("res1", XLuint64),
        ("data", ctypes.c_ubyte * xldefine.MAX_MSG_LEN),
        ("res2", XLuint64),
    ]


class s_xl_can_ev_error(ctypes.Structure):
    _fields_ = [("errorCode", ctypes.c_ubyte), ("reserved", ctypes.c_ubyte * 95)]


class s_xl_chip_state(ctypes.Structure):
    _fields_ = [
        ("busStatus", ctypes.c_ubyte),
        ("txErrorCounter", ctypes.c_ubyte),
        ("rxErrorCounter", ctypes.c_ubyte),
    ]


class s_xl_sync_pulse(ctypes.Structure):
    _fields_ = [
        ("pulseCode", ctypes.c_ubyte),
        ("time", XLuint64),
    ]


class s_xl_can_ev_chip_state(ctypes.Structure):
    _fields_ = [
        ("busStatus", ctypes.c_ubyte),
        ("txErrorCounter", ctypes.c_ubyte),
        ("rxErrorCounter", ctypes.c_ubyte),
        ("reserved", ctypes.c_ubyte),
        ("reserved0", ctypes.c_uint),
    ]


class s_xl_can_ev_sync_pulse(ctypes.Structure):
    _fields_ = [
        ("triggerSource", ctypes.c_uint),
        ("reserved", ctypes.c_uint),
        ("time", XLuint64),
    ]


# BASIC bus message structure
class s_xl_tag_data(ctypes.Union):
    _fields_ = [
        ("msg", s_xl_can_msg),
        ("chipState", s_xl_chip_state),
        ("syncPulse", s_xl_sync_pulse),
    ]


# CAN FD messages
class s_xl_can_ev_rx_msg(ctypes.Structure):
    _fields_ = [
        ("canId", ctypes.c_uint),
        ("msgFlags", ctypes.c_uint),
        ("crc", ctypes.c_uint),
        ("reserved1", ctypes.c_ubyte * 12),
        ("totalBitCnt", ctypes.c_ushort),
        ("dlc", ctypes.c_ubyte),
        ("reserved", ctypes.c_ubyte * 5),
        ("data", ctypes.c_ubyte * xldefine.XL_CAN_MAX_DATA_LEN),
    ]


class s_xl_can_ev_tx_request(ctypes.Structure):
    _fields_ = [
        ("canId", ctypes.c_uint),
        ("msgFlags", ctypes.c_uint),
        ("dlc", ctypes.c_ubyte),
        ("txAttemptConf", ctypes.c_ubyte),
        ("reserved", ctypes.c_ushort),
        ("data", ctypes.c_ubyte * xldefine.XL_CAN_MAX_DATA_LEN),
    ]


class s_xl_can_tx_msg(ctypes.Structure):
    _fields_ = [
        ("canId", ctypes.c_uint),
        ("msgFlags", ctypes.c_uint),
        ("dlc", ctypes.c_ubyte),
        ("reserved", ctypes.c_ubyte * 7),
        ("data", ctypes.c_ubyte * xldefine.XL_CAN_MAX_DATA_LEN),
    ]


class s_rxTagData(ctypes.Union):
    _fields_ = [
        ("canRxOkMsg", s_xl_can_ev_rx_msg),
        ("canTxOkMsg", s_xl_can_ev_rx_msg),
        ("canTxRequest", s_xl_can_ev_tx_request),
        ("canError", s_xl_can_ev_error),
        ("canChipState", s_xl_can_ev_chip_state),
        ("canSyncPulse", s_xl_can_ev_sync_pulse),
    ]


class s_txTagData(ctypes.Union):
    _fields_ = [("canMsg", s_xl_can_tx_msg)]


class XLevent(ctypes.Structure):
    _fields_ = [
        ("tag", XLeventTag),
        ("chanIndex", ctypes.c_ubyte),
        ("transId", ctypes.c_ushort),
        ("portHandle", ctypes.c_ushort),
        ("flags", ctypes.c_ubyte),
        ("reserved", ctypes.c_ubyte),
        ("timeStamp", XLuint64),
        ("tagData", s_xl_tag_data),
    ]


# CAN FD events
class XLcanRxEvent(ctypes.Structure):
    _fields_ = [
        ("size", ctypes.c_int),
        ("tag", ctypes.c_ushort),
        ("chanIndex", ctypes.c_ubyte),
        ("reserved", ctypes.c_ubyte),
        ("userHandle", ctypes.c_int),
        ("flagsChip", ctypes.c_ushort),
        ("reserved0", ctypes.c_ushort),
        ("reserved1", XLuint64),
        ("timeStamp", XLuint64),
        ("tagData", s_rxTagData),
    ]


class XLcanTxEvent(ctypes.Structure):
    _fields_ = [
        ("tag", ctypes.c_ushort),
        ("transId", ctypes.c_ushort),
        ("chanIndex", ctypes.c_ubyte),
        ("reserved", ctypes.c_ubyte * 3),
        ("tagData", s_txTagData),
    ]


# CAN configuration structure
class XLchipParams(ctypes.Structure):
    _fields_ = [
        ("bitRate", ctypes.c_ulong),
        ("sjw", ctypes.c_ubyte),
        ("tseg1", ctypes.c_ubyte),
        ("tseg2", ctypes.c_ubyte),
        ("sam", ctypes.c_ubyte),
    ]


# CAN FD configuration structure
class XLcanFdConf(ctypes.Structure):
    _fields_ = [
        ("arbitrationBitRate", ctypes.c_uint),
        ("sjwAbr", ctypes.c_uint),
        ("tseg1Abr", ctypes.c_uint),
        ("tseg2Abr", ctypes.c_uint),
        ("dataBitRate", ctypes.c_uint),
        ("sjwDbr", ctypes.c_uint),
        ("tseg1Dbr", ctypes.c_uint),
        ("tseg2Dbr", ctypes.c_uint),
        ("reserved", ctypes.c_ubyte),
        ("options", ctypes.c_ubyte),
        ("reserved1", ctypes.c_ubyte * 2),
        ("reserved2", ctypes.c_ubyte),
    ]


# channel configuration structures
class s_xl_bus_params_data_can(ctypes.Structure):
    _fields_ = [
        ("bitRate", ctypes.c_uint),
        ("sjw", ctypes.c_ubyte),
        ("tseg1", ctypes.c_ubyte),
        ("tseg2", ctypes.c_ubyte),
        ("sam", ctypes.c_ubyte),
        ("outputMode", ctypes.c_ubyte),
        ("reserved", ctypes.c_ubyte * 7),
        ("canOpMode", ctypes.c_ubyte),
    ]


class s_xl_bus_params_data_canfd(ctypes.Structure):
    _fields_ = [
        ("arbitrationBitRate", ctypes.c_uint),
        ("sjwAbr", ctypes.c_ubyte),
        ("tseg1Abr", ctypes.c_ubyte),
        ("tseg2Abr", ctypes.c_ubyte),
        ("samAbr", ctypes.c_ubyte),
        ("outputMode", ctypes.c_ubyte),
        ("sjwDbr", ctypes.c_ubyte),
        ("tseg1Dbr", ctypes.c_ubyte),
        ("tseg2Dbr", ctypes.c_ubyte),
        ("dataBitRate", ctypes.c_uint),
        ("canOpMode", ctypes.c_ubyte),
    ]


class s_xl_bus_params_data(ctypes.Union):
    _fields_ = [
        ("can", s_xl_bus_params_data_can),
        ("canFD", s_xl_bus_params_data_canfd),
        ("most", ctypes.c_ubyte * 12),
        ("flexray", ctypes.c_ubyte * 12),
        ("ethernet", ctypes.c_ubyte * 12),
        ("a429", ctypes.c_ubyte * 28),
    ]


class XLbusParams(ctypes.Structure):
    _fields_ = [("busType", ctypes.c_uint), ("data", s_xl_bus_params_data)]


class XLchannelConfig(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("name", ctypes.c_char * 32),
        ("hwType", ctypes.c_ubyte),
        ("hwIndex", ctypes.c_ubyte),
        ("hwChannel", ctypes.c_ubyte),
        ("transceiverType", ctypes.c_ushort),
        ("transceiverState", ctypes.c_ushort),
        ("configError", ctypes.c_ushort),
        ("channelIndex", ctypes.c_ubyte),
        ("channelMask", XLuint64),
        ("channelCapabilities", ctypes.c_uint),
        ("channelBusCapabilities", ctypes.c_uint),
        ("isOnBus", ctypes.c_ubyte),
        ("connectedBusType", ctypes.c_uint),
        ("busParams", XLbusParams),
        ("_doNotUse", ctypes.c_uint),
        ("driverVersion", ctypes.c_uint),
        ("interfaceVersion", ctypes.c_uint),
        ("raw_data", ctypes.c_uint * 10),
        ("serialNumber", ctypes.c_uint),
        ("articleNumber", ctypes.c_uint),
        ("transceiverName", ctypes.c_char * 32),
        ("specialCabFlags", ctypes.c_uint),
        ("dominantTimeout", ctypes.c_uint),
        ("dominantRecessiveDelay", ctypes.c_ubyte),
        ("recessiveDominantDelay", ctypes.c_ubyte),
        ("connectionInfo", ctypes.c_ubyte),
        ("currentlyAvailableTimestamps", ctypes.c_ubyte),
        ("minimalSupplyVoltage", ctypes.c_ushort),
        ("maximalSupplyVoltage", ctypes.c_ushort),
        ("maximalBaudrate", ctypes.c_uint),
        ("fpgaCoreCapabilities", ctypes.c_ubyte),
        ("specialDeviceStatus", ctypes.c_ubyte),
        ("channelBusActiveCapabilities", ctypes.c_ushort),
        ("breakOffset", ctypes.c_ushort),
        ("delimiterOffset", ctypes.c_ushort),
        ("reserved", ctypes.c_uint * 3),
    ]


class XLdriverConfig(ctypes.Structure):
    _fields_ = [
        ("dllVersion", ctypes.c_uint),
        ("channelCount", ctypes.c_uint),
        ("reserved", ctypes.c_uint * 10),
        ("channel", XLchannelConfig * 64),
    ]
