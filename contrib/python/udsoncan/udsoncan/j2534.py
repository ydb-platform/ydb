import ctypes
from ctypes import Structure, WINFUNCTYPE, POINTER, cast, c_long, c_void_p, c_ulong, byref  # type: ignore

from enum import Enum

import logging


class PASSTHRU_MSG(Structure):
    _fields_ = [("ProtocolID", c_ulong),
                ("RxStatus", c_ulong),
                ("TxFlags", c_ulong),
                ("Timestamp", c_ulong),
                ("DataSize", c_ulong),
                ("ExtraDataIndex", c_ulong),
                ("Data", ctypes.c_ubyte * 4128)]


class SCONFIG(Structure):
    _fields_ = [("Parameter", c_ulong),
                ("Value", c_ulong)]


class SCONFIG_LIST(Structure):
    _fields_ = [("NumOfParams", c_ulong),
                ("ConfigPtr", POINTER(SCONFIG))]

    def __init__(self, values):
        self.NumOfParams = len(values)
        self.ConfigPtr = (SCONFIG * self.NumOfParams)(*values)


class J2534():
    dllPassThruOpen = None
    dllPassThruClose = None
    dllPassThruConnect = None
    dllPassThruDisconnect = None
    dllPassThruReadMsgs = None
    dllPassThruWriteMsgs = None
    dllPassThruStartPeriodicMsg = None
    dllPassThruStopPeriodicMsg = None
    dllPassThruReadVersion = None
    dllPassThruGetLastError = None
    dllPassThruStartMsgFilter = None
    dllPassThruIoctl = None

    def __init__(self, windll, rxid, txid):
        global dllPassThruOpen
        global dllPassThruClose
        global dllPassThruConnect
        global dllPassThruDisconnect
        global dllPassThruReadMsgs
        global dllPassThruWriteMsgs
        global dllPassThruStartPeriodicMsg
        global dllPassThruStopPeriodicMsg
        global dllPassThruReadVersion
        global dllPassThruGetLastError
        global dllPassThruStartMsgFilter
        global dllPassThruIoctl

        self.hDLL = ctypes.cdll.LoadLibrary(windll)
        self.rxid = rxid.to_bytes(4, 'big')
        self.txid = txid.to_bytes(4, 'big')
        self.txFlags = TxFlags.ISO15765_FRAME_PAD.value
        self.connectFlags = ConnectFlags.NONE.value
        # Determine mode ID29 or ID11
        if txid >> 11:
            self.txFlags |= TxFlags.CAN_29_BIT_ID.value
            self.connectFlags |= ConnectFlags.CAN_29_BIT_ID.value

        self.logger = logging.getLogger()

        dllPassThruOpenProto = WINFUNCTYPE(
            c_long,
            c_void_p,
            POINTER(c_ulong))

        dllPassThruOpenParams = (1, "pName", 0), (1, "pDeviceID", 0)
        dllPassThruOpen = dllPassThruOpenProto(("PassThruOpen", self.hDLL), dllPassThruOpenParams)

        dllPassThruCloseProto = WINFUNCTYPE(
            c_long,
            c_ulong)

        dllPassThruCloseParams = (1, "DeviceID", 0),
        dllPassThruClose = dllPassThruCloseProto(("PassThruClose", self.hDLL), dllPassThruCloseParams)

        dllPassThruConnectProto = WINFUNCTYPE(
            c_long,
            c_ulong,
            c_ulong,
            c_ulong,
            c_ulong,
            POINTER(c_ulong))

        dllPassThruConnectParams = (1, "DeviceID", 0), (1, "ProtocolID", 0), (1, "Flags", 0), (1, "BaudRate", 500000), (1, "pChannelID", 0)
        dllPassThruConnect = dllPassThruConnectProto(("PassThruConnect", self.hDLL), dllPassThruConnectParams)

        dllPassThruDisconnectProto = WINFUNCTYPE(
            c_long,
            c_ulong)

        dllPassThruDisconnectParams = (1, "ChannelID", 0),
        dllPassThruDisconnect = dllPassThruDisconnectProto(("PassThruDisconnect", self.hDLL), dllPassThruDisconnectParams)

        dllPassThruReadMsgsProto = WINFUNCTYPE(
            c_long,
            c_ulong,
            POINTER(PASSTHRU_MSG),
            POINTER(c_ulong),
            c_ulong)

        dllPassThruReadMsgsParams = (1, "ChannelID", 0), (1, "pMsg", 0), (1, "pNumMsgs", 0), (1, "Timeout", 0)
        dllPassThruReadMsgs = dllPassThruReadMsgsProto(("PassThruReadMsgs", self.hDLL), dllPassThruReadMsgsParams)

        dllPassThruWriteMsgsProto = WINFUNCTYPE(
            c_long,
            c_ulong,
            POINTER(PASSTHRU_MSG),
            POINTER(c_ulong),
            c_ulong)

        dllPassThruWriteMsgsParams = (1, "ChannelID", 0), (1, "pMsg", 0), (1, "pNumMsgs", 0), (1, "Timeout", 0)
        dllPassThruWriteMsgs = dllPassThruWriteMsgsProto(("PassThruWriteMsgs", self.hDLL), dllPassThruWriteMsgsParams)

        dllPassThruStartPeriodicMsgProto = WINFUNCTYPE(
            c_long,
            c_ulong,
            POINTER(PASSTHRU_MSG),
            POINTER(c_ulong),
            c_ulong)

        dllPassThruStartPeriodicMsgParams = (1, "ChannelID", 0), (1, "pMsg", 0), (1, "pMsgID", 0), (1, "TimeInterval", 0)
        dllPassThruStartPeriodicMsg = dllPassThruStartPeriodicMsgProto(("PassThruStartPeriodicMsg", self.hDLL), dllPassThruStartPeriodicMsgParams)

        dllPassThruStopPeriodicMsgProto = WINFUNCTYPE(
            c_long,
            c_ulong,
            c_ulong)

        dllPassThruStopPeriodicMsgParams = (1, "ChannelID", 0), (1, "MsgID", 0)
        dllPassThruStopPeriodicMsg = dllPassThruStopPeriodicMsgProto(("PassThruStopPeriodicMsg", self.hDLL), dllPassThruStopPeriodicMsgParams)

        dllPassThruReadVersionProto = WINFUNCTYPE(
            c_long,
            c_ulong,
            POINTER(ctypes.c_char),
            POINTER(ctypes.c_char),
            POINTER(ctypes.c_char))

        dllPassThruReadVersionParams = (1, "DeviceID", 0), (1, "pFirmwareVersion", 0), (1, "pDllVersion", 0), (1, "pApiVersoin", 0)
        dllPassThruReadVersion = dllPassThruReadVersionProto(("PassThruReadVersion", self.hDLL), dllPassThruReadVersionParams)

        dllPassThruGetLastErrorProto = WINFUNCTYPE(
            c_long,
            POINTER(ctypes.c_char),
        )
        dllPassThruGetLastErrorParams = (1, "pErrorDescription", 0),
        dllPassThruGetLastError = dllPassThruGetLastErrorProto(("PassThruGetLastError", self.hDLL), dllPassThruGetLastErrorParams)

        dllPassThruStartMsgFilterProto = WINFUNCTYPE(
            c_long,
            c_ulong,
            c_ulong,
            POINTER(PASSTHRU_MSG),
            POINTER(PASSTHRU_MSG),
            POINTER(PASSTHRU_MSG),
            POINTER(c_ulong)
        )

        dllPassThruStartMsgFilterParams = (1,"ChannelID",0), (1,"FilterType",0),(1,"pMaskMsg",0),(1,"pPatternMsg",0),(1,"pFlowControlMsg",0),(1,"pMsgID",0)

        dllPassThruStartMsgFilter = dllPassThruStartMsgFilterProto(("PassThruStartMsgFilter", self.hDLL), dllPassThruStartMsgFilterParams)

        dllPassThruIoctlProto = WINFUNCTYPE(
            c_long,
            c_ulong,
            c_ulong,
            c_void_p,
            c_void_p
        )

        dllPassThruIoctlParams = (1, "Handle", 0), (1, "IoctlID", 0), (1, "pInput", 0), (1, "pOutput", 0)

        dllPassThruIoctl = dllPassThruIoctlProto(("PassThruIoctl", self.hDLL), dllPassThruIoctlParams)

    def PassThruOpen(self, pDeviceID=None):
        if not pDeviceID:
            pDeviceID = ctypes.c_ulong()

        result = dllPassThruOpen(bytes('J2534-2:', 'ascii'), byref(pDeviceID))
        return Error_ID(hex(result)), pDeviceID

    def PassThruConnect(self, deviceID, protocol, baudrate, pChannelID=None):
        if not pChannelID:
            pChannelID = c_ulong()

        result = dllPassThruConnect(deviceID, protocol, self.connectFlags, baudrate, byref(pChannelID))
        return Error_ID(hex(result)), pChannelID

    def PassThruClose(self, DeviceID):
        result = dllPassThruClose(DeviceID)
        return Error_ID(hex(result))

    def PassThruDisconnect(self, ChannelID):
        result = dllPassThruDisconnect(ChannelID)
        return Error_ID(hex(result))

    def PassThruReadMsgs(self, ChannelID, protocol, pNumMsgs=1, Timeout=20):
        pMsg = PASSTHRU_MSG()
        pMsg.ProtocolID = protocol

        pNumMsgs = c_ulong(pNumMsgs)

        while 1:
            # breakpoint()
            result = dllPassThruReadMsgs(ChannelID, byref(pMsg), byref(pNumMsgs), c_ulong(Timeout))
            if hex(result) == Error_ID.ERR_BUFFER_EMPTY.value or pNumMsgs == 0:
                return None, None, 0

            if pMsg.RxStatus & (RxStatus.TX_INDICATION.value | RxStatus.TX_MSG_TYPE.value | RxStatus.START_OF_MESSAGE.value):
                continue

            return Error_ID(hex(result)), bytes(pMsg.Data[4:pMsg.DataSize]), pNumMsgs

    def PassThruWriteMsgs(self, ChannelID, Data, protocol, pNumMsgs=1, Timeout=1000):
        txmsg = PASSTHRU_MSG()
        txmsg.TxFlags = self.txFlags
        txmsg.ProtocolID = protocol

        Data = self.txid + Data
        self.logger.info("Sending data: " + str(Data.hex()))

        for i in range(0, len(Data)):
            txmsg.Data[i] = Data[i]

        txmsg.DataSize = len(Data)

        result = dllPassThruWriteMsgs(ChannelID, byref(txmsg), byref(c_ulong(pNumMsgs)), c_ulong(Timeout))

        return Error_ID(hex(result))

    def PassThruStartPeriodicMsg(self, ChannelID, Data, MsgID=0, TimeInterval=100):
        pMsg = PASSTHRU_MSG()

        pMsg.Data = Data
        pMsg.DataSize = len(Data)

        result = dllPassThruStartPeriodicMsg(ChannelID, byref(pMsg), byref(c_ulong(MsgID)), c_ulong(TimeInterval))

        return Error_ID(hex(result))

    def PassThruStopPeriodicMsg(self, ChannelID, MsgID):
        result = dllPassThruStopPeriodicMsg(ChannelID, MsgID)

        return Error_ID(hex(result))

    def PassThruReadVersion(self, DeviceID):
        pFirmwareVersion = (ctypes.c_char * 80)()
        pDllVersion = (ctypes.c_char * 80)()
        pApiVersion = (ctypes.c_char * 80)()
        result = dllPassThruReadVersion(DeviceID, pFirmwareVersion, pDllVersion, pApiVersion)

        return Error_ID(hex(result)), pFirmwareVersion, pDllVersion, pApiVersion

    def PassThruGetLastError(self):
        pErrorDescription = (ctypes.c_char * 80)()
        result = dllPassThruGetLastError(pErrorDescription)

        return Error_ID(hex(result)), pErrorDescription.value.decode()

    def PassThruIoctl(self, Handle, IoctlID, ioctlInput=None, ioctlOutput=None):
        pInput = None if ioctlInput is None else byref(ioctlInput)
        pOutput = None if ioctlOutput is None else byref(ioctlOutput)

        result = dllPassThruIoctl(Handle, c_ulong(IoctlID.value), pInput, pOutput)

        return Error_ID(hex(result))

    def PassThruStartMsgFilter(self, ChannelID, protocol):
        msgMask = PASSTHRU_MSG()
        msgMask.ProtocolID = protocol
        msgMask.TxFlags = self.txFlags
        msgMask.DataSize = 4
        msgMask.RxStatus = msgMask.ExtraDataIndex = 0xCCCC_CCCC
        for i in range(0, 4):
            msgMask.Data[i] = 0xFF

        msgPattern = PASSTHRU_MSG()
        msgPattern.ProtocolID = protocol
        msgPattern.TxFlags = self.txFlags
        msgPattern.DataSize = 4
        msgPattern.RxStatus = msgPattern.ExtraDataIndex = 0xCCCC_CCCC
        for i in range(0, len(self.rxid)):
            msgPattern.Data[i] = self.rxid[i]

        if protocol in [Protocol_ID.ISO9141.value, Protocol_ID.ISO14230.value]:
            filterType = c_ulong(Filter.PASS_FILTER.value)
            msgFlow = None
        else:
            filterType = c_ulong(Filter.FLOW_CONTROL_FILTER.value)
            msgFlow = PASSTHRU_MSG()
            msgFlow.ProtocolID = protocol
            msgFlow.TxFlags = self.txFlags
            msgFlow.DataSize = 4
            msgFlow.RxStatus = msgFlow.ExtraDataIndex = 0xCCCC_CCCC
            for i in range(0, len(self.txid)):
                msgFlow.Data[i] = self.txid[i]
            msgFlow = byref(msgFlow)

        msgID = c_ulong(0)

        result = dllPassThruStartMsgFilter(ChannelID, filterType, byref(msgMask), byref(msgPattern), msgFlow, byref(msgID))

        return Error_ID(hex(result))


class Error_ID(Enum):
    ERR_SUCCESS = hex(0x00)
    STATUS_NOERROR = hex(0x00)
    ERR_NOT_SUPPORTED = hex(0x01)
    ERR_INVALID_CHANNEL_ID = hex(0x02)
    ERR_INVALID_PROTOCOL_ID = hex(0x03)
    ERR_NULL_PARAMETER = hex(0x04)
    ERR_INVALID_IOCTL_VALUE = hex(0x05)
    ERR_INVALID_FLAGS = hex(0x06)
    ERR_FAILED = hex(0x07)
    ERR_DEVICE_NOT_CONNECTED = hex(0x08)
    ERR_TIMEOUT = hex(0x09)
    ERR_INVALID_MSG = hex(0x0A)
    ERR_INVALID_TIME_INTERVAL = hex(0x0B)
    ERR_EXCEEDED_LIMIT = hex(0x0C)
    ERR_INVALID_MSG_ID = hex(0x0D)
    ERR_DEVICE_IN_USE = hex(0x0E)
    ERR_INVALID_IOCTL_ID = hex(0x0F)
    ERR_BUFFER_EMPTY = hex(0x10)
    ERR_BUFFER_FULL = hex(0x11)
    ERR_BUFFER_OVERFLOW = hex(0x12)
    ERR_PIN_INVALID = hex(0x13)
    ERR_CHANNEL_IN_USE = hex(0x14)
    ERR_MSG_PROTOCOL_ID = hex(0x15)
    ERR_INVALID_FILTER_ID = hex(0x16)
    ERR_NO_FLOW_CONTROL = hex(0x17)
    ERR_NOT_UNIQUE = hex(0x18)
    ERR_INVALID_BAUDRATE = hex(0x19)
    ERR_INVALID_DEVICE_ID = hex(0x1A)


class Protocol_ID(Enum):
    J1850VPW = 1
    J1850PWM = 2
    ISO9141 = 3
    ISO14230 = 4
    CAN = 5
    ISO15765 = 6
    SCI_A_ENGINE = 7  # OP2.0: Not supported
    SCI_A_TRANS = 8  # OP2.0: Not supported
    SCI_B_ENGINE = 9  # OP2.0: Not supported
    SCI_B_TRANS = 10  # OP2.0: Not supported


class Filter(Enum):
    PASS_FILTER = 0x00000001
    BLOCK_FILTER = 0x00000002
    FLOW_CONTROL_FILTER = 0x00000003


class ConnectFlags(Enum):
    NONE = 0
    CAN_29_BIT_ID = 0x100
    ISO9141_NO_CHECKSUM = 0x200
    CAN_ID_BOTH = 0x800
    ISO9141_K_LINE_ONLY = 0x1000


class TxFlags(Enum):
    NONE = 0
    # 0 = no padding
    # 1 = pad all flow controlled messages to a full CAN frame using zeroes
    ISO15765_FRAME_PAD = 0x00000040

    ISO15765_ADDR_TYPE = 0x00000080
    CAN_29_BIT_ID = 0x00000100

    # 0 = Interface message timing as specified in ISO 14230
    # 1 = After a response is received for a physical request, the wait time shall be reduced to P3_MIN
    # Does not affect timing on responses to functional requests
    WAIT_P3_MIN_ONLY = 0x00000200

    SW_CAN_HV_TX = 0x00000400

    # 0 = Transmit using SCI Full duplex mode
    # 1 = Transmit using SCI Half duplex mode
    SCI_MODE = 0x00400000

    # 0 = no voltage after message transmit
    # 1 = apply 20V after message transmit
    SCI_TX_VOLTAGE = 0x00800000

    DT_PERIODIC_UPDATE = 0x10000000


class RxStatus(Enum):
    NONE = 0
    # 0 = received
    # 1 = transmitted
    TX_MSG_TYPE  = 0x00000001

    # 0 = Not a start of message indication
    # 1 = First byte or frame received
    START_OF_MESSAGE = 0x00000002
    ISO15765_FIRST_FRAME = 0x00000002  

    ISO15765_EXT_ADDR = 0x00000080

    # 0 = No break received
    # 1 = Break received
    RX_BREAK = 0x00000004

    # 0 = No TxDone
    # 1 = TxDone
    TX_INDICATION = 0x00000008
    TX_DONE = 0x00000008

    # 0 = No Error
    # 1 = Padding Error
    ISO15765_PADDING_ERROR = 0x00000010

    # 0 = no extended address,
    # 1 = extended address is first byte after the CAN ID
    ISO15765_ADDR_TYPE = 0x00000080

    CAN_29_BIT_ID = 0x00000100

    SW_CAN_NS_RX = 0x00040000
    SW_CAN_HS_RX = 0x00020000
    SW_CAN_HV_RX = 0x00010000


class Ioctl_ID(Enum):
    GET_CONFIG = 0x01
    SET_CONFIG = 0x02
    READ_VBATT = 0x03
    FIVE_BAUD_INIT = 0x04
    FAST_INIT = 0x05
    CLEAR_TX_BUFFER = 0x07
    CLEAR_RX_BUFFER = 0x08
    CLEAR_PERIODIC_MSGS = 0x09
    CLEAR_MSG_FILTERS = 0x0A
    CLEAR_FUNCT_MSG_LOOKUP_TABLE = 0x0B
    ADD_TO_FUNCT_MSG_LOOKUP_TABLE = 0x0C
    DELETE_FROM_FUNCT_MSG_LOOKUP_TABLE = 0x0D
    READ_PROG_VOLTAGE = 0x0E

    DATA_RATE = 0x01  # 5 500000 	# Baud rate value used for vehicle network. No default value specified.
    LOOPBACK = 0x03  # 0(OFF)/1(ON)	# 0 = Do not echo transmitted messages to the Receive queue. 1 = Echo transmitted messages to the Receive queue.
    NODE_ADDRESS = 0x04  # 0x00-0xFF	# J1850PWM specific, physical address for node of interest in the vehicle network. Default is no nodes are recognized by scan tool.
    NETWORK_LINE = 0x05  # 0(BUS_NORMAL)/1(BUS_PLUS)/2(BUS_MINUS)	# J1850PWM specific, network line(s) active during message transfers. Default value is 0(BUS_NORMAL).
    P1_MIN = 0x06  # 0x0-0xFFFF	# ISO-9141/14230 specific, min. ECU inter-byte time for responses [02.02-API: ms]. Default value is 0 ms. 04.04-API: NOT ADJUSTABLE, 0ms.
    P1_MAX = 0x07  # 0x0/0x1-0xFFFF # ISO-9141/14230 specific, max. ECU inter-byte time for responses [02.02-API: ms, 04.04-API: *0.5ms]. Default value is 20 ms.
    P2_MIN = 0x08  # 0x0-0xFFFF	# ISO-9141/14230 specific, min. ECU response time to a tester request or between ECU responses [02.02-API: ms, 04.04-API: *0.5ms]. 04.04-API: NOT ADJUSTABLE, 0ms. Default value is 25 ms.
    P2_MAX = 0x09  # 0x0-0xFFFF	# ISO-9141/14230 specific, max. ECU response time to a tester request or between ECU responses [02.02-API: ms, 04.04-API: *0.5ms]. 04.04-API: NOT ADJUSTABLE, all messages up to P3_MIN are receoved. Default value is 50 ms.
    P3_MIN = 0x0A  # 0x0-0xFFFF	# ISO-9141/14230 specific, min. ECU response time between end of ECU response and next tester request [02.02-API: ms, 04.04-API: *0.5ms]. Default value is 55 ms.
    P3_MAX = 0x0B  # 0x0-0xFFFF	# ISO-9141/14230 specific, max. ECU response time between end of ECU response and next tester request [02.02-API: ms, 04.04-API: *0.5ms]. 04.04-API: NOT ADJUSTABLE, messages can be sent at anytime after P3_MIN. Default value is 5000 ms.
    P4_MIN = 0x0C  # 0x0-0xFFFF	# ISO-9141/14230 specific, min. tester inter-byte time for a request [02.02-API: ms, 04.04-API: *0.5ms]. Default value is 5 ms.
    P4_MAX = 0x0D  # 0x0-0xFFFF	# ISO-9141/14230 specific, max. tester inter-byte time for a request [02.02-API: ms, 04.04-API: *0.5ms]. 04.04-API: NOT ADJUSTABLE, P4_MIN is always used. Default value is 20 ms.
    W1 = 0x0E  # 0x0-0xFFFF	# ISO 9141 specific, max. time [ms] from the address byte end to synchronization pattern start. Default value is 300 ms.
    W2 = 0x0F  # 0x0-0xFFFF	# ISO 9141 specific, max. time [ms] from the synchronization byte end to key byte 1 start. Default value is 20 ms.
    W3 = 0x10  # 0x0-0xFFFF	# ISO 9141 specific, max. time [ms] between key byte 1 and key byte 2. Default value is 20 ms.
    W4 = 0x11  # 0x0-0xFFFF	# ISO 9141 specific, 02.02-API: max. time [ms] between key byte 2 and its inversion from the tester. Default value is 50 ms.
    W5 = 0x12  # 0x0-0xFFFF	# ISO 9141 specific, min. time [ms] before the tester begins retransmission of the address byte. Default value is 300 ms.
    TIDLE = 0x13  # 0x0-0xFFFF	# ISO 9141 specific, bus idle time required before starting a fast initialization sequence. Default value is W5 value.
    TINL = 0x14  # 0x0-0xFFFF	# ISO 9141 specific, the duration [ms] of the fast initialization low pulse. Default value is 25 ms.
    TWUP = 0x15  # 0x0-0xFFFF	# ISO 9141 specific, the duration [ms] of the fast initialization wake-up pulse. Default value is 50 ms.
    PARITY = 0x16  # 0(NO_PARITY)/1(ODD_PARITY)/2(EVEN_PARITY)	# ISO9141 specific, parity type for detecting bit errors.  Default value is 0(NO_PARITY).
    BIT_SAMPLE_POINT = 0x17  # 0-100	# CAN specific, the desired bit sample point as a percentage of bit time. Default value is 80%.
    SYNCH_JUMP_WIDTH = 0x18  # 0-100	# CAN specific, the desired synchronization jump width as a percentage of the bit time. Default value is 15%.
    W0 = 0x19
    T1_MAX = 0x1A  # 0x0-0xFFFF	# SCI_X_XXXX specific, the max. interframe response delay. Default value is 20 ms.
    T2_MAX = 0x1B  # 0x0-0xFFFF	# SCI_X_XXXX specific, the max. interframe request delay.Default value is 100 ms.
    T4_MAX = 0x1C  # 0x0-0xFFFF	# SCI_X_XXXX specific, the max. intermessage response delay. Default value is 20 ms.
    T5_MAX = 0x1D  # 0x0-0xFFFF	# SCI_X_XXXX specific, the max. intermessage request delay. Default value is 100 ms.
    ISO15765_BS = 0x1E  # 0x0-0xFF	# ISO15765 specific, the block size for segmented transfers.
    ISO15765_STMIN = 0x1F  # 0x0-0xFF	# ISO15765 specific, the separation time for segmented transfers.
    DATA_BITS = 0x20  # 04.04-API only
    FIVE_BAUD_MOD = 0x21
    BS_TX = 0x22
    STMIN_TX = 0x23
    T3_MAX = 0x24
    ISO15765_WFT_MAX = 0x25


class Ioctl_Flags(Enum):
    TX_IOCTL_BASE = 0x70000
    TX_IOCTL_SET_DLL_DEBUG_FLAGS = 0x70001
    TX_IOCTL_DLL_DEBUG_FLAG_J2534_CALLS = 0x00000001
