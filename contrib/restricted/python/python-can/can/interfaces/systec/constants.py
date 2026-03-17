from ctypes import c_ubyte as BYTE
from ctypes import c_ulong as DWORD
from ctypes import c_ushort as WORD

#: Maximum number of modules that are supported.
MAX_MODULES = 64

#: Maximum number of applications that can use the USB-CAN-library.
MAX_INSTANCES = 64

#: With the method :meth:`UcanServer.init_can` the module is used, which is detected at first.
#: This value only should be used in case only one module is connected to the computer.
ANY_MODULE = 255

#: No valid USB-CAN Handle (only used internally).
INVALID_HANDLE = 0xFF


class Baudrate(WORD):
    """
    Specifies pre-defined baud rate values for GW-001, GW-002 and all systec USB-CANmoduls.

    .. seealso::

       :meth:`UcanServer.init_can`

       :meth:`UcanServer.set_baudrate`

       :meth:`UcanServer.get_baudrate_message`

       :class:`BaudrateEx`
    """

    #: 1000 kBit/sec
    BAUD_1MBit = 0x14
    #: 800 kBit/sec
    BAUD_800kBit = 0x16
    #: 500 kBit/sec
    BAUD_500kBit = 0x1C
    #: 250 kBit/sec
    BAUD_250kBit = 0x11C
    #: 125 kBit/sec
    BAUD_125kBit = 0x31C
    #: 100 kBit/sec
    BAUD_100kBit = 0x432F
    #: 50 kBit/sec
    BAUD_50kBit = 0x472F
    #: 20 kBit/sec
    BAUD_20kBit = 0x532F
    #: 10 kBit/sec
    BAUD_10kBit = 0x672F
    #: Uses pre-defined extended values of baudrate for all systec USB-CANmoduls.
    BAUD_USE_BTREX = 0x0
    #: Automatic baud rate detection (not implemented in this version).
    BAUD_AUTO = -1


class BaudrateEx(DWORD):
    """
    Specifies pre-defined baud rate values for all systec USB-CANmoduls.

    These values cannot be used for GW-001 and GW-002! Use values from enum :class:`Baudrate` instead.

    .. seealso::

       :meth:`UcanServer.init_can`

       :meth:`UcanServer.set_baudrate`

       :meth:`UcanServer.get_baudrate_ex_message`

       :class:`Baudrate`
    """

    #: G3: 1000 kBit/sec
    BAUDEX_1MBit = 0x20354
    #: G3: 800 kBit/sec
    BAUDEX_800kBit = 0x30254
    #: G3: 500 kBit/sec
    BAUDEX_500kBit = 0x50354
    #: G3: 250 kBit/sec
    BAUDEX_250kBit = 0xB0354
    #: G3: 125 kBit/sec
    BAUDEX_125kBit = 0x170354
    #: G3: 100 kBit/sec
    BAUDEX_100kBit = 0x170466
    #: G3: 50 kBit/sec
    BAUDEX_50kBit = 0x2F0466
    #: G3: 20 kBit/sec
    BAUDEX_20kBit = 0x770466
    #: G3: 10 kBit/sec (half CPU clock)
    BAUDEX_10kBit = 0x80770466
    #: G3: 1000 kBit/sec Sample Point: 87,50%
    BAUDEX_SP2_1MBit = 0x20741
    #: G3: 800 kBit/sec Sample Point: 86,67%
    BAUDEX_SP2_800kBit = 0x30731
    #: G3: 500 kBit/sec Sample Point: 87,50%
    BAUDEX_SP2_500kBit = 0x50741
    #: G3: 250 kBit/sec Sample Point: 87,50%
    BAUDEX_SP2_250kBit = 0xB0741
    #: G3: 125 kBit/sec Sample Point: 87,50%
    BAUDEX_SP2_125kBit = 0x170741
    #: G3: 100 kBit/sec Sample Point: 87,50%
    BAUDEX_SP2_100kBit = 0x1D1741
    #: G3: 50 kBit/sec Sample Point: 87,50%
    BAUDEX_SP2_50kBit = 0x3B1741
    #: G3: 20 kBit/sec Sample Point: 85,00%
    BAUDEX_SP2_20kBit = 0x771772
    #: G3: 10 kBit/sec Sample Point: 85,00% (half CPU clock)
    BAUDEX_SP2_10kBit = 0x80771772

    #: G4: 1000 kBit/sec Sample Point: 83,33%
    BAUDEX_G4_1MBit = 0x406F0000
    #: G4: 800 kBit/sec Sample Point: 80,00%
    BAUDEX_G4_800kBit = 0x402A0001
    #: G4: 500 kBit/sec Sample Point: 83,33%
    BAUDEX_G4_500kBit = 0x406F0001
    #: G4: 250 kBit/sec Sample Point: 83,33%
    BAUDEX_G4_250kBit = 0x406F0003
    #: G4: 125 kBit/sec Sample Point: 83,33%
    BAUDEX_G4_125kBit = 0x406F0007
    #: G4: 100 kBit/sec Sample Point: 83,33%
    BAUDEX_G4_100kBit = 0x416F0009
    #: G4: 50 kBit/sec Sample Point: 83,33%
    BAUDEX_G4_50kBit = 0x416F0013
    #: G4: 20 kBit/sec Sample Point: 84,00%
    BAUDEX_G4_20kBit = 0x417F002F
    #: G4: 10 kBit/sec Sample Point: 84,00% (half CPU clock)
    BAUDEX_G4_10kBit = 0x417F005F
    #: Uses pre-defined values of baud rates of :class:`Baudrate`.
    BAUDEX_USE_BTR01 = 0x0
    #: Automatic baud rate detection (not implemented in this version).
    BAUDEX_AUTO = 0xFFFFFFFF


class MsgFrameFormat(BYTE):
    """
    Specifies values for the frame format of CAN messages for member :attr:`CanMsg.m_bFF` in structure
    :class:`CanMsg`. These values can be combined.

    .. seealso:: :class:`CanMsg`
    """

    #: standard CAN data frame with 11 bit ID (CAN2.0A spec.)
    MSG_FF_STD = 0x0
    #: transmit echo
    MSG_FF_ECHO = 0x20
    #: CAN remote request frame with
    MSG_FF_RTR = 0x40
    #: extended CAN data frame with 29 bit ID (CAN2.0B spec.)
    MSG_FF_EXT = 0x80


class ReturnCode(BYTE):
    """
    Specifies all return codes of all methods of this class.
    """

    #: no error
    SUCCESSFUL = 0x0
    # start of error codes coming from USB-CAN-library
    ERR = 0x1
    # start of error codes coming from command interface between host and USB-CANmodul
    ERRCMD = 0x40
    # start of warning codes
    WARNING = 0x80
    # start of reserved codes which are only used internally
    RESERVED = 0xC0

    #: could not created a resource (memory, handle, ...)
    ERR_RESOURCE = 0x1
    #: the maximum number of opened modules is reached
    ERR_MAXMODULES = 0x2
    #: the specified module is already in use
    ERR_HWINUSE = 0x3
    #: the software versions of the module and library are incompatible
    ERR_ILLVERSION = 0x4
    #: the module with the specified device number is not connected (or used by an other application)
    ERR_ILLHW = 0x5
    #: wrong USB-CAN-Handle handed over to the function
    ERR_ILLHANDLE = 0x6
    #: wrong parameter handed over to the function
    ERR_ILLPARAM = 0x7
    #: instruction can not be processed at this time
    ERR_BUSY = 0x8
    #: no answer from module
    ERR_TIMEOUT = 0x9
    #: a request to the driver failed
    ERR_IOFAILED = 0xA
    #: a CAN message did not fit into the transmit buffer
    ERR_DLL_TXFULL = 0xB
    #: maximum number of applications is reached
    ERR_MAXINSTANCES = 0xC
    #: CAN interface is not yet initialized
    ERR_CANNOTINIT = 0xD
    #: USB-CANmodul was disconnected
    ERR_DISCONECT = 0xE
    #: the needed device class does not exist
    ERR_NOHWCLASS = 0xF
    #: illegal CAN channel
    ERR_ILLCHANNEL = 0x10
    #: reserved
    ERR_RESERVED1 = 0x11
    #: the API function can not be used with this hardware
    ERR_ILLHWTYPE = 0x12

    #: the received response does not match to the transmitted command
    ERRCMD_NOTEQU = 0x40
    #: no access to the CAN controller
    ERRCMD_REGTST = 0x41
    #: the module could not interpret the command
    ERRCMD_ILLCMD = 0x42
    #: error while reading the EEPROM
    ERRCMD_EEPROM = 0x43
    #: reserved
    ERRCMD_RESERVED1 = 0x44
    #: reserved
    ERRCMD_RESERVED2 = 0x45
    #: reserved
    ERRCMD_RESERVED3 = 0x46
    #: illegal baud rate value specified in BTR0/BTR1 for systec USB-CANmoduls
    ERRCMD_ILLBDR = 0x47
    #: CAN channel is not initialized
    ERRCMD_NOTINIT = 0x48
    #: CAN channel is already initialized
    ERRCMD_ALREADYINIT = 0x49
    #: illegal sub-command specified
    ERRCMD_ILLSUBCMD = 0x4A
    #: illegal index specified (e.g. index for cyclic CAN messages)
    ERRCMD_ILLIDX = 0x4B
    #: cyclic CAN message(s) can not be defined because transmission of cyclic CAN messages is already running
    ERRCMD_RUNNING = 0x4C

    #: no CAN messages received
    WARN_NODATA = 0x80
    #: overrun in receive buffer of the kernel driver
    WARN_SYS_RXOVERRUN = 0x81
    #: overrun in receive buffer of the USB-CAN-library
    WARN_DLL_RXOVERRUN = 0x82
    #: reserved
    WARN_RESERVED1 = 0x83
    #: reserved
    WARN_RESERVED2 = 0x84
    #: overrun in transmit buffer of the firmware (but this CAN message was successfully stored in buffer of the
    #: library)
    WARN_FW_TXOVERRUN = 0x85
    #: overrun in receive buffer of the firmware (but this CAN message was successfully read)
    WARN_FW_RXOVERRUN = 0x86
    #: reserved
    WARN_FW_TXMSGLOST = 0x87
    #: pointer is NULL
    WARN_NULL_PTR = 0x90
    #: not all CAN messages could be stored to the transmit buffer in USB-CAN-library (check output of parameter
    #: pdwCount_p)
    WARN_TXLIMIT = 0x91
    #: reserved
    WARN_BUSY = 0x92


class CbEvent(BYTE):
    """
    This enum defines events for the callback functions of the library.

    .. seealso:: :meth:`UcanServer.get_status`
    """

    #: The USB-CANmodul has been initialized.
    EVENT_INITHW = 0
    #: The CAN interface has been initialized.
    EVENT_init_can = 1
    #: A new CAN message has been received.
    EVENT_RECEIVE = 2
    #: The error state in the module has changed.
    EVENT_STATUS = 3
    #: The CAN interface has been deinitialized.
    EVENT_DEINIT_CAN = 4
    #: The USB-CANmodul has been deinitialized.
    EVENT_DEINITHW = 5
    #: A new USB-CANmodul has been connected.
    EVENT_CONNECT = 6
    #: Any USB-CANmodul has been disconnected.
    EVENT_DISCONNECT = 7
    #: A USB-CANmodul has been disconnected during operation.
    EVENT_FATALDISCON = 8
    #: Reserved
    EVENT_RESERVED1 = 0x80


class CanStatus(WORD):
    """
    CAN error status bits. These bit values occurs in combination with the method :meth:`UcanServer.get_status`.

    .. seealso::

       :meth:`UcanServer.get_status`

       :meth:`UcanServer.get_can_status_message`
    """

    #: No error.
    CANERR_OK = 0x0
    #: Transmit buffer of the CAN controller is full.
    CANERR_XMTFULL = 0x1
    #: Receive buffer of the CAN controller is full.
    CANERR_OVERRUN = 0x2
    #: Bus error: Error Limit 1 exceeded (Warning Limit reached)
    CANERR_BUSLIGHT = 0x4
    #: Bus error: Error Limit 2 exceeded (Error Passive)
    CANERR_BUSHEAVY = 0x8
    #: Bus error: CAN controller has gone into Bus-Off state.
    #: Method :meth:`UcanServer.reset_can` has to be called.
    CANERR_BUSOFF = 0x10
    #: No CAN message is within the receive buffer.
    CANERR_QRCVEMPTY = 0x20
    #: Receive buffer is full. CAN messages has been lost.
    CANERR_QOVERRUN = 0x40
    #: Transmit buffer is full.
    CANERR_QXMTFULL = 0x80
    #: Register test of the CAN controller failed.
    CANERR_REGTEST = 0x100
    #: Memory test on hardware failed.
    CANERR_MEMTEST = 0x200
    #: Transmit CAN message(s) was/were automatically deleted by firmware (transmit timeout).
    CANERR_TXMSGLOST = 0x400


class UsbStatus(WORD):
    """
    USB error status bits. These bit values occurs in combination with the method :meth:`UcanServer.get_status`.

    .. seealso:: :meth:`UcanServer.get_status`
    """

    #: No error.
    USBERR_OK = 0x0


#: Specifies the acceptance mask for receiving all CAN messages.
#:
#: .. seealso::
#:
#:    :const:`ACR_ALL`
#:
#:    :meth:`UcanServer.init_can`
#:
#:    :meth:`UcanServer.set_acceptance`
AMR_ALL = 0xFFFFFFFF

#: Specifies the acceptance code for receiving all CAN messages.
#:
#: .. seealso::
#:
#:    :const:`AMR_ALL`
#:
#:    :meth:`UcanServer.init_can`
#:
#:    :meth:`UcanServer.set_acceptance`
ACR_ALL = 0x0


class OutputControl(BYTE):
    """
    Specifies pre-defined values for the Output Control Register of SJA1000 on GW-001 and GW-002.
    These values are only important for GW-001 and GW-002.
    They does not have an effect on systec USB-CANmoduls.
    """

    #: default OCR value for the standard USB-CANmodul GW-001/GW-002
    OCR_DEFAULT = 0x1A
    #: OCR value for RS485 interface and galvanic isolation
    OCR_RS485_ISOLATED = 0x1E
    #: OCR value for RS485 interface but without galvanic isolation
    OCR_RS485_NOT_ISOLATED = 0xA


#: Specifies the default value for the maximum number of entries in the receive and transmit buffer.
DEFAULT_BUFFER_ENTRIES = 4096


class Channel(BYTE):
    """
    Specifies values for the CAN channel to be used on multi-channel USB-CANmoduls.
    """

    #: Specifies the first CAN channel (GW-001/GW-002 and USB-CANmodul1 only can be used with this channel).
    CHANNEL_CH0 = 0
    #: Specifies the second CAN channel (this channel cannot be used with GW-001/GW-002 and USB-CANmodul1).
    CHANNEL_CH1 = 1
    #: Specifies all CAN channels (can only be used with the method :meth:`UcanServer.shutdown`).
    CHANNEL_ALL = 254
    #: Specifies the use of any channel (can only be used with the method :meth:`UcanServer.read_can_msg`).
    CHANNEL_ANY = 255
    #: Specifies the first CAN channel (equivalent to :data:`CHANNEL_CH0`).
    CHANNEL_CAN1 = CHANNEL_CH0
    #: Specifies the second CAN channel (equivalent to :data:`CHANNEL_CH1`).
    CHANNEL_CAN2 = CHANNEL_CH1
    #: Specifies the LIN channel (currently not supported by the software).
    CHANNEL_LIN = CHANNEL_CH1


class ResetFlags(DWORD):
    """
    Specifies flags for resetting USB-CANmodul with method :meth:`UcanServer.reset_can`.
    These flags can be used in combination.

    .. seealso:: :meth:`UcanServer.reset_can`
    """

    #: reset everything
    RESET_ALL = 0x0
    #: no CAN status reset (only supported for systec USB-CANmoduls)
    RESET_NO_STATUS = 0x1
    #: no CAN controller reset
    RESET_NO_CANCTRL = 0x2
    #: no transmit message counter reset
    RESET_NO_TXCOUNTER = 0x4
    #: no receive message counter reset
    RESET_NO_RXCOUNTER = 0x8
    #: no transmit message buffer reset at channel level
    RESET_NO_TXBUFFER_CH = 0x10
    #: no transmit message buffer reset at USB-CAN-library level
    RESET_NO_TXBUFFER_DLL = 0x20
    #: no transmit message buffer reset at firmware level
    RESET_NO_TXBUFFER_FW = 0x80
    #: no receive message buffer reset at channel level
    RESET_NO_RXBUFFER_CH = 0x100
    #: no receive message buffer reset at USB-CAN-library level
    RESET_NO_RXBUFFER_DLL = 0x200
    #: no receive message buffer reset at kernel driver level
    RESET_NO_RXBUFFER_SYS = 0x400
    #: no receive message buffer reset at firmware level
    RESET_NO_RXBUFFER_FW = 0x800
    #: complete firmware reset (module will automatically reconnect at USB port in 500msec)
    RESET_FIRMWARE = 0xFFFFFFFF

    #: no reset of all message counters
    RESET_NO_COUNTER_ALL = RESET_NO_TXCOUNTER | RESET_NO_RXCOUNTER
    #: no reset of transmit message buffers at communication level (firmware, kernel and library)
    RESET_NO_TXBUFFER_COMM = RESET_NO_TXBUFFER_DLL | 0x40 | RESET_NO_TXBUFFER_FW
    #: no reset of receive message buffers at communication level (firmware, kernel and library)
    RESET_NO_RXBUFFER_COMM = (
        RESET_NO_RXBUFFER_DLL | RESET_NO_RXBUFFER_SYS | RESET_NO_RXBUFFER_FW
    )
    #: no reset of all transmit message buffers
    RESET_NO_TXBUFFER_ALL = RESET_NO_TXBUFFER_CH | RESET_NO_TXBUFFER_COMM
    #: no reset of all receive message buffers
    RESET_NO_RXBUFFER_ALL = RESET_NO_RXBUFFER_CH | RESET_NO_RXBUFFER_COMM
    #: no reset of all message buffers at communication level (firmware, kernel and library)
    RESET_NO_BUFFER_COMM = RESET_NO_TXBUFFER_COMM | RESET_NO_RXBUFFER_COMM
    #: no reset of all message buffers
    RESET_NO_BUFFER_ALL = RESET_NO_TXBUFFER_ALL | RESET_NO_RXBUFFER_ALL
    #: reset of the CAN status only
    RESET_ONLY_STATUS = 0xFFFF & ~RESET_NO_STATUS
    #: reset of the CAN controller only
    RESET_ONLY_CANCTRL = 0xFFFF & ~RESET_NO_CANCTRL
    #: reset of the transmit buffer in firmware only
    RESET_ONLY_TXBUFFER_FW = 0xFFFF & ~RESET_NO_TXBUFFER_FW
    #: reset of the receive buffer in firmware only
    RESET_ONLY_RXBUFFER_FW = 0xFFFF & ~RESET_NO_RXBUFFER_FW
    #: reset of the specified channel of the receive buffer only
    RESET_ONLY_RXCHANNEL_BUFF = 0xFFFF & ~RESET_NO_RXBUFFER_CH
    #: reset of the specified channel of the transmit buffer only
    RESET_ONLY_TXCHANNEL_BUFF = 0xFFFF & ~RESET_NO_TXBUFFER_CH
    #: reset of the receive buffer and receive message counter only
    RESET_ONLY_RX_BUFF = 0xFFFF & ~(RESET_NO_RXBUFFER_ALL | RESET_NO_RXCOUNTER)
    #: reset of the receive buffer and receive message counter (for GW-002) only
    RESET_ONLY_RX_BUFF_GW002 = 0xFFFF & ~(
        RESET_NO_RXBUFFER_ALL | RESET_NO_RXCOUNTER | RESET_NO_TXBUFFER_FW
    )
    #: reset of the transmit buffer and transmit message counter only
    RESET_ONLY_TX_BUFF = 0xFFFF & ~(RESET_NO_TXBUFFER_ALL | RESET_NO_TXCOUNTER)
    #: reset of all buffers and all message counters only
    RESET_ONLY_ALL_BUFF = RESET_ONLY_RX_BUFF & RESET_ONLY_TX_BUFF
    #: reset of all message counters only
    RESET_ONLY_ALL_COUNTER = 0xFFFF & ~RESET_NO_COUNTER_ALL


PRODCODE_PID_TWO_CHA = 0x1
PRODCODE_PID_TERM = 0x1
PRODCODE_PID_RBUSER = 0x1
PRODCODE_PID_RBCAN = 0x1
PRODCODE_PID_G4 = 0x20
PRODCODE_PID_RESVD = 0x40

PRODCODE_MASK_DID = 0xFFFF0000
PRODCODE_MASK_PID = 0xFFFF
PRODCODE_MASK_PIDG3 = PRODCODE_MASK_PID & 0xFFFFFFBF


class ProductCode(WORD):
    """
    These values defines product codes for all known USB-CANmodul derivatives received in member
    :attr:`HardwareInfoEx.m_dwProductCode` of structure :class:`HardwareInfoEx`
    with method :meth:`UcanServer.get_hardware_info`.

    .. seealso::

       :meth:`UcanServer.get_hardware_info`

       :class:`HardwareInfoEx`
    """

    #: Product code for GW-001 (outdated).
    PRODCODE_PID_GW001 = 0x1100
    #: Product code for GW-002 (outdated).
    PRODCODE_PID_GW002 = 0x1102
    #: Product code for Multiport CAN-to-USB G3.
    PRODCODE_PID_MULTIPORT = 0x1103
    #: Product code for USB-CANmodul1 G3.
    PRODCODE_PID_BASIC = 0x1104
    #: Product code for USB-CANmodul2 G3.
    PRODCODE_PID_ADVANCED = 0x1105
    #: Product code for USB-CANmodul8 G3.
    PRODCODE_PID_USBCAN8 = 0x1107
    #: Product code for USB-CANmodul16 G3.
    PRODCODE_PID_USBCAN16 = 0x1109
    #: Reserved.
    PRODCODE_PID_RESERVED3 = 0x1110
    #: Product code for USB-CANmodul2 G4.
    PRODCODE_PID_ADVANCED_G4 = 0x1121
    #: Product code for USB-CANmodul1 G4.
    PRODCODE_PID_BASIC_G4 = 0x1122
    #: Reserved.
    PRODCODE_PID_RESERVED1 = 0x1144
    #: Reserved.
    PRODCODE_PID_RESERVED2 = 0x1145


#: Definitions for cyclic CAN messages.
MAX_CYCLIC_CAN_MSG = 16


class CyclicFlags(DWORD):
    """
    Specifies flags for cyclical CAN messages.
    These flags can be used in combinations with method :meth:`UcanServer.enable_cyclic_can_msg`.

    .. seealso:: :meth:`UcanServer.enable_cyclic_can_msg`
    """

    #: Stops the transmission of cyclic CAN messages.
    CYCLIC_FLAG_STOPP = 0x0
    #: Global enable of transmission of cyclic CAN messages.
    CYCLIC_FLAG_START = 0x80000000
    #: List of cyclic CAN messages will be processed in sequential mode (otherwise in parallel mode).
    CYCLIC_FLAG_SEQUMODE = 0x40000000
    #: No echo will be sent back if echo mode is enabled with method :meth:`UcanServer.init_can`.
    CYCLIC_FLAG_NOECHO = 0x10000
    #: CAN message with index 0 of the list will not be sent.
    CYCLIC_FLAG_LOCK_0 = 0x1
    #: CAN message with index 1 of the list will not be sent.
    CYCLIC_FLAG_LOCK_1 = 0x2
    #: CAN message with index 2 of the list will not be sent.
    CYCLIC_FLAG_LOCK_2 = 0x4
    #: CAN message with index 3 of the list will not be sent.
    CYCLIC_FLAG_LOCK_3 = 0x8
    #: CAN message with index 4 of the list will not be sent.
    CYCLIC_FLAG_LOCK_4 = 0x10
    #: CAN message with index 5 of the list will not be sent.
    CYCLIC_FLAG_LOCK_5 = 0x20
    #: CAN message with index 6 of the list will not be sent.
    CYCLIC_FLAG_LOCK_6 = 0x40
    #: CAN message with index 7 of the list will not be sent.
    CYCLIC_FLAG_LOCK_7 = 0x80
    #: CAN message with index 8 of the list will not be sent.
    CYCLIC_FLAG_LOCK_8 = 0x100
    #: CAN message with index 9 of the list will not be sent.
    CYCLIC_FLAG_LOCK_9 = 0x200
    #: CAN message with index 10 of the list will not be sent.
    CYCLIC_FLAG_LOCK_10 = 0x400
    #: CAN message with index 11 of the list will not be sent.
    CYCLIC_FLAG_LOCK_11 = 0x800
    #: CAN message with index 12 of the list will not be sent.
    CYCLIC_FLAG_LOCK_12 = 0x1000
    #: CAN message with index 13 of the list will not be sent.
    CYCLIC_FLAG_LOCK_13 = 0x2000
    #: CAN message with index 14 of the list will not be sent.
    CYCLIC_FLAG_LOCK_14 = 0x4000
    #: CAN message with index 15 of the list will not be sent.
    CYCLIC_FLAG_LOCK_15 = 0x8000


class PendingFlags(BYTE):
    """
    Specifies flags for method :meth:`UcanServer.get_msg_pending`.
    These flags can be uses in combinations.

    .. seealso:: :meth:`UcanServer.get_msg_pending`
    """

    #: number of pending CAN messages in receive buffer of USB-CAN-library
    PENDING_FLAG_RX_DLL = 0x1
    #: reserved
    PENDING_FLAG_RX_SYS = 0x2
    #: number of pending CAN messages in receive buffer of firmware
    PENDING_FLAG_RX_FW = 0x4
    #: number of pending CAN messages in transmit buffer of USB-CAN-library
    PENDING_FLAG_TX_DLL = 0x10
    #: reserved
    PENDING_FLAG_TX_SYS = 0x20
    #: number of pending CAN messages in transmit buffer of firmware
    PENDING_FLAG_TX_FW = 0x40
    #: number of pending CAN messages in all receive buffers
    PENDING_FLAG_RX_ALL = PENDING_FLAG_RX_DLL | PENDING_FLAG_RX_SYS | PENDING_FLAG_RX_FW
    #: number of pending CAN messages in all transmit buffers
    PENDING_FLAG_TX_ALL = PENDING_FLAG_TX_DLL | PENDING_FLAG_TX_SYS | PENDING_FLAG_TX_FW
    #: number of pending CAN messages in all buffers
    PENDING_FLAG_ALL = PENDING_FLAG_RX_ALL | PENDING_FLAG_TX_ALL


class Mode(BYTE):
    """
    Specifies values for operation mode of a CAN channel.
    These values can be combined by OR operation with the method :meth:`UcanServer.init_can`.
    """

    #: normal operation mode (transmitting and receiving)
    MODE_NORMAL = 0
    #: listen only mode (receiving only, no ACK at CAN bus)
    MODE_LISTEN_ONLY = 1
    #: CAN messages which was sent will be received back with method :meth:`UcanServer.read_can_msg`
    MODE_TX_ECHO = 2
    #: reserved (not implemented in this version)
    MODE_RX_ORDER_CH = 4
    #: high resolution time stamps in received CAN messages (only available with STM derivatives)
    MODE_HIGH_RES_TIMER = 8


class VersionType(BYTE):
    """
    Specifies values for receiving the version information of several driver files.

    .. note:: This structure is only used internally.
    """

    #: version of the USB-CAN-library
    VER_TYPE_USER_LIB = 1
    #: equivalent to :attr:`VER_TYPE_USER_LIB`
    VER_TYPE_USER_DLL = 1
    #: version of USBCAN.SYS (not supported in this version)
    VER_TYPE_SYS_DRV = 2
    #: version of firmware in hardware (not supported, use method :meth:`UcanServer.get_fw_version`)
    VER_TYPE_FIRMWARE = 3
    #: version of UCANNET.SYS
    VER_TYPE_NET_DRV = 4
    #: version of USBCANLD.SYS
    VER_TYPE_SYS_LD = 5
    #: version of USBCANL2.SYS
    VER_TYPE_SYS_L2 = 6
    #: version of USBCANL3.SYS
    VER_TYPE_SYS_L3 = 7
    #: version of USBCANL4.SYS
    VER_TYPE_SYS_L4 = 8
    #: version of USBCANL5.SYS
    VER_TYPE_SYS_L5 = 9
    #: version of USBCANCP.CPL
    VER_TYPE_CPL = 10
