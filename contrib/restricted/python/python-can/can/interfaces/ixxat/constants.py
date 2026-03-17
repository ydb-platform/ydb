"""
Ctypes wrapper module for IXXAT Virtual CAN Interface V4 on win32 systems

Copyright (C) 2016 Giuseppe Corbelli <giuseppe.corbelli@weightpack.com>
"""

from . import structures

FALSE = 0
TRUE = 1

INFINITE = 0xFFFFFFFF

VCI_MAX_ERRSTRLEN = 256

# Bitrates
CAN_BT0_10KB = 0x31
CAN_BT1_10KB = 0x1C
CAN_BT0_20KB = 0x18
CAN_BT1_20KB = 0x1C
CAN_BT0_50KB = 0x09
CAN_BT1_50KB = 0x1C
CAN_BT0_100KB = 0x04
CAN_BT1_100KB = 0x1C
CAN_BT0_125KB = 0x03
CAN_BT1_125KB = 0x1C
CAN_BT0_250KB = 0x01
CAN_BT1_250KB = 0x1C
CAN_BT0_500KB = 0x00
CAN_BT1_500KB = 0x1C
CAN_BT0_667KB = 0x00
CAN_BT1_667KB = 0x18
CAN_BT0_800KB = 0x00
CAN_BT1_800KB = 0x16
CAN_BT0_1000KB = 0x00
CAN_BT1_1000KB = 0x14

# Facilities/severities
SEV_INFO = 0x40000000
SEV_WARN = 0x80000000
SEV_ERROR = 0xC0000000
SEV_MASK = 0xC0000000
SEV_SUCCESS = 0x00000000

RESERVED_FLAG = 0x10000000
CUSTOMER_FLAG = 0x20000000

STATUS_MASK = 0x0000FFFF
FACILITY_MASK = 0x0FFF0000

# Or so I hope
FACILITY_STD = 0

SEV_STD_INFO = SEV_INFO | CUSTOMER_FLAG | FACILITY_STD
SEV_STD_WARN = SEV_WARN | CUSTOMER_FLAG | FACILITY_STD
SEV_STD_ERROR = SEV_ERROR | CUSTOMER_FLAG | FACILITY_STD

FACILITY_VCI = 0x00010000
SEV_VCI_INFO = SEV_INFO | CUSTOMER_FLAG | FACILITY_VCI
SEV_VCI_WARN = SEV_WARN | CUSTOMER_FLAG | FACILITY_VCI
SEV_VCI_ERROR = SEV_ERROR | CUSTOMER_FLAG | FACILITY_VCI

FACILITY_DAL = 0x00020000
SEV_DAL_INFO = SEV_INFO | CUSTOMER_FLAG | FACILITY_DAL
SEV_DAL_WARN = SEV_WARN | CUSTOMER_FLAG | FACILITY_DAL
SEV_DAL_ERROR = SEV_ERROR | CUSTOMER_FLAG | FACILITY_DAL

FACILITY_CCL = 0x00030000
SEV_CCL_INFO = SEV_INFO | CUSTOMER_FLAG | FACILITY_CCL
SEV_CCL_WARN = SEV_WARN | CUSTOMER_FLAG | FACILITY_CCL
SEV_CCL_ERROR = SEV_ERROR | CUSTOMER_FLAG | FACILITY_CCL

FACILITY_BAL = 0x00040000
SEV_BAL_INFO = SEV_INFO | CUSTOMER_FLAG | FACILITY_BAL
SEV_BAL_WARN = SEV_WARN | CUSTOMER_FLAG | FACILITY_BAL
SEV_BAL_ERROR = SEV_ERROR | CUSTOMER_FLAG | FACILITY_BAL

# Errors
VCI_SUCCESS = 0x00
VCI_OK = 0x00
VCI_E_UNEXPECTED = SEV_VCI_ERROR | 0x0001
VCI_E_NOT_IMPLEMENTED = SEV_VCI_ERROR | 0x0002
VCI_E_OUTOFMEMORY = SEV_VCI_ERROR | 0x0003
VCI_E_INVALIDARG = SEV_VCI_ERROR | 0x0004
VCI_E_NOINTERFACE = SEV_VCI_ERROR | 0x0005
VCI_E_INVPOINTER = SEV_VCI_ERROR | 0x0006
VCI_E_INVHANDLE = SEV_VCI_ERROR | 0x0007
VCI_E_ABORT = SEV_VCI_ERROR | 0x0008
VCI_E_FAIL = SEV_VCI_ERROR | 0x0009
VCI_E_ACCESSDENIED = SEV_VCI_ERROR | 0x000A
VCI_E_TIMEOUT = SEV_VCI_ERROR | 0x000B
VCI_E_BUSY = SEV_VCI_ERROR | 0x000C
VCI_E_PENDING = SEV_VCI_ERROR | 0x000D
VCI_E_NO_DATA = SEV_VCI_ERROR | 0x000E
VCI_E_NO_MORE_ITEMS = SEV_VCI_ERROR | 0x000F
VCI_E_NOT_INITIALIZED = SEV_VCI_ERROR | 0x0010
VCI_E_ALREADY_INITIALIZED = SEV_VCI_ERROR | 0x00011
VCI_E_RXQUEUE_EMPTY = SEV_VCI_ERROR | 0x00012
VCI_E_TXQUEUE_FULL = SEV_VCI_ERROR | 0x0013
VCI_E_BUFFER_OVERFLOW = SEV_VCI_ERROR | 0x0014
VCI_E_INVALID_STATE = SEV_VCI_ERROR | 0x0015
VCI_E_OBJECT_ALREADY_EXISTS = SEV_VCI_ERROR | 0x0016
VCI_E_INVALID_INDEX = SEV_VCI_ERROR | 0x0017
VCI_E_END_OF_FILE = SEV_VCI_ERROR | 0x0018
VCI_E_DISCONNECTED = SEV_VCI_ERROR | 0x0019
VCI_E_WRONG_FLASHFWVERSION = SEV_VCI_ERROR | 0x001A

# Controller status
CAN_STATUS_TXPEND = 0x01  # transmission pending
CAN_STATUS_OVRRUN = 0x02  # data overrun occurred
CAN_STATUS_ERRLIM = 0x04  # error warning limit exceeded
CAN_STATUS_BUSOFF = 0x08  # bus off status
CAN_STATUS_ININIT = 0x10  # init mode active
CAN_STATUS_BUSCERR = 0x20  # bus coupling error

# Controller operating modes
CAN_OPMODE_UNDEFINED = 0x00  # undefined
CAN_OPMODE_STANDARD = 0x01  # reception of 11-bit id messages
CAN_OPMODE_EXTENDED = 0x02  # reception of 29-bit id messages
CAN_OPMODE_ERRFRAME = 0x04  # reception of error frames
CAN_OPMODE_LISTONLY = 0x08  # listen only mode (TX passive)
CAN_OPMODE_LOWSPEED = 0x10  # use low speed bus interface
CAN_OPMODE_AUTOBAUD = 0x20  # automatic bit rate detection

# Extended operating modes
CAN_EXMODE_DISABLED = 0x00
CAN_EXMODE_EXTDATALEN = 0x01
CAN_EXMODE_FASTDATA = 0x02
CAN_EXMODE_NONISOCANFD = 0x04

# Message types
CAN_MSGTYPE_DATA = 0
CAN_MSGTYPE_INFO = 1
CAN_MSGTYPE_ERROR = 2
CAN_MSGTYPE_STATUS = 3
CAN_MSGTYPE_WAKEUP = 4
CAN_MSGTYPE_TIMEOVR = 5
CAN_MSGTYPE_TIMERST = 6

# Information supplied in the abData[0] field of info frames
# (CANMSGINFO.Bytes.bType = CAN_MSGTYPE_INFO).
CAN_INFO_START = 1
CAN_INFO_STOP = 2
CAN_INFO_RESET = 3

# Information supplied in the abData[0] field of info frames
# (CANMSGINFO.Bytes.bType = CAN_MSGTYPE_ERROR).
CAN_ERROR_STUFF = 1  # stuff error
CAN_ERROR_FORM = 2  # form error
CAN_ERROR_ACK = 3  # acknowledgment error
CAN_ERROR_BIT = 4  # bit error
CAN_ERROR_CRC = 6  # CRC error
CAN_ERROR_OTHER = 7  # other (unspecified) error

# acceptance code and mask to reject all CAN IDs
CAN_ACC_MASK_NONE = 0xFFFFFFFF
CAN_ACC_CODE_NONE = 0x80000000

# BTMODEs
CAN_BTMODE_RAW = 0x00000001  # raw mode
CAN_BTMODE_TSM = 0x00000002  # triple sampling mode


CAN_FILTER_VOID = 0x00  # invalid or unknown filter mode (do not use for initialization)
CAN_FILTER_LOCK = 0x01  # lock filter (inhibit all IDs)
CAN_FILTER_PASS = 0x02  # bypass filter (pass all IDs)
CAN_FILTER_INCL = 0x03  # inclusive filtering (pass registered IDs)
CAN_FILTER_EXCL = 0x04  # exclusive filtering (inhibit registered IDs)


# message information flags (used by <CANMSGINFO.Bytes.bFlags>)
CAN_MSGFLAGS_DLC = 0x0F  # [bit 0] data length code
CAN_MSGFLAGS_OVR = 0x10  # [bit 4] data overrun flag
CAN_MSGFLAGS_SRR = 0x20  # [bit 5] self reception request
CAN_MSGFLAGS_RTR = 0x40  # [bit 6] remote transmission request
CAN_MSGFLAGS_EXT = 0x80  # [bit 7] frame format (0=11-bit, 1=29-bit)

# extended message information flags (used by <CANMSGINFO.Bytes.[bFlags2|bAddFlags]>)
CAN_MSGFLAGS2_SSM = 0x01  # [bit 0] single shot mode
CAN_MSGFLAGS2_HPM = 0x02  # [bit 1] high priority message
CAN_MSGFLAGS2_EDL = 0x04  # [bit 2] extended data length
CAN_MSGFLAGS2_FDR = 0x08  # [bit 3] fast data bit rate
CAN_MSGFLAGS2_ESI = 0x10  # [bit 4] error state indicator
CAN_MSGFLAGS2_RES = 0xE0  # [bit 5..7] reserved bits


CAN_ACCEPT_REJECT = 0x00  # message not accepted
CAN_ACCEPT_ALWAYS = 0xFF  # message always accepted
CAN_ACCEPT_FILTER_1 = 0x01  # message accepted by filter 1
CAN_ACCEPT_FILTER_2 = 0x02  # message accepted by filter 2
CAN_ACCEPT_PASSEXCL = 0x03  # message passes exclusion filter


CAN_FEATURE_STDOREXT = 0x00000001  # 11 OR 29 bit (exclusive)
CAN_FEATURE_STDANDEXT = 0x00000002  # 11 AND 29 bit (simultaneous)
CAN_FEATURE_RMTFRAME = 0x00000004  # reception of remote frames
CAN_FEATURE_ERRFRAME = 0x00000008  # reception of error frames
CAN_FEATURE_BUSLOAD = 0x00000010  # bus load measurement
CAN_FEATURE_IDFILTER = 0x00000020  # exact message filter
CAN_FEATURE_LISTONLY = 0x00000040  # listen only mode
CAN_FEATURE_SCHEDULER = 0x00000080  # cyclic message scheduler
CAN_FEATURE_GENERRFRM = 0x00000100  # error frame generation
CAN_FEATURE_DELAYEDTX = 0x00000200  # delayed message transmitter
CAN_FEATURE_SINGLESHOT = 0x00000400  # single shot mode
CAN_FEATURE_HIGHPRIOR = 0x00000800  # high priority message
CAN_FEATURE_AUTOBAUD = 0x00001000  # automatic bit rate detection
CAN_FEATURE_EXTDATA = 0x00002000  # extended data length (CANFD)
CAN_FEATURE_FASTDATA = 0x00004000  # fast data bit rate (CANFD)
CAN_FEATURE_ISOFRAME = 0x00008000  # ISO conform frame (CANFD)
CAN_FEATURE_NONISOFRM = (
    0x00010000  # non ISO conform frame (CANFD) (different CRC computation)
)
CAN_FEATURE_64BITTSC = 0x00020000  # 64-bit time stamp counter


CAN_BITRATE_PRESETS = {
    250000: structures.CANBTP(
        dwMode=0, dwBPS=250000, wTS1=6400, wTS2=1600, wSJW=1600, wTDO=0
    ),  # SP = 80,0%
    500000: structures.CANBTP(
        dwMode=0, dwBPS=500000, wTS1=6400, wTS2=1600, wSJW=1600, wTDO=0
    ),  # SP = 80,0%
    1000000: structures.CANBTP(
        dwMode=0, dwBPS=1000000, wTS1=6400, wTS2=1600, wSJW=1600, wTDO=0
    ),  # SP = 80,0%
}

CAN_DATABITRATE_PRESETS = {
    500000: structures.CANBTP(
        dwMode=0, dwBPS=500000, wTS1=6400, wTS2=1600, wSJW=1600, wTDO=6400
    ),  # SP = 80,0%
    833333: structures.CANBTP(
        dwMode=0, dwBPS=833333, wTS1=1600, wTS2=400, wSJW=400, wTDO=1620
    ),  # SP = 80,0%
    1000000: structures.CANBTP(
        dwMode=0, dwBPS=1000000, wTS1=1600, wTS2=400, wSJW=400, wTDO=1600
    ),  # SP = 80,0%
    1538461: structures.CANBTP(
        dwMode=0, dwBPS=1538461, wTS1=1000, wTS2=300, wSJW=300, wTDO=1040
    ),  # SP = 76,9%
    2000000: structures.CANBTP(
        dwMode=0, dwBPS=2000000, wTS1=1600, wTS2=400, wSJW=400, wTDO=1600
    ),  # SP = 80,0%
    4000000: structures.CANBTP(
        dwMode=0, dwBPS=4000000, wTS1=800, wTS2=200, wSJW=200, wTDO=800
    ),  # SP = 80,0%
    5000000: structures.CANBTP(
        dwMode=0, dwBPS=5000000, wTS1=600, wTS2=200, wSJW=200, wTDO=600
    ),  # SP = 75,0%
    6666666: structures.CANBTP(
        dwMode=0, dwBPS=6666666, wTS1=400, wTS2=200, wSJW=200, wTDO=402
    ),  # SP = 66,7%
    8000000: structures.CANBTP(
        dwMode=0, dwBPS=8000000, wTS1=400, wTS2=100, wSJW=100, wTDO=250
    ),  # SP = 80,0%
    10000000: structures.CANBTP(
        dwMode=0, dwBPS=10000000, wTS1=300, wTS2=100, wSJW=100, wTDO=200
    ),  # SP = 75,0%
}
