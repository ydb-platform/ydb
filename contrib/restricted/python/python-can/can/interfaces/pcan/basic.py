#  PCANBasic.py
#
#  ~~~~~~~~~~~~
#
#  PCAN-Basic API
#
#  ~~~~~~~~~~~~
#
#  ------------------------------------------------------------------
#  Author : Keneth Wagner
#  Last change: 2022-07-06
#  ------------------------------------------------------------------
#
#  Copyright (C) 1999-2022  PEAK-System Technik GmbH, Darmstadt
#  more Info at http://www.peak-system.com

# Module Imports
import logging
import platform
from ctypes import *
from ctypes.util import find_library

PLATFORM = platform.system()
IS_WINDOWS = PLATFORM == "Windows"
IS_LINUX = PLATFORM == "Linux"

logger = logging.getLogger("can.pcan")

# ///////////////////////////////////////////////////////////
# Type definitions
# ///////////////////////////////////////////////////////////

TPCANHandle = c_ushort  # Represents a PCAN hardware channel handle
TPCANStatus = int  # Represents a PCAN status/error code
TPCANParameter = c_ubyte  # Represents a PCAN parameter to be read or set
TPCANDevice = c_ubyte  # Represents a PCAN device
TPCANMessageType = c_ubyte  # Represents the type of a PCAN message
TPCANType = c_ubyte  # Represents the type of PCAN hardware to be initialized
TPCANMode = c_ubyte  # Represents a PCAN filter mode
TPCANBaudrate = c_ushort  # Represents a PCAN Baud rate register value
TPCANBitrateFD = c_char_p  # Represents a PCAN-FD bit rate string
TPCANTimestampFD = c_ulonglong  # Represents a timestamp of a received PCAN FD message

# ///////////////////////////////////////////////////////////
# Value definitions
# ///////////////////////////////////////////////////////////

# Currently defined and supported PCAN channels
#
PCAN_NONEBUS = TPCANHandle(0x00)  # Undefined/default value for a PCAN bus

PCAN_ISABUS1 = TPCANHandle(0x21)  # PCAN-ISA interface, channel 1
PCAN_ISABUS2 = TPCANHandle(0x22)  # PCAN-ISA interface, channel 2
PCAN_ISABUS3 = TPCANHandle(0x23)  # PCAN-ISA interface, channel 3
PCAN_ISABUS4 = TPCANHandle(0x24)  # PCAN-ISA interface, channel 4
PCAN_ISABUS5 = TPCANHandle(0x25)  # PCAN-ISA interface, channel 5
PCAN_ISABUS6 = TPCANHandle(0x26)  # PCAN-ISA interface, channel 6
PCAN_ISABUS7 = TPCANHandle(0x27)  # PCAN-ISA interface, channel 7
PCAN_ISABUS8 = TPCANHandle(0x28)  # PCAN-ISA interface, channel 8

PCAN_DNGBUS1 = TPCANHandle(0x31)  # PCAN-Dongle/LPT interface, channel 1

PCAN_PCIBUS1 = TPCANHandle(0x41)  # PCAN-PCI interface, channel 1
PCAN_PCIBUS2 = TPCANHandle(0x42)  # PCAN-PCI interface, channel 2
PCAN_PCIBUS3 = TPCANHandle(0x43)  # PCAN-PCI interface, channel 3
PCAN_PCIBUS4 = TPCANHandle(0x44)  # PCAN-PCI interface, channel 4
PCAN_PCIBUS5 = TPCANHandle(0x45)  # PCAN-PCI interface, channel 5
PCAN_PCIBUS6 = TPCANHandle(0x46)  # PCAN-PCI interface, channel 6
PCAN_PCIBUS7 = TPCANHandle(0x47)  # PCAN-PCI interface, channel 7
PCAN_PCIBUS8 = TPCANHandle(0x48)  # PCAN-PCI interface, channel 8
PCAN_PCIBUS9 = TPCANHandle(0x409)  # PCAN-PCI interface, channel 9
PCAN_PCIBUS10 = TPCANHandle(0x40A)  # PCAN-PCI interface, channel 10
PCAN_PCIBUS11 = TPCANHandle(0x40B)  # PCAN-PCI interface, channel 11
PCAN_PCIBUS12 = TPCANHandle(0x40C)  # PCAN-PCI interface, channel 12
PCAN_PCIBUS13 = TPCANHandle(0x40D)  # PCAN-PCI interface, channel 13
PCAN_PCIBUS14 = TPCANHandle(0x40E)  # PCAN-PCI interface, channel 14
PCAN_PCIBUS15 = TPCANHandle(0x40F)  # PCAN-PCI interface, channel 15
PCAN_PCIBUS16 = TPCANHandle(0x410)  # PCAN-PCI interface, channel 16

PCAN_USBBUS1 = TPCANHandle(0x51)  # PCAN-USB interface, channel 1
PCAN_USBBUS2 = TPCANHandle(0x52)  # PCAN-USB interface, channel 2
PCAN_USBBUS3 = TPCANHandle(0x53)  # PCAN-USB interface, channel 3
PCAN_USBBUS4 = TPCANHandle(0x54)  # PCAN-USB interface, channel 4
PCAN_USBBUS5 = TPCANHandle(0x55)  # PCAN-USB interface, channel 5
PCAN_USBBUS6 = TPCANHandle(0x56)  # PCAN-USB interface, channel 6
PCAN_USBBUS7 = TPCANHandle(0x57)  # PCAN-USB interface, channel 7
PCAN_USBBUS8 = TPCANHandle(0x58)  # PCAN-USB interface, channel 8
PCAN_USBBUS9 = TPCANHandle(0x509)  # PCAN-USB interface, channel 9
PCAN_USBBUS10 = TPCANHandle(0x50A)  # PCAN-USB interface, channel 10
PCAN_USBBUS11 = TPCANHandle(0x50B)  # PCAN-USB interface, channel 11
PCAN_USBBUS12 = TPCANHandle(0x50C)  # PCAN-USB interface, channel 12
PCAN_USBBUS13 = TPCANHandle(0x50D)  # PCAN-USB interface, channel 13
PCAN_USBBUS14 = TPCANHandle(0x50E)  # PCAN-USB interface, channel 14
PCAN_USBBUS15 = TPCANHandle(0x50F)  # PCAN-USB interface, channel 15
PCAN_USBBUS16 = TPCANHandle(0x510)  # PCAN-USB interface, channel 16

PCAN_PCCBUS1 = TPCANHandle(0x61)  # PCAN-PC Card interface, channel 1
PCAN_PCCBUS2 = TPCANHandle(0x62)  # PCAN-PC Card interface, channel 2

PCAN_LANBUS1 = TPCANHandle(0x801)  # PCAN-LAN interface, channel 1
PCAN_LANBUS2 = TPCANHandle(0x802)  # PCAN-LAN interface, channel 2
PCAN_LANBUS3 = TPCANHandle(0x803)  # PCAN-LAN interface, channel 3
PCAN_LANBUS4 = TPCANHandle(0x804)  # PCAN-LAN interface, channel 4
PCAN_LANBUS5 = TPCANHandle(0x805)  # PCAN-LAN interface, channel 5
PCAN_LANBUS6 = TPCANHandle(0x806)  # PCAN-LAN interface, channel 6
PCAN_LANBUS7 = TPCANHandle(0x807)  # PCAN-LAN interface, channel 7
PCAN_LANBUS8 = TPCANHandle(0x808)  # PCAN-LAN interface, channel 8
PCAN_LANBUS9 = TPCANHandle(0x809)  # PCAN-LAN interface, channel 9
PCAN_LANBUS10 = TPCANHandle(0x80A)  # PCAN-LAN interface, channel 10
PCAN_LANBUS11 = TPCANHandle(0x80B)  # PCAN-LAN interface, channel 11
PCAN_LANBUS12 = TPCANHandle(0x80C)  # PCAN-LAN interface, channel 12
PCAN_LANBUS13 = TPCANHandle(0x80D)  # PCAN-LAN interface, channel 13
PCAN_LANBUS14 = TPCANHandle(0x80E)  # PCAN-LAN interface, channel 14
PCAN_LANBUS15 = TPCANHandle(0x80F)  # PCAN-LAN interface, channel 15
PCAN_LANBUS16 = TPCANHandle(0x810)  # PCAN-LAN interface, channel 16

# Represent the PCAN error and status codes
#
PCAN_ERROR_OK = TPCANStatus(0x00000)  # No error
PCAN_ERROR_XMTFULL = TPCANStatus(0x00001)  # Transmit buffer in CAN controller is full
PCAN_ERROR_OVERRUN = TPCANStatus(0x00002)  # CAN controller was read too late
PCAN_ERROR_BUSLIGHT = TPCANStatus(
    0x00004
)  # Bus error: an error counter reached the 'light' limit
PCAN_ERROR_BUSHEAVY = TPCANStatus(
    0x00008
)  # Bus error: an error counter reached the 'heavy' limit
PCAN_ERROR_BUSWARNING = TPCANStatus(
    PCAN_ERROR_BUSHEAVY
)  # Bus error: an error counter reached the 'warning' limit
PCAN_ERROR_BUSPASSIVE = TPCANStatus(
    0x40000
)  # Bus error: the CAN controller is error passive
PCAN_ERROR_BUSOFF = TPCANStatus(
    0x00010
)  # Bus error: the CAN controller is in bus-off state
PCAN_ERROR_ANYBUSERR = TPCANStatus(
    PCAN_ERROR_BUSWARNING
    | PCAN_ERROR_BUSLIGHT
    | PCAN_ERROR_BUSHEAVY
    | PCAN_ERROR_BUSOFF
    | PCAN_ERROR_BUSPASSIVE
)  # Mask for all bus errors
PCAN_ERROR_QRCVEMPTY = TPCANStatus(0x00020)  # Receive queue is empty
PCAN_ERROR_QOVERRUN = TPCANStatus(0x00040)  # Receive queue was read too late
PCAN_ERROR_QXMTFULL = TPCANStatus(0x00080)  # Transmit queue is full
PCAN_ERROR_REGTEST = TPCANStatus(
    0x00100
)  # Test of the CAN controller hardware registers failed (no hardware found)
PCAN_ERROR_NODRIVER = TPCANStatus(0x00200)  # Driver not loaded
PCAN_ERROR_HWINUSE = TPCANStatus(0x00400)  # Hardware already in use by a Net
PCAN_ERROR_NETINUSE = TPCANStatus(0x00800)  # A Client is already connected to the Net
PCAN_ERROR_ILLHW = TPCANStatus(0x01400)  # Hardware handle is invalid
PCAN_ERROR_ILLNET = TPCANStatus(0x01800)  # Net handle is invalid
PCAN_ERROR_ILLCLIENT = TPCANStatus(0x01C00)  # Client handle is invalid
PCAN_ERROR_ILLHANDLE = TPCANStatus(
    PCAN_ERROR_ILLHW | PCAN_ERROR_ILLNET | PCAN_ERROR_ILLCLIENT
)  # Mask for all handle errors
PCAN_ERROR_RESOURCE = TPCANStatus(
    0x02000
)  # Resource (FIFO, Client, timeout) cannot be created
PCAN_ERROR_ILLPARAMTYPE = TPCANStatus(0x04000)  # Invalid parameter
PCAN_ERROR_ILLPARAMVAL = TPCANStatus(0x08000)  # Invalid parameter value
PCAN_ERROR_UNKNOWN = TPCANStatus(0x10000)  # Unknown error
PCAN_ERROR_ILLDATA = TPCANStatus(0x20000)  # Invalid data, function, or action
PCAN_ERROR_ILLMODE = TPCANStatus(
    0x80000
)  # Driver object state is wrong for the attempted operation
PCAN_ERROR_CAUTION = TPCANStatus(
    0x2000000
)  # An operation was successfully carried out, however, irregularities were registered
PCAN_ERROR_INITIALIZE = TPCANStatus(
    0x4000000
)  # Channel is not initialized [Value was changed from 0x40000 to 0x4000000]
PCAN_ERROR_ILLOPERATION = TPCANStatus(
    0x8000000
)  # Invalid operation [Value was changed from 0x80000 to 0x8000000]

# PCAN devices
#
PCAN_NONE = TPCANDevice(0x00)  # Undefined, unknown or not selected PCAN device value
PCAN_PEAKCAN = TPCANDevice(0x01)  # PCAN Non-PnP devices. NOT USED WITHIN PCAN-Basic API
PCAN_ISA = TPCANDevice(0x02)  # PCAN-ISA, PCAN-PC/104, and PCAN-PC/104-Plus
PCAN_DNG = TPCANDevice(0x03)  # PCAN-Dongle
PCAN_PCI = TPCANDevice(0x04)  # PCAN-PCI, PCAN-cPCI, PCAN-miniPCI, and PCAN-PCI Express
PCAN_USB = TPCANDevice(0x05)  # PCAN-USB and PCAN-USB Pro
PCAN_PCC = TPCANDevice(0x06)  # PCAN-PC Card
PCAN_VIRTUAL = TPCANDevice(
    0x07
)  # PCAN Virtual hardware. NOT USED WITHIN PCAN-Basic API
PCAN_LAN = TPCANDevice(0x08)  # PCAN Gateway devices

# PCAN parameters
#
PCAN_DEVICE_ID = TPCANParameter(0x01)  # Device identifier parameter
PCAN_5VOLTS_POWER = TPCANParameter(0x02)  # 5-Volt power parameter
PCAN_RECEIVE_EVENT = TPCANParameter(0x03)  # PCAN receive event handler parameter
PCAN_MESSAGE_FILTER = TPCANParameter(0x04)  # PCAN message filter parameter
PCAN_API_VERSION = TPCANParameter(0x05)  # PCAN-Basic API version parameter
PCAN_CHANNEL_VERSION = TPCANParameter(0x06)  # PCAN device channel version parameter
PCAN_BUSOFF_AUTORESET = TPCANParameter(0x07)  # PCAN Reset-On-Busoff parameter
PCAN_LISTEN_ONLY = TPCANParameter(0x08)  # PCAN Listen-Only parameter
PCAN_LOG_LOCATION = TPCANParameter(0x09)  # Directory path for log files
PCAN_LOG_STATUS = TPCANParameter(0x0A)  # Debug-Log activation status
PCAN_LOG_CONFIGURE = TPCANParameter(
    0x0B
)  # Configuration of the debugged information (LOG_FUNCTION_***)
PCAN_LOG_TEXT = TPCANParameter(0x0C)  # Custom insertion of text into the log file
PCAN_CHANNEL_CONDITION = TPCANParameter(0x0D)  # Availability status of a PCAN-Channel
PCAN_HARDWARE_NAME = TPCANParameter(0x0E)  # PCAN hardware name parameter
PCAN_RECEIVE_STATUS = TPCANParameter(0x0F)  # Message reception status of a PCAN-Channel
PCAN_CONTROLLER_NUMBER = TPCANParameter(0x10)  # CAN-Controller number of a PCAN-Channel
PCAN_TRACE_LOCATION = TPCANParameter(0x11)  # Directory path for PCAN trace files
PCAN_TRACE_STATUS = TPCANParameter(0x12)  # CAN tracing activation status
PCAN_TRACE_SIZE = TPCANParameter(
    0x13
)  # Configuration of the maximum file size of a CAN trace
PCAN_TRACE_CONFIGURE = TPCANParameter(
    0x14
)  # Configuration of the trace file storing mode (TRACE_FILE_***)
PCAN_CHANNEL_IDENTIFYING = TPCANParameter(
    0x15
)  # Physical identification of a USB based PCAN-Channel by blinking its associated LED
PCAN_CHANNEL_FEATURES = TPCANParameter(
    0x16
)  # Capabilities of a PCAN device (FEATURE_***)
PCAN_BITRATE_ADAPTING = TPCANParameter(
    0x17
)  # Using of an existing bit rate (PCAN-View connected to a channel)
PCAN_BITRATE_INFO = TPCANParameter(0x18)  # Configured bit rate as Btr0Btr1 value
PCAN_BITRATE_INFO_FD = TPCANParameter(
    0x19
)  # Configured bit rate as TPCANBitrateFD string
PCAN_BUSSPEED_NOMINAL = TPCANParameter(
    0x1A
)  # Configured nominal CAN Bus speed as Bits per seconds
PCAN_BUSSPEED_DATA = TPCANParameter(
    0x1B
)  # Configured CAN data speed as Bits per seconds
PCAN_IP_ADDRESS = TPCANParameter(
    0x1C
)  # Remote address of a LAN channel as string in IPv4 format
PCAN_LAN_SERVICE_STATUS = TPCANParameter(
    0x1D
)  # Status of the Virtual PCAN-Gateway Service
PCAN_ALLOW_STATUS_FRAMES = TPCANParameter(
    0x1E
)  # Status messages reception status within a PCAN-Channel
PCAN_ALLOW_RTR_FRAMES = TPCANParameter(
    0x1F
)  # RTR messages reception status within a PCAN-Channel
PCAN_ALLOW_ERROR_FRAMES = TPCANParameter(
    0x20
)  # Error messages reception status within a PCAN-Channel
PCAN_INTERFRAME_DELAY = TPCANParameter(
    0x21
)  # Delay, in microseconds, between sending frames
PCAN_ACCEPTANCE_FILTER_11BIT = TPCANParameter(
    0x22
)  # Filter over code and mask patterns for 11-Bit messages
PCAN_ACCEPTANCE_FILTER_29BIT = TPCANParameter(
    0x23
)  # Filter over code and mask patterns for 29-Bit messages
PCAN_IO_DIGITAL_CONFIGURATION = TPCANParameter(
    0x24
)  # Output mode of 32 digital I/O pin of a PCAN-USB Chip. 1: Output-Active 0 : Output Inactive
PCAN_IO_DIGITAL_VALUE = TPCANParameter(
    0x25
)  # Value assigned to a 32 digital I/O pins of a PCAN-USB Chip
PCAN_IO_DIGITAL_SET = TPCANParameter(
    0x26
)  # Value assigned to a 32 digital I/O pins of a PCAN-USB Chip - Multiple digital I/O pins to 1 = High
PCAN_IO_DIGITAL_CLEAR = TPCANParameter(0x27)  # Clear multiple digital I/O pins to 0
PCAN_IO_ANALOG_VALUE = TPCANParameter(0x28)  # Get value of a single analog input pin
PCAN_FIRMWARE_VERSION = TPCANParameter(
    0x29
)  # Get the version of the firmware used by the device associated with a PCAN-Channel
PCAN_ATTACHED_CHANNELS_COUNT = TPCANParameter(
    0x2A
)  # Get the amount of PCAN channels attached to a system
PCAN_ATTACHED_CHANNELS = TPCANParameter(
    0x2B
)  # Get information about PCAN channels attached to a system
PCAN_ALLOW_ECHO_FRAMES = TPCANParameter(
    0x2C
)  # Echo messages reception status within a PCAN-Channel
PCAN_DEVICE_PART_NUMBER = TPCANParameter(
    0x2D
)  # Get the part number associated to a device

# DEPRECATED parameters
#
PCAN_DEVICE_NUMBER = PCAN_DEVICE_ID  # DEPRECATED. Use PCAN_DEVICE_ID instead

# PCAN parameter values
#
PCAN_PARAMETER_OFF = 0x00  # The PCAN parameter is not set (inactive)
PCAN_PARAMETER_ON = 0x01  # The PCAN parameter is set (active)
PCAN_FILTER_CLOSE = 0x00  # The PCAN filter is closed. No messages will be received
PCAN_FILTER_OPEN = (
    0x01  # The PCAN filter is fully opened. All messages will be received
)
PCAN_FILTER_CUSTOM = 0x02  # The PCAN filter is custom configured. Only registered messages will be received
PCAN_CHANNEL_UNAVAILABLE = 0x00  # The PCAN-Channel handle is illegal, or its associated hardware is not available
PCAN_CHANNEL_AVAILABLE = 0x01  # The PCAN-Channel handle is available to be connected (PnP Hardware: it means furthermore that the hardware is plugged-in)
PCAN_CHANNEL_OCCUPIED = (
    0x02  # The PCAN-Channel handle is valid, and is already being used
)
PCAN_CHANNEL_PCANVIEW = (
    PCAN_CHANNEL_AVAILABLE | PCAN_CHANNEL_OCCUPIED
)  # The PCAN-Channel handle is already being used by a PCAN-View application, but is available to connect

LOG_FUNCTION_DEFAULT = 0x00  # Logs system exceptions / errors
LOG_FUNCTION_ENTRY = 0x01  # Logs the entries to the PCAN-Basic API functions
LOG_FUNCTION_PARAMETERS = (
    0x02  # Logs the parameters passed to the PCAN-Basic API functions
)
LOG_FUNCTION_LEAVE = 0x04  # Logs the exits from the PCAN-Basic API functions
LOG_FUNCTION_WRITE = 0x08  # Logs the CAN messages passed to the CAN_Write function
LOG_FUNCTION_READ = 0x10  # Logs the CAN messages received within the CAN_Read function
LOG_FUNCTION_ALL = (
    0xFFFF  # Logs all possible information within the PCAN-Basic API functions
)

TRACE_FILE_SINGLE = (
    0x00  # A single file is written until it size reaches PAN_TRACE_SIZE
)
TRACE_FILE_SEGMENTED = (
    0x01  # Traced data is distributed in several files with size PAN_TRACE_SIZE
)
TRACE_FILE_DATE = 0x02  # Includes the date into the name of the trace file
TRACE_FILE_TIME = 0x04  # Includes the start time into the name of the trace file
TRACE_FILE_OVERWRITE = 0x80  # Causes the overwriting of available traces (same name)

FEATURE_FD_CAPABLE = 0x01  # Device supports flexible data-rate (CAN-FD)
FEATURE_DELAY_CAPABLE = (
    0x02  # Device supports a delay between sending frames (FPGA based USB devices)
)
FEATURE_IO_CAPABLE = (
    0x04  # Device supports I/O functionality for electronic circuits (USB-Chip devices)
)

SERVICE_STATUS_STOPPED = 0x01  # The service is not running
SERVICE_STATUS_RUNNING = 0x04  # The service is running

# Other constants
#
MAX_LENGTH_HARDWARE_NAME = (
    33  # Maximum length of the name of a device: 32 characters + terminator
)
MAX_LENGTH_VERSION_STRING = (
    256  # Maximum length of a version string: 255 characters + terminator
)

# PCAN message types
#
PCAN_MESSAGE_STANDARD = TPCANMessageType(
    0x00
)  # The PCAN message is a CAN Standard Frame (11-bit identifier)
PCAN_MESSAGE_RTR = TPCANMessageType(
    0x01
)  # The PCAN message is a CAN Remote-Transfer-Request Frame
PCAN_MESSAGE_EXTENDED = TPCANMessageType(
    0x02
)  # The PCAN message is a CAN Extended Frame (29-bit identifier)
PCAN_MESSAGE_FD = TPCANMessageType(
    0x04
)  # The PCAN message represents a FD frame in terms of CiA Specs
PCAN_MESSAGE_BRS = TPCANMessageType(
    0x08
)  # The PCAN message represents a FD bit rate switch (CAN data at a higher bit rate)
PCAN_MESSAGE_ESI = TPCANMessageType(
    0x10
)  # The PCAN message represents a FD error state indicator(CAN FD transmitter was error active)
PCAN_MESSAGE_ECHO = TPCANMessageType(
    0x20
)  # The PCAN message represents an echo CAN Frame
PCAN_MESSAGE_ERRFRAME = TPCANMessageType(
    0x40
)  # The PCAN message represents an error frame
PCAN_MESSAGE_STATUS = TPCANMessageType(
    0x80
)  # The PCAN message represents a PCAN status message

# LookUp Parameters
#
LOOKUP_DEVICE_TYPE = (
    b"devicetype"  # Lookup channel by Device type (see PCAN devices e.g. PCAN_USB)
)
LOOKUP_DEVICE_ID = b"deviceid"  # Lookup channel by device id
LOOKUP_CONTROLLER_NUMBER = (
    b"controllernumber"  # Lookup channel by CAN controller 0-based index
)
LOOKUP_IP_ADDRESS = b"ipaddress"  # Lookup channel by IP address (LAN channels only)

# Frame Type / Initialization Mode
#
PCAN_MODE_STANDARD = PCAN_MESSAGE_STANDARD
PCAN_MODE_EXTENDED = PCAN_MESSAGE_EXTENDED

# Baud rate codes = BTR0/BTR1 register values for the CAN controller.
# You can define your own Baud rate with the BTROBTR1 register.
# Take a look at www.peak-system.com for our free software "BAUDTOOL"
# to calculate the BTROBTR1 register for every bit rate and sample point.
#
PCAN_BAUD_1M = TPCANBaudrate(0x0014)  #   1 MBit/s
PCAN_BAUD_800K = TPCANBaudrate(0x0016)  # 800 kBit/s
PCAN_BAUD_500K = TPCANBaudrate(0x001C)  # 500 kBit/s
PCAN_BAUD_250K = TPCANBaudrate(0x011C)  # 250 kBit/s
PCAN_BAUD_125K = TPCANBaudrate(0x031C)  # 125 kBit/s
PCAN_BAUD_100K = TPCANBaudrate(0x432F)  # 100 kBit/s
PCAN_BAUD_95K = TPCANBaudrate(0xC34E)  #  95,238 kBit/s
PCAN_BAUD_83K = TPCANBaudrate(0x852B)  #  83,333 kBit/s
PCAN_BAUD_50K = TPCANBaudrate(0x472F)  #  50 kBit/s
PCAN_BAUD_47K = TPCANBaudrate(0x1414)  #  47,619 kBit/s
PCAN_BAUD_33K = TPCANBaudrate(0x8B2F)  #  33,333 kBit/s
PCAN_BAUD_20K = TPCANBaudrate(0x532F)  #  20 kBit/s
PCAN_BAUD_10K = TPCANBaudrate(0x672F)  #  10 kBit/s
PCAN_BAUD_5K = TPCANBaudrate(0x7F7F)  #   5 kBit/s

# Represents the configuration for a CAN bit rate
# Note:
#    * Each parameter and its value must be separated with a '='.
#    * Each pair of parameter/value must be separated using ','.
#
# Example:
#    f_clock=80000000,nom_brp=10,nom_tseg1=5,nom_tseg2=2,nom_sjw=1,data_brp=4,data_tseg1=7,data_tseg2=2,data_sjw=1
#
PCAN_BR_CLOCK = TPCANBitrateFD(b"f_clock")
PCAN_BR_CLOCK_MHZ = TPCANBitrateFD(b"f_clock_mhz")
PCAN_BR_NOM_BRP = TPCANBitrateFD(b"nom_brp")
PCAN_BR_NOM_TSEG1 = TPCANBitrateFD(b"nom_tseg1")
PCAN_BR_NOM_TSEG2 = TPCANBitrateFD(b"nom_tseg2")
PCAN_BR_NOM_SJW = TPCANBitrateFD(b"nom_sjw")
PCAN_BR_NOM_SAMPLE = TPCANBitrateFD(b"nom_sam")
PCAN_BR_DATA_BRP = TPCANBitrateFD(b"data_brp")
PCAN_BR_DATA_TSEG1 = TPCANBitrateFD(b"data_tseg1")
PCAN_BR_DATA_TSEG2 = TPCANBitrateFD(b"data_tseg2")
PCAN_BR_DATA_SJW = TPCANBitrateFD(b"data_sjw")
PCAN_BR_DATA_SAMPLE = TPCANBitrateFD(b"data_ssp_offset")

# Supported Non-PnP Hardware types
#
PCAN_TYPE_ISA = TPCANType(0x01)  # PCAN-ISA 82C200
PCAN_TYPE_ISA_SJA = TPCANType(0x09)  # PCAN-ISA SJA1000
PCAN_TYPE_ISA_PHYTEC = TPCANType(0x04)  # PHYTEC ISA
PCAN_TYPE_DNG = TPCANType(0x02)  # PCAN-Dongle 82C200
PCAN_TYPE_DNG_EPP = TPCANType(0x03)  # PCAN-Dongle EPP 82C200
PCAN_TYPE_DNG_SJA = TPCANType(0x05)  # PCAN-Dongle SJA1000
PCAN_TYPE_DNG_SJA_EPP = TPCANType(0x06)  # PCAN-Dongle EPP SJA1000

# string description of the error codes
PCAN_DICT_STATUS = {
    PCAN_ERROR_OK: "OK",
    PCAN_ERROR_XMTFULL: "XMTFULL",
    PCAN_ERROR_OVERRUN: "OVERRUN",
    PCAN_ERROR_BUSLIGHT: "BUSLIGHT",
    PCAN_ERROR_BUSHEAVY: "BUSHEAVY",
    PCAN_ERROR_BUSWARNING: "BUSWARNING",
    PCAN_ERROR_BUSPASSIVE: "BUSPASSIVE",
    PCAN_ERROR_BUSOFF: "BUSOFF",
    PCAN_ERROR_ANYBUSERR: "ANYBUSERR",
    PCAN_ERROR_QRCVEMPTY: "QRCVEMPTY",
    PCAN_ERROR_QOVERRUN: "QOVERRUN",
    PCAN_ERROR_QXMTFULL: "QXMTFULL",
    PCAN_ERROR_REGTEST: "ERR_REGTEST",
    PCAN_ERROR_NODRIVER: "NODRIVER",
    PCAN_ERROR_HWINUSE: "HWINUSE",
    PCAN_ERROR_NETINUSE: "NETINUSE",
    PCAN_ERROR_ILLHW: "ILLHW",
    PCAN_ERROR_ILLNET: "ILLNET",
    PCAN_ERROR_ILLCLIENT: "ILLCLIENT",
    PCAN_ERROR_ILLHANDLE: "ILLHANDLE",
    PCAN_ERROR_RESOURCE: "ERR_RESOURCE",
    PCAN_ERROR_ILLPARAMTYPE: "ILLPARAMTYPE",
    PCAN_ERROR_ILLPARAMVAL: "ILLPARAMVAL",
    PCAN_ERROR_UNKNOWN: "UNKNOWN",
    PCAN_ERROR_ILLDATA: "ILLDATA",
    PCAN_ERROR_CAUTION: "CAUTION",
    PCAN_ERROR_INITIALIZE: "ERR_INITIALIZE",
    PCAN_ERROR_ILLOPERATION: "ILLOPERATION",
}


# Represents a PCAN message
#
class TPCANMsg(Structure):
    """
    Represents a PCAN message
    """

    _fields_ = [
        ("ID", c_uint),  # 11/29-bit message identifier
        ("MSGTYPE", TPCANMessageType),  # Type of the message
        ("LEN", c_ubyte),  # Data Length Code of the message (0..8)
        ("DATA", c_ubyte * 8),
    ]  # Data of the message (DATA[0]..DATA[7])


# Represents a timestamp of a received PCAN message
# Total Microseconds = micros + 1000 * millis + 0x100000000 * 1000 * millis_overflow
#
class TPCANTimestamp(Structure):
    """
    Represents a timestamp of a received PCAN message
    Total Microseconds = micros + 1000 * millis + 0x100000000 * 1000 * millis_overflow
    """

    _fields_ = [
        ("millis", c_uint),  # Base-value: milliseconds: 0.. 2^32-1
        ("millis_overflow", c_ushort),  # Roll-arounds of millis
        ("micros", c_ushort),
    ]  # Microseconds: 0..999


# Represents a PCAN message from a FD capable hardware
#
class TPCANMsgFD(Structure):
    """
    Represents a PCAN message
    """

    _fields_ = [
        ("ID", c_uint),  # 11/29-bit message identifier
        ("MSGTYPE", TPCANMessageType),  # Type of the message
        ("DLC", c_ubyte),  # Data Length Code of the message (0..15)
        ("DATA", c_ubyte * 64),
    ]  # Data of the message (DATA[0]..DATA[63])


# Describes an available PCAN channel
#
class TPCANChannelInformation(Structure):
    """
    Describes an available PCAN channel
    """

    _fields_ = [
        ("channel_handle", TPCANHandle),  # PCAN channel handle
        ("device_type", TPCANDevice),  # Kind of PCAN device
        ("controller_number", c_ubyte),  # CAN-Controller number
        ("device_features", c_uint),  # Device capabilities flag (see FEATURE_*)
        ("device_name", c_char * MAX_LENGTH_HARDWARE_NAME),  # Device name
        ("device_id", c_uint),  # Device number
        ("channel_condition", c_uint),
    ]  # Availability status of a PCAN-Channel


# ///////////////////////////////////////////////////////////
# Additional objects
# ///////////////////////////////////////////////////////////

PCAN_BITRATES = {
    1000000: PCAN_BAUD_1M,
    800000: PCAN_BAUD_800K,
    500000: PCAN_BAUD_500K,
    250000: PCAN_BAUD_250K,
    125000: PCAN_BAUD_125K,
    100000: PCAN_BAUD_100K,
    95000: PCAN_BAUD_95K,
    83000: PCAN_BAUD_83K,
    50000: PCAN_BAUD_50K,
    47000: PCAN_BAUD_47K,
    33000: PCAN_BAUD_33K,
    20000: PCAN_BAUD_20K,
    10000: PCAN_BAUD_10K,
    5000: PCAN_BAUD_5K,
}

PCAN_FD_PARAMETER_LIST = (
    "nom_brp",
    "nom_tseg1",
    "nom_tseg2",
    "nom_sjw",
    "data_brp",
    "data_tseg1",
    "data_tseg2",
    "data_sjw",
)

PCAN_CHANNEL_NAMES = {
    "PCAN_NONEBUS": PCAN_NONEBUS,
    "PCAN_ISABUS1": PCAN_ISABUS1,
    "PCAN_ISABUS2": PCAN_ISABUS2,
    "PCAN_ISABUS3": PCAN_ISABUS3,
    "PCAN_ISABUS4": PCAN_ISABUS4,
    "PCAN_ISABUS5": PCAN_ISABUS5,
    "PCAN_ISABUS6": PCAN_ISABUS6,
    "PCAN_ISABUS7": PCAN_ISABUS7,
    "PCAN_ISABUS8": PCAN_ISABUS8,
    "PCAN_DNGBUS1": PCAN_DNGBUS1,
    "PCAN_PCIBUS1": PCAN_PCIBUS1,
    "PCAN_PCIBUS2": PCAN_PCIBUS2,
    "PCAN_PCIBUS3": PCAN_PCIBUS3,
    "PCAN_PCIBUS4": PCAN_PCIBUS4,
    "PCAN_PCIBUS5": PCAN_PCIBUS5,
    "PCAN_PCIBUS6": PCAN_PCIBUS6,
    "PCAN_PCIBUS7": PCAN_PCIBUS7,
    "PCAN_PCIBUS8": PCAN_PCIBUS8,
    "PCAN_PCIBUS9": PCAN_PCIBUS9,
    "PCAN_PCIBUS10": PCAN_PCIBUS10,
    "PCAN_PCIBUS11": PCAN_PCIBUS11,
    "PCAN_PCIBUS12": PCAN_PCIBUS12,
    "PCAN_PCIBUS13": PCAN_PCIBUS13,
    "PCAN_PCIBUS14": PCAN_PCIBUS14,
    "PCAN_PCIBUS15": PCAN_PCIBUS15,
    "PCAN_PCIBUS16": PCAN_PCIBUS16,
    "PCAN_USBBUS1": PCAN_USBBUS1,
    "PCAN_USBBUS2": PCAN_USBBUS2,
    "PCAN_USBBUS3": PCAN_USBBUS3,
    "PCAN_USBBUS4": PCAN_USBBUS4,
    "PCAN_USBBUS5": PCAN_USBBUS5,
    "PCAN_USBBUS6": PCAN_USBBUS6,
    "PCAN_USBBUS7": PCAN_USBBUS7,
    "PCAN_USBBUS8": PCAN_USBBUS8,
    "PCAN_USBBUS9": PCAN_USBBUS9,
    "PCAN_USBBUS10": PCAN_USBBUS10,
    "PCAN_USBBUS11": PCAN_USBBUS11,
    "PCAN_USBBUS12": PCAN_USBBUS12,
    "PCAN_USBBUS13": PCAN_USBBUS13,
    "PCAN_USBBUS14": PCAN_USBBUS14,
    "PCAN_USBBUS15": PCAN_USBBUS15,
    "PCAN_USBBUS16": PCAN_USBBUS16,
    "PCAN_PCCBUS1": PCAN_PCCBUS1,
    "PCAN_PCCBUS2": PCAN_PCCBUS2,
    "PCAN_LANBUS1": PCAN_LANBUS1,
    "PCAN_LANBUS2": PCAN_LANBUS2,
    "PCAN_LANBUS3": PCAN_LANBUS3,
    "PCAN_LANBUS4": PCAN_LANBUS4,
    "PCAN_LANBUS5": PCAN_LANBUS5,
    "PCAN_LANBUS6": PCAN_LANBUS6,
    "PCAN_LANBUS7": PCAN_LANBUS7,
    "PCAN_LANBUS8": PCAN_LANBUS8,
    "PCAN_LANBUS9": PCAN_LANBUS9,
    "PCAN_LANBUS10": PCAN_LANBUS10,
    "PCAN_LANBUS11": PCAN_LANBUS11,
    "PCAN_LANBUS12": PCAN_LANBUS12,
    "PCAN_LANBUS13": PCAN_LANBUS13,
    "PCAN_LANBUS14": PCAN_LANBUS14,
    "PCAN_LANBUS15": PCAN_LANBUS15,
    "PCAN_LANBUS16": PCAN_LANBUS16,
}

VALID_PCAN_CAN_CLOCKS = [8_000_000]

VALID_PCAN_FD_CLOCKS = [
    20_000_000,
    24_000_000,
    30_000_000,
    40_000_000,
    60_000_000,
    80_000_000,
]

# ///////////////////////////////////////////////////////////
# PCAN-Basic API function declarations
# ///////////////////////////////////////////////////////////


# PCAN-Basic API class implementation
#
class PCANBasic:
    """PCAN-Basic API class implementation"""

    def __init__(self):
        if platform.system() == "Windows":
            load_library_func = windll.LoadLibrary
        else:
            load_library_func = cdll.LoadLibrary

        if platform.system() == "Windows" or "CYGWIN" in platform.system():
            lib_name = "PCANBasic"
        elif platform.system() == "Darwin":
            # PCBUSB library is a third-party software created
            # and maintained by the MacCAN project
            lib_name = "PCBUSB"
        else:
            lib_name = "pcanbasic"

        lib_path = find_library(lib_name)
        if not lib_path:
            raise OSError(f"{lib_name} library not found.")

        try:
            self.__m_dllBasic = load_library_func(lib_path)
        except OSError:
            raise OSError(
                f"The PCAN-Basic API could not be loaded. ({lib_path})"
            ) from None

    # Initializes a PCAN Channel
    #
    def Initialize(
        self,
        Channel,
        Btr0Btr1,
        HwType=TPCANType(0),  # noqa: B008
        IOPort=c_uint(0),  # noqa: B008
        Interrupt=c_ushort(0),  # noqa: B008
    ):
        """Initializes a PCAN Channel

        Parameters:
          Channel  : A TPCANHandle representing a PCAN Channel
          Btr0Btr1 : The speed for the communication (BTR0BTR1 code)
          HwType   : Non-PnP: The type of hardware and operation mode
          IOPort   : Non-PnP: The I/O address for the parallel port
          Interrupt: Non-PnP: Interrupt number of the parallel port

        Returns:
          A TPCANStatus error code
        """
        try:
            res = self.__m_dllBasic.CAN_Initialize(
                Channel, Btr0Btr1, HwType, IOPort, Interrupt
            )
            return TPCANStatus(res)
        except:
            logger.error("Exception on PCANBasic.Initialize")
            raise

    # Initializes a FD capable PCAN Channel
    #
    def InitializeFD(self, Channel, BitrateFD):
        """Initializes a FD capable PCAN Channel

        Parameters:
          Channel  : The handle of a FD capable PCAN Channel
          BitrateFD : The speed for the communication (FD bit rate string)

        Remarks:
          * See PCAN_BR_* values.
          * parameter and values must be separated by '='
          * Couples of Parameter/value must be separated by ','
          * Following Parameter must be filled out: f_clock, data_brp, data_sjw, data_tseg1, data_tseg2,
            nom_brp, nom_sjw, nom_tseg1, nom_tseg2.
          * Following Parameters are optional (not used yet): data_ssp_offset, nom_sam

        Example:
          f_clock=80000000,nom_brp=10,nom_tseg1=5,nom_tseg2=2,nom_sjw=1,data_brp=4,data_tseg1=7,data_tseg2=2,data_sjw=1

        Returns:
          A TPCANStatus error code
        """
        try:
            res = self.__m_dllBasic.CAN_InitializeFD(Channel, BitrateFD)
            return TPCANStatus(res)
        except:
            logger.error("Exception on PCANBasic.InitializeFD")
            raise

    #  Uninitializes one or all PCAN Channels initialized by CAN_Initialize
    #
    def Uninitialize(self, Channel):
        """Uninitializes one or all PCAN Channels initialized by CAN_Initialize

        Remarks:
          Giving the TPCANHandle value "PCAN_NONEBUS", uninitialize all initialized channels

        Parameters:
          Channel  : A TPCANHandle representing a PCAN Channel

        Returns:
          A TPCANStatus error code
        """
        try:
            res = self.__m_dllBasic.CAN_Uninitialize(Channel)
            return TPCANStatus(res)
        except:
            logger.error("Exception on PCANBasic.Uninitialize")
            raise

    #  Resets the receive and transmit queues of the PCAN Channel
    #
    def Reset(self, Channel):
        """Resets the receive and transmit queues of the PCAN Channel

        Remarks:
          A reset of the CAN controller is not performed

        Parameters:
          Channel  : A TPCANHandle representing a PCAN Channel

        Returns:
          A TPCANStatus error code
        """
        try:
            res = self.__m_dllBasic.CAN_Reset(Channel)
            return TPCANStatus(res)
        except:
            logger.error("Exception on PCANBasic.Reset")
            raise

    #  Gets the current status of a PCAN Channel
    #
    def GetStatus(self, Channel):
        """Gets the current status of a PCAN Channel

        Parameters:
          Channel  : A TPCANHandle representing a PCAN Channel

        Returns:
          A TPCANStatus error code
        """
        try:
            res = self.__m_dllBasic.CAN_GetStatus(Channel)
            return TPCANStatus(res)
        except:
            logger.error("Exception on PCANBasic.GetStatus")
            raise

    # Reads a CAN message from the receive queue of a PCAN Channel
    #
    def Read(self, Channel):
        """Reads a CAN message from the receive queue of a PCAN Channel

        Remarks:
          The return value of this method is a 3-tuple, where
          the first value is the result (TPCANStatus) of the method.
          The order of the values are:
          [0]: A TPCANStatus error code
          [1]: A TPCANMsg structure with the CAN message read
          [2]: A TPCANTimestamp structure with the time when a message was read

        Parameters:
          Channel  : A TPCANHandle representing a PCAN Channel

        Returns:
          A tuple with three values
        """
        try:
            msg = TPCANMsg()
            timestamp = TPCANTimestamp()
            res = self.__m_dllBasic.CAN_Read(Channel, byref(msg), byref(timestamp))
            return TPCANStatus(res), msg, timestamp
        except:
            logger.error("Exception on PCANBasic.Read")
            raise

    # Reads a CAN message from the receive queue of a FD capable PCAN Channel
    #
    def ReadFD(self, Channel):
        """Reads a CAN message from the receive queue of a FD capable PCAN Channel

        Remarks:
          The return value of this method is a 3-tuple, where
          the first value is the result (TPCANStatus) of the method.
          The order of the values are:
          [0]: A TPCANStatus error code
          [1]: A TPCANMsgFD structure with the CAN message read
          [2]: A TPCANTimestampFD that is the time when a message was read

        Parameters:
          Channel  : The handle of a FD capable PCAN Channel

        Returns:
          A tuple with three values
        """
        try:
            msg = TPCANMsgFD()
            timestamp = TPCANTimestampFD()
            res = self.__m_dllBasic.CAN_ReadFD(Channel, byref(msg), byref(timestamp))
            return TPCANStatus(res), msg, timestamp
        except:
            logger.error("Exception on PCANBasic.ReadFD")
            raise

    # Transmits a CAN message
    #
    def Write(self, Channel, MessageBuffer):
        """Transmits a CAN message

        Parameters:
          Channel      : A TPCANHandle representing a PCAN Channel
          MessageBuffer: A TPCANMsg representing the CAN message to be sent

        Returns:
          A TPCANStatus error code
        """
        try:
            res = self.__m_dllBasic.CAN_Write(Channel, byref(MessageBuffer))
            return TPCANStatus(res)
        except:
            logger.error("Exception on PCANBasic.Write")
            raise

    # Transmits a CAN message over a FD capable PCAN Channel
    #
    def WriteFD(self, Channel, MessageBuffer):
        """Transmits a CAN message over a FD capable PCAN Channel

        Parameters:
          Channel      : The handle of a FD capable PCAN Channel
          MessageBuffer: A TPCANMsgFD buffer with the message to be sent

        Returns:
          A TPCANStatus error code
        """
        try:
            res = self.__m_dllBasic.CAN_WriteFD(Channel, byref(MessageBuffer))
            return TPCANStatus(res)
        except:
            logger.error("Exception on PCANBasic.WriteFD")
            raise

    # Configures the reception filter
    #
    def FilterMessages(self, Channel, FromID, ToID, Mode):
        """Configures the reception filter

        Remarks:
          The message filter will be expanded with every call to this function.
          If it is desired to reset the filter, please use the 'SetValue' function.

        Parameters:
          Channel : A TPCANHandle representing a PCAN Channel
          FromID  : A c_uint value with the lowest CAN ID to be received
          ToID    : A c_uint value with the highest CAN ID to be received
          Mode    : A TPCANMode representing the message type (Standard, 11-bit
                    identifier, or Extended, 29-bit identifier)

        Returns:
          A TPCANStatus error code
        """
        try:
            res = self.__m_dllBasic.CAN_FilterMessages(Channel, FromID, ToID, Mode)
            return TPCANStatus(res)
        except:
            logger.error("Exception on PCANBasic.FilterMessages")
            raise

    # Retrieves a PCAN Channel value
    #
    def GetValue(self, Channel, Parameter):
        """Retrieves a PCAN Channel value

        Remarks:
          Parameters can be present or not according with the kind
          of Hardware (PCAN Channel) being used. If a parameter is not available,
          a PCAN_ERROR_ILLPARAMTYPE error will be returned.

          The return value of this method is a 2-tuple, where
          the first value is the result (TPCANStatus) of the method and
          the second one, the asked value

        Parameters:
          Channel   : A TPCANHandle representing a PCAN Channel
          Parameter : The TPCANParameter parameter to get

        Returns:
          A tuple with 2 values
        """
        try:
            if (
                Parameter == PCAN_API_VERSION
                or Parameter == PCAN_HARDWARE_NAME
                or Parameter == PCAN_CHANNEL_VERSION
                or Parameter == PCAN_LOG_LOCATION
                or Parameter == PCAN_TRACE_LOCATION
                or Parameter == PCAN_BITRATE_INFO_FD
                or Parameter == PCAN_IP_ADDRESS
                or Parameter == PCAN_FIRMWARE_VERSION
                or Parameter == PCAN_DEVICE_PART_NUMBER
            ):
                mybuffer = create_string_buffer(256)

            elif Parameter == PCAN_ATTACHED_CHANNELS:
                res = self.GetValue(Channel, PCAN_ATTACHED_CHANNELS_COUNT)
                if TPCANStatus(res[0]) != PCAN_ERROR_OK:
                    return TPCANStatus(res[0]), ()
                mybuffer = (TPCANChannelInformation * res[1])()

            elif (
                Parameter == PCAN_ACCEPTANCE_FILTER_11BIT
                or PCAN_ACCEPTANCE_FILTER_29BIT
            ):
                mybuffer = c_int64(0)

            else:
                mybuffer = c_int(0)

            res = self.__m_dllBasic.CAN_GetValue(
                Channel, Parameter, byref(mybuffer), sizeof(mybuffer)
            )
            if Parameter == PCAN_ATTACHED_CHANNELS:
                return TPCANStatus(res), mybuffer
            else:
                return TPCANStatus(res), mybuffer.value
        except:
            logger.error("Exception on PCANBasic.GetValue")
            raise

    # Returns a descriptive text of a given TPCANStatus
    # error code, in any desired language
    #
    def SetValue(self, Channel, Parameter, Buffer):
        """Returns a descriptive text of a given TPCANStatus error
        code, in any desired language

        Remarks:
          Parameters can be present or not according with the kind
          of Hardware (PCAN Channel) being used. If a parameter is not available,
          a PCAN_ERROR_ILLPARAMTYPE error will be returned.

        Parameters:
          Channel      : A TPCANHandle representing a PCAN Channel
          Parameter    : The TPCANParameter parameter to set
          Buffer       : Buffer with the value to be set
          BufferLength : Size in bytes of the buffer

        Returns:
          A TPCANStatus error code
        """
        try:
            if (
                Parameter == PCAN_LOG_LOCATION
                or Parameter == PCAN_LOG_TEXT
                or Parameter == PCAN_TRACE_LOCATION
            ):
                mybuffer = create_string_buffer(256)
            elif (
                Parameter == PCAN_ACCEPTANCE_FILTER_11BIT
                or PCAN_ACCEPTANCE_FILTER_29BIT
            ):
                mybuffer = c_int64(0)
            else:
                mybuffer = c_int(0)

            mybuffer.value = Buffer
            res = self.__m_dllBasic.CAN_SetValue(
                Channel, Parameter, byref(mybuffer), sizeof(mybuffer)
            )
            return TPCANStatus(res)
        except:
            logger.error("Exception on PCANBasic.SetValue")
            raise

    def GetErrorText(self, Error, Language=0):
        """Configures or sets a PCAN Channel value

        Remarks:

          The current languages available for translation are:
          Neutral (0x00), German (0x07), English (0x09), Spanish (0x0A),
          Italian (0x10) and French (0x0C)

          The return value of this method is a 2-tuple, where
          the first value is the result (TPCANStatus) of the method and
          the second one, the error text

        Parameters:
          Error    : A TPCANStatus error code
          Language : Indicates a 'Primary language ID' (Default is Neutral(0))

        Returns:
          A tuple with 2 values
        """
        try:
            mybuffer = create_string_buffer(256)
            res = self.__m_dllBasic.CAN_GetErrorText(Error, Language, byref(mybuffer))
            return TPCANStatus(res), mybuffer.value
        except:
            logger.error("Exception on PCANBasic.GetErrorText")
            raise

    def LookUpChannel(self, Parameters):
        """Finds a PCAN-Basic channel that matches with the given parameters

        Remarks:

          The return value of this method is a 2-tuple, where
          the first value is the result (TPCANStatus) of the method and
          the second one a TPCANHandle value

        Parameters:
            Parameters   : A comma separated string contained pairs of parameter-name/value
                           to be matched within a PCAN-Basic channel

        Returns:
          A tuple with 2 values
        """
        try:
            mybuffer = TPCANHandle(0)
            res = self.__m_dllBasic.CAN_LookUpChannel(Parameters, byref(mybuffer))
            return TPCANStatus(res), mybuffer
        except:
            logger.error("Exception on PCANBasic.LookUpChannel")
            raise
