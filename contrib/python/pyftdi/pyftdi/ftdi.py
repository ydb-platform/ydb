# Copyright (c) 2010-2024 Emmanuel Blot <emmanuel.blot@free.fr>
# Copyright (c) 2016 Emmanuel Bouaziz <ebouaziz@free.fr>
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

"""FTDI core driver."""

from binascii import hexlify
from collections import OrderedDict
from enum import IntEnum, unique
from errno import ENODEV
from logging import getLogger, DEBUG
from struct import unpack as sunpack
from sys import platform
from typing import Callable, Optional, List, Sequence, TextIO, Tuple, Union
from usb.core import (Configuration as UsbConfiguration, Device as UsbDevice,
                      USBError)
from usb.util import (build_request_type, release_interface, CTRL_IN, CTRL_OUT,
                      CTRL_TYPE_VENDOR, CTRL_RECIPIENT_DEVICE)
from .misc import to_bool
from .usbtools import UsbDeviceDescriptor, UsbTools

# pylint: disable=invalid-name


class FtdiError(IOError):
    """Base class error for all FTDI device"""


class FtdiFeatureError(FtdiError):
    """Requested feature is not available on FTDI device"""


class FtdiMpsseError(FtdiFeatureError):
    """MPSSE mode not supported on FTDI device"""


class FtdiEepromError(FtdiError):
    """FTDI EEPROM access errors"""


class Ftdi:
    """FTDI device driver"""

    SCHEME = 'ftdi'
    """URL scheme for :py:class:`UsbTools`."""

    FTDI_VENDOR = 0x403
    """USB VID for FTDI chips."""

    VENDOR_IDS = {'ftdi': FTDI_VENDOR}
    """Supported vendors, only FTDI.
       To add third parties vendors see :py:meth:`add_custom_vendor`.
    """

    PRODUCT_IDS = {
        FTDI_VENDOR: OrderedDict((
            # use an ordered dict so that the first occurence of a PID takes
            # precedence when generating URLs - order does matter.
            ('232', 0x6001),
            ('232r', 0x6001),
            ('232h', 0x6014),
            ('2232', 0x6010),
            ('2232c', 0x6010),
            ('2232d', 0x6010),
            ('2232h', 0x6010),
            ('4232', 0x6011),
            ('4232h', 0x6011),
            ('ft-x', 0x6015),
            ('230x', 0x6015),
            ('231x', 0x6015),
            ('234x', 0x6015),
            ('4232ha', 0x6048),
            ('ft232', 0x6001),
            ('ft232r', 0x6001),
            ('ft232h', 0x6014),
            ('ft2232', 0x6010),
            ('ft2232c', 0x6010),
            ('ft2232d', 0x6010),
            ('ft2232h', 0x6010),
            ('ft4232', 0x6011),
            ('ft4232h', 0x6011),
            ('ft230x', 0x6015),
            ('ft231x', 0x6015),
            ('ft234x', 0x6015),
            ('ft4232ha', 0x6048)))
        }
    """Supported products, only FTDI officials ones.
       To add third parties and customized products, see
       :py:meth:`add_custom_product`.
    """

    DEFAULT_VENDOR = FTDI_VENDOR
    """Default vendor: FTDI."""

    DEVICE_NAMES = {
        0x0200: 'ft232am',
        0x0400: 'ft232bm',
        0x0500: 'ft2232c',
        0x0600: 'ft232r',
        0x0700: 'ft2232h',
        0x0800: 'ft4232h',
        0x0900: 'ft232h',
        0x1000: 'ft-x',
        0x3600: 'ft4232ha'}
    """Common names of FTDI supported devices."""

    # Note that the FTDI datasheets contradict themselves, so
    # the following values may not be the right ones...
    FIFO_SIZES = {
        0x0200: (128, 128),    # FT232AM:   TX: 128, RX: 128
        0x0400: (128, 384),    # FT232BM:   TX: 128, RX: 384
        0x0500: (128, 384),    # FT2232C:   TX: 128, RX: 384
        0x0600: (256, 128),    # FT232R:    TX: 256, RX: 128
        0x0700: (4096, 4096),  # FT2232H:   TX: 4KiB, RX: 4KiB
        0x0800: (2048, 2048),  # FT4232H:   TX: 2KiB, RX: 2KiB
        0x0900: (1024, 1024),  # FT232H:    TX: 1KiB, RX: 1KiB
        0x1000: (512, 512),    # FT-X:      TX: 512, RX: 512
        0x3600: (2048, 2048),  # FT4232HA:  TX: 2KiB, RX: 2KiB
    }
    """FTDI chip internal FIFO sizes

       Note that 'TX' and 'RX' are inverted with the datasheet terminology:
       Values here are seen from the host perspective, whereas datasheet
       values are defined from the device perspective
    """

    @unique
    class BitMode(IntEnum):
        """Function selection."""

        RESET = 0x00    # switch off altnerative mode (default to UART)
        BITBANG = 0x01  # classical asynchronous bitbang mode
        MPSSE = 0x02    # MPSSE mode, available on 2232x chips
        SYNCBB = 0x04   # synchronous bitbang mode
        MCU = 0x08      # MCU Host Bus Emulation mode,
        OPTO = 0x10     # Fast Opto-Isolated Serial Interface Mode
        CBUS = 0x20     # Bitbang on CBUS pins of R-type chips
        SYNCFF = 0x40   # Single Channel Synchronous FIFO mode

    # MPSSE Commands
    WRITE_BYTES_PVE_MSB = 0x10
    WRITE_BYTES_NVE_MSB = 0x11
    WRITE_BITS_PVE_MSB = 0x12
    WRITE_BITS_NVE_MSB = 0x13
    WRITE_BYTES_PVE_LSB = 0x18
    WRITE_BYTES_NVE_LSB = 0x19
    WRITE_BITS_PVE_LSB = 0x1a
    WRITE_BITS_NVE_LSB = 0x1b
    READ_BYTES_PVE_MSB = 0x20
    READ_BYTES_NVE_MSB = 0x24
    READ_BITS_PVE_MSB = 0x22
    READ_BITS_NVE_MSB = 0x26
    READ_BYTES_PVE_LSB = 0x28
    READ_BYTES_NVE_LSB = 0x2c
    READ_BITS_PVE_LSB = 0x2a
    READ_BITS_NVE_LSB = 0x2e
    RW_BYTES_PVE_NVE_MSB = 0x31
    RW_BYTES_NVE_PVE_MSB = 0x34
    RW_BITS_PVE_NVE_MSB = 0x33
    RW_BITS_NVE_PVE_MSB = 0x36
    RW_BYTES_PVE_NVE_LSB = 0x39
    RW_BYTES_NVE_PVE_LSB = 0x3c
    RW_BITS_PVE_NVE_LSB = 0x3b
    RW_BITS_NVE_PVE_LSB = 0x3e
    WRITE_BITS_TMS_PVE = 0x4a
    WRITE_BITS_TMS_NVE = 0x4b
    RW_BITS_TMS_PVE_PVE = 0x6a
    RW_BITS_TMS_PVE_NVE = 0x6b
    RW_BITS_TMS_NVE_PVE = 0x6e
    RW_BITS_TMS_NVE_NVE = 0x6f
    SEND_IMMEDIATE = 0x87
    WAIT_ON_HIGH = 0x88
    WAIT_ON_LOW = 0x89
    READ_SHORT = 0x90
    READ_EXTENDED = 0x91
    WRITE_SHORT = 0x92
    WRITE_EXTENDED = 0x93
    # -H series only
    DISABLE_CLK_DIV5 = 0x8a
    ENABLE_CLK_DIV5 = 0x8b

    # Modem status
    MODEM_CTS = 1 << 4      # Clear to send
    MODEM_DSR = 1 << 5      # Data set ready
    MODEM_RI = 1 << 6       # Ring indicator
    MODEM_RLSD = 1 << 7     # Carrier detect
    MODEM_DR = 1 << 8       # Data ready
    MODEM_OE = 1 << 9       # Overrun error
    MODEM_PE = 1 << 10      # Parity error
    MODEM_FE = 1 << 11      # Framing error
    MODEM_BI = 1 << 12      # Break interrupt
    MODEM_THRE = 1 << 13    # Transmitter holding register
    MODEM_TEMT = 1 << 14    # Transmitter empty
    MODEM_RCVE = 1 << 15    # Error in RCVR FIFO

    # FTDI MPSSE commands
    SET_BITS_LOW = 0x80     # Change LSB GPIO output
    SET_BITS_HIGH = 0x82    # Change MSB GPIO output
    GET_BITS_LOW = 0x81     # Get LSB GPIO output
    GET_BITS_HIGH = 0x83    # Get MSB GPIO output
    LOOPBACK_START = 0x84   # Enable loopback
    LOOPBACK_END = 0x85     # Disable loopback
    SET_TCK_DIVISOR = 0x86  # Set clock
    # -H series only
    ENABLE_CLK_3PHASE = 0x8c       # Enable 3-phase data clocking (I2C)
    DISABLE_CLK_3PHASE = 0x8d      # Disable 3-phase data clocking
    CLK_BITS_NO_DATA = 0x8e        # Allows JTAG clock to be output w/o data
    CLK_BYTES_NO_DATA = 0x8f       # Allows JTAG clock to be output w/o data
    CLK_WAIT_ON_HIGH = 0x94        # Clock until GPIOL1 is high
    CLK_WAIT_ON_LOW = 0x95         # Clock until GPIOL1 is low
    ENABLE_CLK_ADAPTIVE = 0x96     # Enable JTAG adaptive clock for ARM
    DISABLE_CLK_ADAPTIVE = 0x97    # Disable JTAG adaptive clock
    CLK_COUNT_WAIT_ON_HIGH = 0x9c  # Clock byte cycles until GPIOL1 is high
    CLK_COUNT_WAIT_ON_LOW = 0x9d   # Clock byte cycles until GPIOL1 is low
    # FT232H only
    DRIVE_ZERO = 0x9e       # Drive-zero mode

    # USB control requests
    REQ_OUT = build_request_type(CTRL_OUT, CTRL_TYPE_VENDOR,
                                 CTRL_RECIPIENT_DEVICE)
    REQ_IN = build_request_type(CTRL_IN, CTRL_TYPE_VENDOR,
                                CTRL_RECIPIENT_DEVICE)

    # Requests
    SIO_REQ_RESET = 0x0              # Reset the port
    SIO_REQ_SET_MODEM_CTRL = 0x1     # Set the modem control register
    SIO_REQ_SET_FLOW_CTRL = 0x2      # Set flow control register
    SIO_REQ_SET_BAUDRATE = 0x3       # Set baud rate
    SIO_REQ_SET_DATA = 0x4           # Set the data characteristics of the port
    SIO_REQ_POLL_MODEM_STATUS = 0x5  # Get line status
    SIO_REQ_SET_EVENT_CHAR = 0x6     # Change event character
    SIO_REQ_SET_ERROR_CHAR = 0x7     # Change error character
    SIO_REQ_SET_LATENCY_TIMER = 0x9  # Change latency timer
    SIO_REQ_GET_LATENCY_TIMER = 0xa  # Get latency timer
    SIO_REQ_SET_BITMODE = 0xb        # Change bit mode
    SIO_REQ_READ_PINS = 0xc          # Read GPIO pin value (or "get bitmode")

    # Eeprom requests
    SIO_REQ_EEPROM = 0x90
    SIO_REQ_READ_EEPROM = SIO_REQ_EEPROM + 0   # Read EEPROM content
    SIO_REQ_WRITE_EEPROM = SIO_REQ_EEPROM + 1  # Write EEPROM content
    SIO_REQ_ERASE_EEPROM = SIO_REQ_EEPROM + 2  # Erase EEPROM content

    # Reset arguments
    SIO_RESET_SIO = 0        # Reset device
    SIO_RESET_PURGE_RX = 1   # Drain USB RX buffer (host-to-ftdi)
    SIO_RESET_PURGE_TX = 2   # Drain USB TX buffer (ftdi-to-host)

    # Flow control arguments
    SIO_DISABLE_FLOW_CTRL = 0x0
    SIO_RTS_CTS_HS = 0x1 << 8
    SIO_DTR_DSR_HS = 0x2 << 8
    SIO_XON_XOFF_HS = 0x4 << 8
    SIO_SET_DTR_MASK = 0x1
    SIO_SET_DTR_HIGH = SIO_SET_DTR_MASK | (SIO_SET_DTR_MASK << 8)
    SIO_SET_DTR_LOW = 0x0 | (SIO_SET_DTR_MASK << 8)
    SIO_SET_RTS_MASK = 0x2
    SIO_SET_RTS_HIGH = SIO_SET_RTS_MASK | (SIO_SET_RTS_MASK << 8)
    SIO_SET_RTS_LOW = 0x0 | (SIO_SET_RTS_MASK << 8)

    # Parity bits
    PARITY_NONE, PARITY_ODD, PARITY_EVEN, PARITY_MARK, PARITY_SPACE = range(5)
    # Number of stop bits
    STOP_BIT_1, STOP_BIT_15, STOP_BIT_2 = range(3)
    # Number of bits
    BITS_7, BITS_8 = [7+i for i in range(2)]
    # Break type
    BREAK_OFF, BREAK_ON = range(2)

    # cts:  Clear to send
    # dsr:  Data set ready
    # ri:   Ring indicator
    # dcd:  Data carrier detect
    # dr:   Data ready
    # oe:   Overrun error
    # pe:   Parity error
    # fe:   Framing error
    # bi:   Break interrupt
    # thre: Transmitter holding register empty
    # temt: Transmitter empty
    # err:  Error in RCVR FIFO
    MODEM_STATUS = [('', '', '', '', 'cts', 'dsr', 'ri', 'dcd'),
                    ('dr', 'overrun', 'parity', 'framing',
                     'break', 'thre', 'txe', 'rcve')]

    ERROR_BITS = (0x00, 0x8E)
    TX_EMPTY_BITS = 0x60

    # Clocks and baudrates
    BUS_CLOCK_BASE = 6.0E6  # 6 MHz
    BUS_CLOCK_HIGH = 30.0E6  # 30 MHz
    BAUDRATE_REF_BASE = int(3.0E6)  # 3 MHz
    BAUDRATE_REF_HIGH = int(12.0E6)  # 12 MHz
    BITBANG_BAUDRATE_RATIO_BASE = 16
    BITBANG_BAUDRATE_RATIO_HIGH = 5
    BAUDRATE_TOLERANCE = 3.0  # acceptable clock drift for UART, in %

    FRAC_DIV_CODE = (0, 3, 2, 4, 1, 5, 6, 7)

    # Latency
    LATENCY_MIN = 1
    LATENCY_MAX = 255
    LATENCY_EEPROM_FT232R = 77

    # EEPROM Properties
    EXT_EEPROM_SIZES = (128, 256)  # in bytes (93C66 seen as 93C56)

    INT_EEPROMS = {
        0x0600: 0x80,  # FT232R: 128 bytes, 1024 bits
        0x1000: 0x400  # FT230*X: 1KiB
    }

    def __init__(self):
        self.log = getLogger('pyftdi.ftdi')
        self._debug_log = False
        self._usb_dev = None
        self._usb_read_timeout = 5000
        self._usb_write_timeout = 5000
        self._baudrate = -1
        self._readbuffer = bytearray()
        self._readoffset = 0
        self._readbuffer_chunksize = 4 << 10  # 4KiB
        self._writebuffer_chunksize = 4 << 10  # 4KiB
        self._max_packet_size = 0
        self._interface = None
        self._index = None
        self._in_ep = None
        self._out_ep = None
        self._bitmode = Ftdi.BitMode.RESET
        self._latency = 0
        self._latency_count = 0
        self._latency_min = self.LATENCY_MIN
        self._latency_max = self.LATENCY_MAX
        self._latency_threshold = None  # disable dynamic latency
        self._lineprop = 0
        self._cbus_pins = (0, 0)
        self._cbus_out = 0
        self._tracer = None

    # --- Public API -------------------------------------------------------

    @classmethod
    def create_from_url(cls, url: str) -> 'Ftdi':
        """Create an Ftdi instance from an URL

           URL scheme: ftdi://[vendor[:product[:index|:serial]]]/interface

           :param url: FTDI device selector
           :return: a fresh, open Ftdi instance
        """
        device = Ftdi()
        device.open_from_url(url)
        return device

    @classmethod
    def list_devices(cls, url: Optional[str] = None) -> \
            List[Tuple[UsbDeviceDescriptor, int]]:
        """List of URLs of connected FTDI devices.

           :param url: a pattern URL to restrict the search
           :return: list of (UsbDeviceDescriptor, interface)
        """
        return UsbTools.list_devices(url or 'ftdi:///?',
                                     cls.VENDOR_IDS, cls.PRODUCT_IDS,
                                     cls.DEFAULT_VENDOR)

    @classmethod
    def show_devices(cls, url: Optional[str] = None,
                     out: Optional[TextIO] = None) -> None:
        """Print the URLs and descriptors of connected FTDI devices.

           :param url: a pattern URL to restrict the search
           :param out: output stream, default to stdout
        """
        devdescs = UsbTools.list_devices(url or 'ftdi:///?',
                                         cls.VENDOR_IDS, cls.PRODUCT_IDS,
                                         cls.DEFAULT_VENDOR)
        UsbTools.show_devices('ftdi', cls.VENDOR_IDS, cls.PRODUCT_IDS,
                              devdescs, out)

    @classmethod
    def get_identifiers(cls, url: str) -> Tuple[UsbDeviceDescriptor, int]:
        """Extract the identifiers of an FTDI device from URL, if any

           :param url: input URL to parse
        """
        return UsbTools.parse_url(url,
                                  cls.SCHEME, cls.VENDOR_IDS, cls.PRODUCT_IDS,
                                  cls.DEFAULT_VENDOR)

    @classmethod
    def get_device(cls, url: str) -> UsbDevice:
        """Get a USB device from its URL, without opening an instance.

           :param url: input URL to parse
           :return: the USB device that match the specified URL
        """
        devdesc, _ = cls.get_identifiers(url)
        return UsbTools.get_device(devdesc)

    @classmethod
    def add_custom_vendor(cls, vid: int, vidname: str = '') -> None:
        """Add a custom USB vendor identifier.

           It can be useful to use a pretty URL for opening FTDI device

           :param vid: Vendor ID (USB 16-bit identifier)
           :param vidname: Vendor name (arbitrary string)
           :raise ValueError: if the vendor id is already referenced
        """
        if vid in cls.VENDOR_IDS.values():
            raise ValueError(f'Vendor ID 0x{vid:04x} already registered')
        if not vidname:
            vidname = f'0x{vid:04x}'
        cls.VENDOR_IDS[vidname] = vid

    @classmethod
    def add_custom_product(cls, vid: int, pid: int, pidname: str = '') -> None:
        """Add a custom USB product identifier.

           It is required for opening FTDI device with non-standard VID/PID
           USB identifiers.

           :param vid: Vendor ID (USB 16-bit identifier)
           :param pid: Product ID (USB 16-bit identifier)
           :param pidname: Product name (arbitrary string)
           :raise ValueError: if the product id is already referenced
        """
        if vid not in cls.PRODUCT_IDS:
            cls.PRODUCT_IDS[vid] = OrderedDict()
        elif pid in cls.PRODUCT_IDS[vid].values():
            raise ValueError(f'Product ID 0x{vid:04x}:0x{pid:04x} already '
                             f'registered')
        if not pidname:
            pidname = f'0x{pid:04x}'
        cls.PRODUCT_IDS[vid][pidname] = pid

    @classmethod
    def decode_modem_status(cls, value: bytes, error_only: bool = False) -> \
            Tuple[str, ...]:
        """Decode the FTDI modem status bitfield into short strings.

           :param value: 2-byte mode status
           :param error_only: only decode error flags
           :return: a tuple of status identifiers
        """
        status = []
        for pos, (byte_, ebits) in enumerate(zip(value, cls.ERROR_BITS)):
            for bit, _ in enumerate(cls.MODEM_STATUS[pos]):
                if error_only:
                    byte_ &= ebits
                if byte_ & (1 << bit):
                    status.append(cls.MODEM_STATUS[pos][bit])
        return tuple(status)

    @staticmethod
    def find_all(vps: Sequence[Tuple[int, int]], nocache: bool = False) -> \
            List[Tuple[UsbDeviceDescriptor, int]]:
        """Find all devices that match the vendor/product pairs of the vps
           list.

           :param vps: a sequence of 2-tuple (vid, pid) pairs
           :type vps: tuple(int, int)
           :param bool nocache: bypass cache to re-enumerate USB devices on
                                the host
           :return: a list of 5-tuple (vid, pid, sernum, iface, description)
                    device descriptors
           :rtype: list(tuple(int,int,str,int,str))
        """
        return UsbTools.find_all(vps, nocache)

    @property
    def is_connected(self) -> bool:
        """Tells whether this instance is connected to an actual USB slave.

           :return: the slave connection status
        """
        return bool(self._usb_dev)

    def open_from_url(self, url: str) -> None:
        """Open a new interface to the specified FTDI device.

           :param str url: a FTDI URL selector
        """
        devdesc, interface = self.get_identifiers(url)
        device = UsbTools.get_device(devdesc)
        self.open_from_device(device, interface)

    def open(self, vendor: int, product: int, bus: Optional[int] = None,
             address: Optional[int] = None, index: int = 0,
             serial: Optional[str] = None,
             interface: int = 1) -> None:
        """Open a new interface to the specified FTDI device.

           If several FTDI devices of the same kind (vid, pid) are connected
           to the host, either index or serial argument should be used to
           discriminate the FTDI device.

           index argument is not a reliable solution as the host may enumerate
           the USB device in random order. serial argument is more reliable
           selector and should always be prefered.

           Some FTDI devices support several interfaces/ports (such as FT2232H,
           FT4232H and FT4232HA). The interface argument selects the FTDI port
           to use, starting from 1 (not 0).

           :param int vendor: USB vendor id
           :param int product: USB product id
           :param int bus: optional selector,  USB bus
           :param int address: optional selector, USB address on bus
           :param int index: optional selector, specified the n-th matching
                             FTDI enumerated USB device on the host
           :param str serial: optional selector, specified the FTDI device
                              by its serial number
           :param str interface: FTDI interface/port
        """
        devdesc = UsbDeviceDescriptor(vendor, product, bus, address, serial,
                                      index, None)
        device = UsbTools.get_device(devdesc)
        self.open_from_device(device, interface)

    def open_from_device(self, device: UsbDevice,
                         interface: int = 1) -> None:
        """Open a new interface from an existing USB device.

           :param device: FTDI USB device (PyUSB instance)
           :param interface: FTDI interface to use (integer starting from 1)
        """
        if not isinstance(device, UsbDevice):
            raise FtdiError(f"Device '{device}' is not a PyUSB device")
        self._usb_dev = device
        try:
            self._usb_dev.set_configuration()
        except USBError:
            pass
        # detect invalid interface as early as possible
        config = self._usb_dev.get_active_configuration()
        if interface > config.bNumInterfaces:
            raise FtdiError(f'No such FTDI port: {interface}')
        self._set_interface(config, interface)
        self._max_packet_size = self._get_max_packet_size()
        # Invalidate data in the readbuffer
        self._readoffset = 0
        self._readbuffer = bytearray()
        # Drain input buffer
        self.purge_buffers()
        # Shallow reset
        self._reset_device()
        # Reset feature mode
        self.set_bitmode(0, Ftdi.BitMode.RESET)
        # Init latency
        self._latency_threshold = None
        self.set_latency_timer(self.LATENCY_MIN)
        self._debug_log = self.log.getEffectiveLevel() == DEBUG

    def close(self, freeze: bool = False) -> None:
        """Close the FTDI interface/port.

           :param freeze: if set, FTDI port is not reset to its default
                          state on close. This means the port is left with
                          its current configuration and output signals.
                          This feature should not be used except for very
                          specific needs.
        """
        if self._usb_dev:
            dev = self._usb_dev
            if self._is_pyusb_handle_active():
                # Do not attempt to execute the following calls if the
                # device has been closed: the ResourceManager may attempt
                # to re-open the device that has been already closed, and
                # this may lead to a (native) crash in libusb.
                try:
                    if not freeze:
                        self.set_bitmode(0, Ftdi.BitMode.RESET)
                        self.set_latency_timer(self.LATENCY_MAX)
                    release_interface(dev, self._index - 1)
                except FtdiError as exc:
                    self.log.warning('FTDI device may be gone: %s', exc)
                try:
                    self._usb_dev.attach_kernel_driver(self._index - 1)
                except (NotImplementedError, USBError):
                    pass
            self._usb_dev = None
            UsbTools.release_device(dev)

    def reset(self, usb_reset: bool = False) -> None:
        """Reset FTDI device.

           :param usb_reset: wether to perform a full USB reset of the device.

           Beware that selecting usb_reset performs a full USB device reset,
           which means all other interfaces of the same device are also
           affected.
        """
        if not self.is_connected:
            raise FtdiError('Not connected')
        self._reset_device()
        if usb_reset:
            self._reset_usb_device()

    def open_mpsse_from_url(self, url: str, direction: int = 0x0,
                            initial: int = 0x0, frequency: float = 6.0E6,
                            latency: int = 16, debug: bool = False) -> float:
        """Open a new interface to the specified FTDI device in MPSSE mode.

           MPSSE enables I2C, SPI, JTAG or other synchronous serial interface
           modes (vs. UART mode).

           :param url: a FTDI URL selector
           :param direction: a bitfield specifying the FTDI GPIO direction,
                where high level defines an output, and low level defines an
                input
           :param initial: a bitfield specifying the initial output value
           :param float frequency: serial interface clock in Hz
           :param latency: low-level latency in milliseconds. The shorter
                the delay, the higher the host CPU load. Do not use shorter
                values than the default, as it triggers data loss in FTDI.
           :param debug: use a tracer to decode MPSSE protocol
           :return: actual bus frequency in Hz
        """
        devdesc, interface = self.get_identifiers(url)
        device = UsbTools.get_device(devdesc)
        return self.open_mpsse_from_device(device, interface,
                                           direction=direction,
                                           initial=initial,
                                           frequency=frequency,
                                           latency=latency,
                                           debug=debug)

    def open_mpsse(self, vendor: int, product: int, bus: Optional[int] = None,
                   address: Optional[int] = None, index: int = 0,
                   serial: Optional[str] = None, interface: int = 1,
                   direction: int = 0x0, initial: int = 0x0,
                   frequency: float = 6.0E6, latency: int = 16,
                   debug: bool = False) -> float:
        """Open a new interface to the specified FTDI device in MPSSE mode.

           MPSSE enables I2C, SPI, JTAG or other synchronous serial interface
           modes (vs. UART mode).

           If several FTDI devices of the same kind (vid, pid) are connected
           to the host, either index or serial argument should be used to
           discriminate the FTDI device.

           index argument is not a reliable solution as the host may enumerate
           the USB device in random order. serial argument is more reliable
           selector and should always be prefered.

           Some FTDI devices support several interfaces/ports (such as FT2232H,
           FT4232H and FT4232HA). The interface argument selects the FTDI port
           to use, starting from 1 (not 0). Note that not all FTDI ports are
           MPSSE capable.

           :param vendor: USB vendor id
           :param product: USB product id
           :param bus: optional selector, USB bus
           :param address: optional selector, USB address on bus
           :param index: optional selector, specified the n-th matching
                             FTDI enumerated USB device on the host
           :param serial: optional selector, specified the FTDI device
                              by its serial number
           :param interface: FTDI interface/port
           :param direction: a bitfield specifying the FTDI GPIO direction,
                where high level defines an output, and low level defines an
                input
           :param initial: a bitfield specifying the initial output value
           :param frequency: serial interface clock in Hz
           :param latency: low-level latency in milliseconds. The shorter
                the delay, the higher the host CPU load. Do not use shorter
                values than the default, as it triggers data loss in FTDI.
           :param bool debug: use a tracer to decode MPSSE protocol
           :return: actual bus frequency in Hz
        """
        devdesc = UsbDeviceDescriptor(vendor, product, bus, address, serial,
                                      index, None)
        device = UsbTools.get_device(devdesc)
        return self.open_mpsse_from_device(device, interface,
                                           direction=direction,
                                           initial=initial,
                                           frequency=frequency,
                                           latency=latency,
                                           debug=debug)

    def open_mpsse_from_device(self, device: UsbDevice,
                               interface: int = 1, direction: int = 0x0,
                               initial: int = 0x0, frequency: float = 6.0E6,
                               latency: int = 16, tracer: bool = False,
                               debug: bool = False) -> float:
        """Open a new interface to the specified FTDI device in MPSSE mode.

           MPSSE enables I2C, SPI, JTAG or other synchronous serial interface
           modes (vs. UART mode).

           If several FTDI devices of the same kind (vid, pid) are connected
           to the host, either index or serial argument should be used to
           discriminate the FTDI device.

           index argument is not a reliable solution as the host may enumerate
           the USB device in random order. serial argument is more reliable
           selector and should always be prefered.

           Some FTDI devices support several interfaces/ports (such as FT2232H,
           FT4232H and FT4232HA). The interface argument selects the FTDI port
           to use, starting from 1 (not 0). Note that not all FTDI ports are
           MPSSE capable.

           :param device: FTDI USB device
           :param interface: FTDI interface/port
           :param direction: a bitfield specifying the FTDI GPIO direction,
                where high level defines an output, and low level defines an
                input
           :param initial: a bitfield specifying the initial output value
           :param frequency: serial interface clock in Hz
           :param latency: low-level latency in milliseconds. The shorter
                the delay, the higher the host CPU load. Do not use shorter
                values than the default, as it triggers data loss in FTDI.
           :param bool tracer: use a tracer to decode MPSSE protocol
           :param bool debug: add more debug traces
           :return: actual bus frequency in Hz
        """
        # pylint: disable=unused-argument
        self.open_from_device(device, interface)
        if not self.is_mpsse_interface(interface):
            self.close()
            raise FtdiMpsseError('This interface does not support MPSSE')
        if to_bool(tracer):  # accept strings as boolean
            # pylint: disable=import-outside-toplevel
            from .tracer import FtdiMpsseTracer
            self._tracer = FtdiMpsseTracer(self.device_version)
            self.log.debug('Using MPSSE tracer')
        # Set latency timer
        self.set_latency_timer(latency)
        # Set chunk size
        self.write_data_set_chunksize()
        self.read_data_set_chunksize()
        # Reset feature mode
        self.set_bitmode(0, Ftdi.BitMode.RESET)
        # Drain buffers
        self.purge_buffers()
        # Disable event and error characters
        self.set_event_char(0, False)
        self.set_error_char(0, False)
        # Enable MPSSE mode
        self.set_bitmode(direction, Ftdi.BitMode.MPSSE)
        # Configure clock
        frequency = self._set_frequency(frequency)
        # Configure I/O
        cmd = bytearray((Ftdi.SET_BITS_LOW, initial & 0xFF, direction & 0xFF))
        if self.has_wide_port:
            initial >>= 8
            direction >>= 8
            cmd.extend((Ftdi.SET_BITS_HIGH, initial & 0xFF, direction & 0xFF))
        self.write_data(cmd)
        # Disable loopback
        self.write_data(bytearray((Ftdi.LOOPBACK_END,)))
        self.validate_mpsse()
        # Return the actual frequency
        return frequency

    def open_bitbang_from_url(self, url: str, direction: int = 0x0,
                              latency: int = 16, baudrate: int = 1000000,
                              sync: bool = False) -> float:
        """Open a new interface to the specified FTDI device in bitbang mode.

           Bitbang enables direct read or write to FTDI GPIOs.

           :param url: a FTDI URL selector
           :param direction: a bitfield specifying the FTDI GPIO direction,
                where high level defines an output, and low level defines an
                input
           :param latency: low-level latency to select the USB FTDI poll
                delay. The shorter the delay, the higher the host CPU load.
           :param baudrate: pace to sequence GPIO exchanges
           :param sync: whether to use synchronous or asynchronous bitbang
           :return: actual bitbang baudrate in bps
        """
        devdesc, interface = self.get_identifiers(url)
        device = UsbTools.get_device(devdesc)
        return self.open_bitbang_from_device(device, interface,
                                             direction=direction,
                                             latency=latency,
                                             baudrate=baudrate,
                                             sync=sync)

    def open_bitbang(self, vendor: int, product: int,
                     bus: Optional[int] = None, address: Optional[int] = None,
                     index: int = 0, serial: Optional[str] = None,
                     interface: int = 1, direction: int = 0x0,
                     latency: int = 16, baudrate: int = 1000000,
                     sync: bool = False) -> float:
        """Open a new interface to the specified FTDI device in bitbang mode.

           Bitbang enables direct read or write to FTDI GPIOs.

           :param vendor: USB vendor id
           :param product: USB product id
           :param index: optional selector, specified the n-th matching
                             FTDI enumerated USB device on the host
           :param serial: optional selector, specified the FTDI device
                              by its serial number
           :param interface: FTDI interface/port
           :param direction: a bitfield specifying the FTDI GPIO direction,
                where high level defines an output, and low level defines an
                input
           :param latency: low-level latency to select the USB FTDI poll
                delay. The shorter the delay, the higher the host CPU load.
           :param baudrate: pace to sequence GPIO exchanges
           :param sync: whether to use synchronous or asynchronous bitbang
           :return: actual bitbang baudrate in bps
        """
        devdesc = UsbDeviceDescriptor(vendor, product, bus, address, serial,
                                      index, None)
        device = UsbTools.get_device(devdesc)
        return self.open_bitbang_from_device(device, interface,
                                             direction=direction,
                                             latency=latency,
                                             baudrate=baudrate,
                                             sync=sync)

    def open_bitbang_from_device(self, device: UsbDevice,
                                 interface: int = 1, direction: int = 0x0,
                                 latency: int = 16, baudrate: int = 1000000,
                                 sync: bool = False) -> int:
        """Open a new interface to the specified FTDI device in bitbang mode.

           Bitbang enables direct read or write to FTDI GPIOs.

           :param device: FTDI USB device
           :param interface: FTDI interface/port
           :param direction: a bitfield specifying the FTDI GPIO direction,
                where high level defines an output, and low level defines an
                input
           :param latency: low-level latency to select the USB FTDI poll
                delay. The shorter the delay, the higher the host CPU load.
           :param baudrate: pace to sequence GPIO exchanges
           :param sync: whether to use synchronous or asynchronous bitbang
           :return: actual bitbang baudrate in bps
        """
        self.open_from_device(device, interface)
        # Set latency timer
        self.set_latency_timer(latency)
        # Set chunk size
        # Beware that RX buffer, over 512 bytes, contains 2-byte modem marker
        # on every 512 byte chunk, so data and out-of-band marker get
        # interleaved. This is not yet supported with read_data_bytes for now
        self.write_data_set_chunksize()
        self.read_data_set_chunksize()
        # disable flow control
        self.set_flowctrl('')
        # Enable BITBANG mode
        self.set_bitmode(direction, Ftdi.BitMode.BITBANG if not sync else
                         Ftdi.BitMode.SYNCBB)
        # Configure clock
        if baudrate:
            self._baudrate = self._set_baudrate(baudrate, False)
        # Drain input buffer
        self.purge_buffers()
        return self._baudrate

    @property
    def usb_path(self) -> Tuple[int, int, int]:
        """Provide the physical location on the USB topology.

           :return: a tuple of bus, address, interface; if connected
        """
        if not self.is_connected:
            raise FtdiError('Not connected')
        return (self._usb_dev.bus, self._usb_dev.address,
                self._interface.bInterfaceNumber)

    @property
    def device_version(self) -> int:
        """Report the device version, i.e. the kind of device.

           :see: :py:meth:`ic_name` for a product version of this information.

           :return: the device version (16-bit integer)
        """
        if not self.is_connected:
            raise FtdiError('Device characteristics not yet known')
        return self._usb_dev.bcdDevice

    @property
    def ic_name(self) -> str:
        """Return the current type of the FTDI device as a string

           see also http://www.ftdichip.com/Support/
           Documents/TechnicalNotes/TN_100_USB_VID-PID_Guidelines.pdf

           :return: the identified FTDI device as a string
        """
        if not self.is_connected:
            return 'unknown'
        return self.DEVICE_NAMES.get(self.device_version, 'undefined')

    @property
    def device_port_count(self) -> int:
        """Report the count of port/interface of the Ftdi device.

           :return: the count of ports
        """
        if not self.is_connected:
            raise FtdiError('Device characteristics not yet known')
        return self._usb_dev.get_active_configuration().bNumInterfaces

    @property
    def port_index(self) -> int:
        """Report the port/interface index, starting from 1

           :return: the port position/index
        """
        if not self.is_connected:
            raise FtdiError('Device characteristics not yet known')
        return self._index

    @property
    def port_width(self) -> int:
        """Report the width of a single port / interface

           :return: the width of the port, in bits
           :raise FtdiError: if no FTDI port is open
        """
        if not self.is_connected:
            raise FtdiError('Device characteristics not yet known')
        if self.device_version in (0x0700, 0x0900):
            return 16
        if self.device_version in (0x0500, ):
            return 12
        return 8

    @property
    def has_mpsse(self) -> bool:
        """Tell whether the device supports MPSSE (I2C, SPI, JTAG, ...)

           :return: True if the FTDI device supports MPSSE
           :raise FtdiError: if no FTDI port is open
        """
        if not self.is_connected:
            raise FtdiError('Device characteristics not yet known')
        return self.device_version in (0x0500, 0x0700, 0x0800, 0x0900, 0x3600)

    @property
    def has_wide_port(self) -> bool:
        """Tell whether the device supports 16-bit GPIO ports (vs. 8 bits)

           :return: True if the FTDI device supports wide GPIO port
           :raise FtdiError: if no FTDI port is open
        """
        return self.port_width > 8

    @property
    def has_cbus(self) -> bool:
        """Tell whether the device supports CBUS bitbang.

           CBUS bitbanging feature requires a special configuration in EEPROM.
           This function only reports if the current device supports this mode,
           not if this mode has been enabled in EEPROM.

           EEPROM configuration must be queried to check which CBUS pins have
           been configured for GPIO/bitbang mode.

           :return: True if the FTDI device supports CBUS bitbang
           :raise FtdiError: if no FTDI port is open
        """
        if not self.is_connected:
            raise FtdiError('Device characteristics not yet known')
        return self.device_version in (0x0600, 0x0900, 0x1000)

    @property
    def has_drivezero(self) -> bool:
        """Tell whether the device supports drive-zero mode, i.e. if the
           device supports the open-collector drive mode, useful for I2C
           communication for example.

           :return: True if the FTDI device features drive-zero mode
           :raise FtdiError: if no FTDI port is open
        """
        if not self.is_connected:
            raise FtdiError('Device characteristics not yet known')
        return self.device_version in (0x0900, )

    @property
    def is_legacy(self) -> bool:
        """Tell whether the device is a low-end FTDI

           :return: True if the FTDI device can only be used as a slow USB-UART
                    bridge
           :raise FtdiError: if no FTDI port is open
        """
        if not self.is_connected:
            raise FtdiError('Device characteristics not yet known')
        return self.device_version <= 0x0200

    @property
    def is_H_series(self) -> bool:
        """Tell whether the device is a high-end FTDI

           :return: True if the FTDI device is a high-end USB-UART bridge
           :raise FtdiError: if no FTDI port is open
        """
        if not self.is_connected:
            raise FtdiError('Device characteristics not yet known')
        return self.device_version in (0x0700, 0x0800, 0x0900, 0x3600)

    @property
    def is_mpsse(self) -> bool:
        """Tell whether the device is configured in MPSSE mode

           :return: True if the FTDI interface is configured in MPSSE mode
        """
        return self._bitmode == Ftdi.BitMode.MPSSE

    def is_mpsse_interface(self, interface: int) -> bool:
        """Tell whether the interface supports MPSSE (I2C, SPI, JTAG, ...)

           :return: True if the FTDI interface supports MPSSE
           :raise FtdiError: if no FTDI port is open
        """
        if not self.has_mpsse:
            return False
        if self.device_version == 0x0800 and interface > 2:
            return False
        if self.device_version == 0x3600 and interface > 2:
            return False
        return True

    @property
    def is_bitbang_enabled(self) -> bool:
        """Tell whether some bitbang mode is activated

           :return: True if the FTDI interface is configured to support
                    bitbanging
        """
        return self._bitmode not in (
            Ftdi.BitMode.RESET,
            Ftdi.BitMode.MPSSE,
            Ftdi.BitMode.CBUS  # CBUS mode does not change base frequency
        )

    # legacy API
    bitbang_enabled = is_bitbang_enabled

    @property
    def is_eeprom_internal(self) -> bool:
        """Tell whether the device has an internal EEPROM.

           :return: True if the device has an internal EEPROM.
        """
        return self.device_version in self.INT_EEPROMS

    @property
    def max_eeprom_size(self) -> int:
        """Report the maximum size of the EEPROM.
           The actual size may be lower, of even 0 if no EEPROM is connected
           or supported.

           :return: the maximum size in bytes.
        """
        if self.device_version in self.INT_EEPROMS:
            return self.INT_EEPROMS[self.device_version]
        if self.device_version == 0x0600:
            return 0x80
        return 0x100

    @property
    def frequency_max(self) -> float:
        """Tells the maximum frequency for MPSSE clock.

           :return: the maximum supported frequency in Hz
        """
        return Ftdi.BUS_CLOCK_HIGH if self.is_H_series else Ftdi.BUS_CLOCK_BASE

    @property
    def fifo_sizes(self) -> Tuple[int, int]:
        """Return the (TX, RX) tupple of hardware FIFO sizes

           :return: 2-tuple of TX, RX FIFO size in bytes
        """
        try:
            return Ftdi.FIFO_SIZES[self.device_version]
        except KeyError as exc:
            raise FtdiFeatureError(f'Unsupported device: '
                                   f'0x{self.device_version:04x}') from exc

    @property
    def mpsse_bit_delay(self) -> float:
        """Delay between execution of two MPSSE SET_BITS commands.

           :return: minimum delay (actual value might be larger) in seconds
        """
        # measured on FTDI2232H, not documented in datasheet, hence may vary
        # from on FTDI model to another...
        # left as a variable so it could be tweaked base on the FTDI bcd type,
        # the frequency, or ... whatever else
        return 0.5E-6  # seems to vary between 5 and 6.5 us

    @property
    def baudrate(self) -> int:
        """Return current baudrate.
        """
        return self._baudrate

    @property
    def usb_dev(self) -> UsbDevice:
        """Return the underlying USB Device.
        """
        return self._usb_dev

    def set_baudrate(self, baudrate: int, constrain: bool = True) -> int:
        """Change the current UART or BitBang baudrate.

           The FTDI device is not able to use an arbitrary baudrate. Its
           internal dividors are only able to achieve some baudrates.

           PyFtdi attemps to find the closest configurable baudrate and if
           the deviation from the requested baudrate is too high, it rejects
           the configuration if constrain is set.

           :py:attr:`baudrate` attribute can be used to retrieve the exact
           selected baudrate.

           :py:const:`BAUDRATE_TOLERANCE` defines the maximum deviation between
           the requested baudrate and the closest FTDI achieveable baudrate,
           which matches standard UART clock drift (3%). If the achievable
           baudrate is not within limits, baudrate setting is rejected.

           :param baudrate: the new baudrate for the UART.
           :param constrain: whether to validate baudrate is in RS232 tolerance
                             limits or allow larger drift
           :raise ValueError: if deviation from selected baudrate is too large
           :raise FtdiError: on IO Error
           :return: the effective baudrate
        """
        self._baudrate = self._set_baudrate(baudrate, constrain)
        return self._baudrate

    def set_frequency(self, frequency: float) -> float:
        """Change the current MPSSE bus frequency

           The FTDI device is not able to use an arbitrary frequency. Its
           internal dividors are only able to achieve some frequencies.

           PyFtdi finds and selects the closest configurable frequency.

           :param frequency: the new frequency for the serial interface,
                in Hz.
           :return: the selected frequency, which may differ from the requested
                one, in Hz
        """
        return self._set_frequency(frequency)

    def purge_rx_buffer(self) -> None:
        """Clear the USB receive buffer on the chip (host-to-ftdi) and the
           internal read buffer."""
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_RESET,
                                   Ftdi.SIO_RESET_PURGE_RX):
            raise FtdiError('Unable to flush RX buffer')
        # Invalidate data in the readbuffer
        self._readoffset = 0
        self._readbuffer = bytearray()
        self.log.debug('rx buf purged')

    def purge_tx_buffer(self) -> None:
        """Clear the USB transmit buffer on the chip (ftdi-to-host)."""
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_RESET,
                                   Ftdi.SIO_RESET_PURGE_TX):
            raise FtdiError('Unable to flush TX buffer')

    def purge_buffers(self) -> None:
        """Clear the buffers on the chip and the internal read buffer."""
        self.purge_rx_buffer()
        self.purge_tx_buffer()

    def write_data_set_chunksize(self, chunksize: int = 0) -> None:
        """Configure write buffer chunk size.

           This is a low-level configuration option, which is not intended to
           be use for a regular usage.

           :param chunksize: the optional size of the write buffer in bytes,
                             it is recommended to use 0 to force automatic
                             evaluation of the best value.
        """
        if chunksize == 0:
            chunksize = self.fifo_sizes[0]
        self._writebuffer_chunksize = chunksize
        self.log.debug('TX chunksize: %d', self._writebuffer_chunksize)

    def write_data_get_chunksize(self) -> int:
        """Get write buffer chunk size.

           :return: the size of the write buffer in bytes
        """
        return self._writebuffer_chunksize

    def read_data_set_chunksize(self, chunksize: int = 0) -> None:
        """Configure read buffer chunk size.

           This is a low-level configuration option, which is not intended to
           be use for a regular usage.

           :param chunksize: the optional size of the read buffer in bytes,
                             it is recommended to use 0 to force automatic
                             evaluation of the best value.
        """
        # Invalidate all remaining data
        self._readoffset = 0
        self._readbuffer = bytearray()
        if chunksize == 0:
            # status byte prolog is emitted every maxpacketsize, but for "some"
            # reasons, FT232R emits it every RX FIFO size bytes... Other
            # devices use a maxpacketsize which is smaller or equal to their
            # FIFO size, so this weird behavior is for now only experienced
            # with FT232R. Any, the following compution should address all
            # devices.
            chunksize = min(self.fifo_sizes[0], self.fifo_sizes[1],
                            self._max_packet_size)
        if platform == 'linux':
            chunksize = min(chunksize, 16384)
        self._readbuffer_chunksize = chunksize
        self.log.debug('RX chunksize: %d', self._readbuffer_chunksize)

    def read_data_get_chunksize(self) -> int:
        """Get read buffer chunk size.

           :return: the size of the write buffer in bytes
        """
        return self._readbuffer_chunksize

    def set_bitmode(self, bitmask: int, mode: 'Ftdi.BitMode') -> None:
        """Enable/disable bitbang modes.

           Switch the FTDI interface to bitbang mode.
        """
        self.log.debug('bitmode: %s', mode.name)
        mask = sum(Ftdi.BitMode)
        value = (bitmask & 0xff) | ((mode.value & mask) << 8)
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_SET_BITMODE, value):
            raise FtdiError('Unable to set bitmode')
        self._bitmode = mode

    def read_pins(self) -> int:
        """Directly read pin state, circumventing the read buffer.
           Useful for bitbang mode.

           :return: bitfield of FTDI interface input GPIO
        """
        pins = self._ctrl_transfer_in(Ftdi.SIO_REQ_READ_PINS, 1)
        if not pins:
            raise FtdiError('Unable to read pins')
        return pins[0]

    def set_cbus_direction(self, mask: int, direction: int) -> None:
        """Configure the CBUS pins used as GPIOs

           :param mask: which pins to configure as GPIOs
           :param direction: which pins are output (vs. input)
        """
        # sanity check: there cannot be more than 4 CBUS pins in bitbang mode
        if not 0 <= mask <= 0x0F:
            raise ValueError(f'Invalid CBUS gpio mask: 0x{mask:02x}')
        if not 0 <= direction <= 0x0F:
            raise ValueError(f'Invalid CBUS gpio direction: 0x{direction:02x}')
        self._cbus_pins = (mask, direction)

    def get_cbus_gpio(self) -> int:
        """Get the CBUS pins configured as GPIO inputs

           :return: bitfield of CBUS read pins
        """
        if self._bitmode not in (Ftdi.BitMode.RESET, Ftdi.BitMode.CBUS):
            raise FtdiError('CBUS gpio not available from current mode')
        if not self._cbus_pins[0] & ~self._cbus_pins[1]:
            raise FtdiError('No CBUS IO configured as input')
        outv = (self._cbus_pins[1] << 4) | self._cbus_out
        oldmode = self._bitmode
        try:
            self.set_bitmode(outv, Ftdi.BitMode.CBUS)
            inv = self.read_pins()
        finally:
            if oldmode != self._bitmode:
                self.set_bitmode(0, oldmode)
        return inv & ~self._cbus_pins[1] & self._cbus_pins[0]

    def set_cbus_gpio(self, pins: int) -> None:
        """Set the CBUS pins configured as GPIO outputs

           :param pins: bitfield to apply to CBUS output pins
        """
        if self._bitmode not in (Ftdi.BitMode.RESET, Ftdi.BitMode.CBUS):
            raise FtdiError('CBUS gpio not available from current mode')
        # sanity check: there cannot be more than 4 CBUS pins in bitbang mode
        if not 0 <= pins <= 0x0F:
            raise ValueError(f'Invalid CBUS gpio pins: 0x{pins:02x}')
        if not self._cbus_pins[0] & self._cbus_pins[1]:
            raise FtdiError('No CBUS IO configured as output')
        pins &= self._cbus_pins[0] & self._cbus_pins[1]
        value = (self._cbus_pins[1] << 4) | pins
        oldmode = self._bitmode
        try:
            self.set_bitmode(value, Ftdi.BitMode.CBUS)
            self._cbus_out = pins
        finally:
            if oldmode != self._bitmode:
                self.set_bitmode(0, oldmode)

    def set_latency_timer(self, latency: int):
        """Set latency timer.

           The FTDI chip keeps data in the internal buffer for a specific
           amount of time if the buffer is not full yet to decrease
           load on the usb bus.

           The shorted the latency, the shorted the delay to obtain data and
           the higher the host CPU load. Be careful with this option.

           :param latency: latency (unspecified unit)
        """
        if not Ftdi.LATENCY_MIN <= latency <= Ftdi.LATENCY_MAX:
            raise ValueError("Latency out of range")
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_SET_LATENCY_TIMER, latency):
            raise FtdiError('Unable to latency timer')

    def get_latency_timer(self) -> int:
        """Get latency timer.

           :return: the current latency (unspecified unit)
        """
        latency = self._ctrl_transfer_in(Ftdi.SIO_REQ_GET_LATENCY_TIMER, 1)
        if not latency:
            raise FtdiError('Unable to get latency')
        return latency[0]

    def poll_modem_status(self) -> int:
        """Poll modem status information.

           This function allows the retrieve the two status bytes of the
           device, useful in UART mode.

           FTDI device does not have a so-called USB "interrupt" end-point,
           event polling on the UART interface is done through the regular
           control endpoint.

           see :py:func:`modem_status` to obtain decoded status strings

           :return: modem status, as a proprietary bitfield
        """
        value = self._ctrl_transfer_in(Ftdi.SIO_REQ_POLL_MODEM_STATUS, 2)
        if not value or len(value) != 2:
            raise FtdiError('Unable to get modem status')
        status, = sunpack('<H', value)
        return status

    def modem_status(self) -> Tuple[str, ...]:
        """Provide the current modem status as a tuple of set signals

           :return: decodede modem status as short strings
        """
        value = self._ctrl_transfer_in(Ftdi.SIO_REQ_POLL_MODEM_STATUS, 2)
        if not value or len(value) != 2:
            raise FtdiError('Unable to get modem status')
        return self.decode_modem_status(value)

    def set_flowctrl(self, flowctrl: str) -> None:
        """Select flowcontrol in UART mode.

           Either hardware flow control through RTS/CTS UART lines,
           software or no flow control.

           :param str flowctrl: either 'hw' for HW flow control or '' (empty
                                string) for no flow control.
           :raise ValueError: if the flow control argument is invalid

           .. note:: How does RTS/CTS flow control work (from FTDI FAQ):

                FTxxx RTS# pin is an output. It should be connected to the CTS#
                input pin of the device at the other end of the UART link.

                    * If RTS# is logic 0 it is indicating the FTxxx device can
                      accept more data on the RXD pin.
                    * If RTS# is logic 1 it is indicating the FTxxx device
                      cannot accept more data.

                RTS# changes state when the chip buffer reaches its last 32
                bytes of space to allow time for the external device to stop
                sending data to the FTxxx device.

                FTxxx CTS# pin is an input. It should be connected to the RTS#
                output pin of the device at the other end of the UART link.

                  * If CTS# is logic 0 it is indicating the external device can
                    accept more data, and the FTxxx will transmit on the TXD
                    pin.
                  * If CTS# is logic 1 it is indicating the external device
                    cannot accept more data. the FTxxx will stop transmitting
                    within 0~3 characters, depending on what is in the buffer.

                    **This potential 3 character overrun does occasionally
                    present problems.** Customers shoud be made aware the FTxxx
                    is a USB device and not a "normal" RS232 device as seen on
                    a PC. As such the device operates on a packet basis as
                    opposed to a byte basis.

                Word to the wise. Not only do RS232 level shifting devices
                level shift, but they also invert the signal.
        """
        ctrl = {'hw': Ftdi.SIO_RTS_CTS_HS,
                '': Ftdi.SIO_DISABLE_FLOW_CTRL}
        try:
            value = ctrl[flowctrl] | self._index
        except KeyError as exc:
            raise ValueError(f'Unknown flow control: {flowctrl}') from exc
        try:
            if self._usb_dev.ctrl_transfer(
                    Ftdi.REQ_OUT, Ftdi.SIO_REQ_SET_FLOW_CTRL, 0, value,
                    bytearray(), self._usb_write_timeout):
                raise FtdiError('Unable to set flow control')
        except USBError as exc:
            raise FtdiError(f'UsbError: {exc}') from exc

    def set_dtr(self, state: bool) -> None:
        """Set dtr line

           :param state: new DTR logical level
        """
        value = Ftdi.SIO_SET_DTR_HIGH if state else Ftdi.SIO_SET_DTR_LOW
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_SET_MODEM_CTRL, value):
            raise FtdiError('Unable to set DTR line')

    def set_rts(self, state: bool) -> None:
        """Set rts line

           :param state: new RTS logical level
        """
        value = Ftdi.SIO_SET_RTS_HIGH if state else Ftdi.SIO_SET_RTS_LOW
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_SET_MODEM_CTRL, value):
            raise FtdiError('Unable to set RTS line')

    def set_dtr_rts(self, dtr: bool, rts: bool) -> None:
        """Set dtr and rts lines at once

           :param dtr: new DTR logical level
           :param rts: new RTS logical level
        """
        value = 0
        value |= Ftdi.SIO_SET_DTR_HIGH if dtr else Ftdi.SIO_SET_DTR_LOW
        value |= Ftdi.SIO_SET_RTS_HIGH if rts else Ftdi.SIO_SET_RTS_LOW
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_SET_MODEM_CTRL, value):
            raise FtdiError('Unable to set DTR/RTS lines')

    def set_break(self, break_: bool) -> None:
        """Start or stop a break exception event on the serial line

           :param break_: either start or stop break event
        """
        if break_:
            value = self._lineprop | (0x01 << 14)
            if self._ctrl_transfer_out(Ftdi.SIO_REQ_SET_DATA, value):
                raise FtdiError('Unable to start break sequence')
        else:
            value = self._lineprop & ~(0x01 << 14)
            if self._ctrl_transfer_out(Ftdi.SIO_REQ_SET_DATA, value):
                raise FtdiError('Unable to stop break sequence')
        self._lineprop = value

    def set_event_char(self, eventch: int, enable: bool) -> None:
        """Set the special event character"""
        value = eventch
        if enable:
            value |= 1 << 8
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_SET_EVENT_CHAR, value):
            raise FtdiError('Unable to set event char')

    def set_error_char(self, errorch: int, enable: bool) -> None:
        """Set error character"""
        value = errorch
        if enable:
            value |= 1 << 8
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_SET_ERROR_CHAR, value):
            raise FtdiError('Unable to set error char')

    def set_line_property(self, bits: int, stopbit: Union[int, float],
                          parity: str, break_: bool = False) -> None:
        """Configure the (RS232) UART characteristics.

           Arguments match the valid subset for FTDI HW of pyserial
           definitions.

           Bits accepts one of the following values:

           * ``7`` for 7-bit characters
           * ``8`` for 8-bit characters

           Stopbit accepts one of the following values:

           * ``1`` for a single bit
           * ``1.5`` for a bit and a half
           * ``2`` for two bits

           Parity accepts one of the following strings:

           * ``N`` for no parity bit
           * ``O`` for odd parity bit
           * ``E`` for even parity bit
           * ``M`` for parity bit always set
           * ``S`` for parity bit always reset

           :param bits: data bit count
           :param stopbit: stop bit count
           :param parity: parity mode as a single uppercase character
           :param break_: force break event
        """
        bytelength = {7: Ftdi.BITS_7,
                      8: Ftdi.BITS_8}
        parities = {'N': Ftdi.PARITY_NONE,
                    'O': Ftdi.PARITY_ODD,
                    'E': Ftdi.PARITY_EVEN,
                    'M': Ftdi.PARITY_MARK,
                    'S': Ftdi.PARITY_SPACE}
        stopbits = {1: Ftdi.STOP_BIT_1,
                    1.5: Ftdi.STOP_BIT_15,
                    2: Ftdi.STOP_BIT_2}
        if parity not in parities:
            raise FtdiFeatureError("Unsupported parity")
        if bits not in bytelength:
            raise FtdiFeatureError("Unsupported byte length")
        if stopbit not in stopbits:
            raise FtdiFeatureError("Unsupported stop bits")
        value = bits & 0x0F
        try:
            value |= {Ftdi.PARITY_NONE: 0x00 << 8,
                      Ftdi.PARITY_ODD: 0x01 << 8,
                      Ftdi.PARITY_EVEN: 0x02 << 8,
                      Ftdi.PARITY_MARK: 0x03 << 8,
                      Ftdi.PARITY_SPACE: 0x04 << 8}[parities[parity]]
            value |= {Ftdi.STOP_BIT_1: 0x00 << 11,
                      Ftdi.STOP_BIT_15: 0x01 << 11,
                      Ftdi.STOP_BIT_2: 0x02 << 11}[stopbits[stopbit]]
            if break_ == Ftdi.BREAK_ON:
                value |= 0x01 << 14
        except KeyError as exc:
            raise ValueError('Invalid line property') from exc
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_SET_DATA, value):
            raise FtdiError('Unable to set line property')
        self._lineprop = value

    def enable_adaptive_clock(self, enable: bool = True) -> None:
        """Enable adaptative clock mode, useful in MPSEE mode.

           Adaptive clock is a unique feature designed for a feedback clock
           for JTAG with ARM core.

           :param enable: whether to enable or disable this mode.
           :raise FtdiMpsseError: if MPSSE mode is not enabled
        """
        if not self.is_mpsse:
            raise FtdiMpsseError('Setting adaptive clock mode is only '
                                 'available from MPSSE mode')
        self.write_data(bytearray([enable and Ftdi.ENABLE_CLK_ADAPTIVE or
                                   Ftdi.DISABLE_CLK_ADAPTIVE]))

    def enable_3phase_clock(self, enable: bool = True) -> None:
        """Enable 3-phase clocking mode, useful in MPSSE mode.

           3-phase clock is mostly useful with I2C mode. It is also be used
           as a workaround to support SPI mode 3.

           :param enable: whether to enable or disable this mode.
           :raise FtdiMpsseError: if MPSSE mode is not enabled or device is
                not capable of 3-phase clocking
        """
        if not self.is_mpsse:
            raise FtdiMpsseError('Setting 3-phase clock mode is only '
                                 'available from MPSSE mode')
        if not self.is_H_series:
            raise FtdiFeatureError('This device does not support 3-phase '
                                   'clock')
        self.write_data(bytearray([enable and Ftdi.ENABLE_CLK_3PHASE or
                                   Ftdi.DISABLE_CLK_3PHASE]))

    def enable_drivezero_mode(self, lines: int) -> None:
        """Enable drive-zero mode, useful in MPSSE mode.

           drive-zero mode is mostly useful with I2C mode, to support the open
           collector driving mode.

           :param lines: bitfield of GPIO to drive in collector driven mode
           :raise FtdiMpsseError: if MPSSE mode is not enabled or device is
                not capable of drive-zero mode
        """
        if not self.is_mpsse:
            raise FtdiMpsseError('Setting drive-zero mode is only '
                                 'available from MPSSE mode')
        if not self.has_drivezero:
            raise FtdiFeatureError('This device does not support drive-zero '
                                   'mode')
        self.write_data(bytearray([Ftdi.DRIVE_ZERO, lines & 0xff,
                                   (lines >> 8) & 0xff]))

    def enable_loopback_mode(self, loopback: bool = False) -> None:
        """Enable loopback, i.e. connect DO to DI in FTDI MPSSE port for test
           purposes only. It does not support UART (TX to RX) mode.

           :param loopback: whether to enable or disable this mode
        """
        self.write_data(bytearray((Ftdi.LOOPBACK_START if loopback else
                                   Ftdi.LOOPBACK_END,)))

    def calc_eeprom_checksum(self, data: Union[bytes, bytearray]) -> int:
        """Calculate EEPROM checksum over the data

           :param data: data to compute checksum over. Must be an even number
                        of bytes
           :return: checksum
        """
        length = len(data)
        if not length:
            raise ValueError('No data to checksum')
        if length & 0x1:
            raise ValueError('Length not even')
        # NOTE: checksum is computed using 16-bit values in little endian
        # ordering
        checksum = 0XAAAA
        mtp = self.device_version == 0x1000  # FT230X
        for idx in range(0, length, 2):
            if mtp and 0x24 <= idx < 0x80:
                # special MTP user section which is not considered for the CRC
                continue
            val = ((data[idx+1] << 8) + data[idx]) & 0xffff
            checksum = val ^ checksum
            checksum = ((checksum << 1) & 0xffff) | ((checksum >> 15) & 0xffff)
        return checksum

    def read_eeprom(self, addr: int = 0, length: Optional[int] = None,
                    eeprom_size: Optional[int] = None) -> bytes:
        """Read the EEPROM starting at byte address, addr, and returning
           length bytes. Here, addr and length are in bytes but we
           access a 16-bit word at a time, so automatically update
           addr and length to work with word accesses.

           :param addr: byte address that desire to read.
           :param length: byte length to read or None
           :param eeprom_size: total size in bytes of the eeprom or None
           :return: eeprom bytes, as an array of bytes
        """
        eeprom_size = self._check_eeprom_size(eeprom_size)
        if length is None:
            length = eeprom_size
        if addr < 0 or (addr+length) > eeprom_size:
            raise ValueError('Invalid address/length')
        word_addr = addr >> 1
        word_count = length >> 1
        if (addr & 0x1) | (length & 0x1):
            word_count += 1
        try:
            data = bytearray()
            while word_count:
                buf = self._usb_dev.ctrl_transfer(
                    Ftdi.REQ_IN, Ftdi.SIO_REQ_READ_EEPROM, 0,
                    word_addr, 2, self._usb_read_timeout)
                if not buf:
                    err_addr = word_addr << 1
                    raise FtdiEepromError(f'EEPROM read error @ {err_addr}')
                data.extend(buf)
                word_count -= 1
                word_addr += 1
            start = addr & 0x1
            return bytes(data[start:start+length])
        except USBError as exc:
            raise FtdiError(f'UsbError: {exc}') from exc

    def write_eeprom(self, addr: int, data: Union[bytes, bytearray],
                     eeprom_size: Optional[int] = None,
                     dry_run: bool = True) -> None:
        """Write multiple bytes to the EEPROM starting at byte address,
           addr. This function also updates the checksum
           automatically.

           .. warning:: You can brick your device with invalid size or content.
                        Use this function at your own risk, and RTFM.

           :param addr: starting byte address to start writing
           :param data: data to be written
           :param eeprom_size: total size in bytes of the eeprom or None
           :param dry_run: log what should be written, do not actually
                           change the EEPROM content
        """
        eeprom_size = self._check_eeprom_size(eeprom_size)
        if not data:
            return
        length = len(data)
        if addr < 0 or (addr+length) > eeprom_size:
            # accept up to eeprom_size, even if the last two bytes are
            # overwritten with a locally computed checksum
            raise ValueError('Invalid address/length')
        # First, read out the entire EEPROM, based on eeprom_size.
        eeprom = bytearray(self.read_eeprom(0, eeprom_size))
        # patch in the new data
        eeprom[addr:addr+len(data)] = data
        # compute new checksum
        chksum = self.calc_eeprom_checksum(eeprom[:-2])
        self.log.info('New EEPROM checksum: 0x%04x', chksum)
        # insert updated checksum - it is last 16-bits in EEPROM
        if self.device_version == 0x1000:
            # FT230x EEPROM structure is different
            eeprom[0x7e] = chksum & 0x0ff
            eeprom[0x7f] = chksum >> 8
        else:
            eeprom[-2] = chksum & 0x0ff
            eeprom[-1] = chksum >> 8
        # Write back the new data and checksum back to
        # EEPROM. Only write data that is changing instead of writing
        # everything in EEPROM, even if the data does not change.
        #
        # Compute start and end sections of eeprom baring in mind that
        # they must be even since it is a 16-bit EEPROM.
        # If start addr is odd, back it up one.
        start = addr
        size = length
        if start & 0x1:
            start -= 1
            size += 1
        if size & 0x1:
            size += 1
        size = min(size, eeprom_size - 2)
        # finally, write new section of data and ...
        self._write_eeprom_raw(start, eeprom[start:start+size],
                               dry_run=dry_run)
        # ... updated checksum
        self._write_eeprom_raw((eeprom_size-2), eeprom[-2:], dry_run=dry_run)

    def overwrite_eeprom(self, data: Union[bytes, bytearray],
                         dry_run: bool = True) -> None:
        """Write the whole EEPROM content, from first to last byte.

           .. warning:: You can brick your device with invalid size or content.
                        Use this function at your own risk, and RTFM.

           :param data: data to be written (should include the checksum)
           :param dry_run: log what should be written, do not actually
                           change the EEPROM content
        """
        if self.is_eeprom_internal:
            eeprom_size = self.INT_EEPROMS[self.device_version]
            if len(data) != eeprom_size:
                raise ValueError('Invalid EEPROM size')
        elif len(data) not in self.EXT_EEPROM_SIZES:
            raise ValueError('Invalid EEPROM size')
        self._write_eeprom_raw(0, data, dry_run=dry_run)

    def write_data(self, data: Union[bytes, bytearray]) -> int:
        """Write data to the FTDI port.

           In UART mode, data contains the serial stream to write to the UART
           interface.

           In MPSSE mode, data contains the sequence of MPSSE commands and
           data.

           Data buffer is split into chunk-sized blocks before being sent over
           the USB bus.

           :param data: the byte stream to send to the FTDI interface
           :return: count of written bytes
        """
        offset = 0
        size = len(data)
        try:
            while offset < size:
                write_size = self._writebuffer_chunksize
                if offset + write_size > size:
                    write_size = size - offset
                length = self._write(data[offset:offset+write_size])
                if length <= 0:
                    raise FtdiError("Usb bulk write error")
                offset += length
            return offset
        except USBError as exc:
            raise FtdiError(f'UsbError: {exc}') from exc

    def read_data_bytes(self, size: int, attempt: int = 1,
            request_gen: Optional[Callable[[int],
                                  Union[bytes, bytearray]]] = None) \
        -> bytearray:
        """Read data from the FTDI interface

           In UART mode, data contains the serial stream read from the UART
           interface.

           In MPSSE mode, data contains the sequence of data received and
           processed with the MPSEE engine.

           Data buffer is rebuilt from chunk-sized blocks received over the USB
           bus.

           FTDI device always sends internal status bytes, which are stripped
           out as not part of the data payload.

           Because of the multiple buses, buffers, FIFOs, and MPSSE command
           processing, data might not be immediately available on the host
           side. The attempt argument can be used to increase the attempt count
           to retrieve the expected amount of data, before giving up and
           returning all the received data, which may be shorted than the
           requested amount.

           :param size: the number of bytes to received from the device
           :param attempt: attempt cycle count
           :param request_gen: a callable that takes the number of bytes read
                               and expect a bytes byffer to send back to the
                               remote device. This is only useful to perform
                               optimized/continuous transfer from a slave
                               device.
           :return: payload bytes, as bytes
        """
        # Packet size sanity check
        if not self._max_packet_size:
            raise FtdiError("max_packet_size is bogus")
        packet_size = self._max_packet_size
        length = 1  # initial condition to enter the usb_read loop
        data = bytearray()
        # everything we want is still in the cache?
        if size <= len(self._readbuffer)-self._readoffset:
            data = self._readbuffer[self._readoffset:self._readoffset+size]
            self._readoffset += size
            return data
        # something still in the cache, but not enough to satisfy 'size'?
        if len(self._readbuffer)-self._readoffset != 0:
            data = self._readbuffer[self._readoffset:]
            # end of readbuffer reached
            self._readoffset = len(self._readbuffer)
        # read from USB, filling in the local cache as it is empty
        retry = attempt
        req_size = size
        try:
            while (len(data) < size) and (length > 0):
                while True:
                    tempbuf = self._read()
                    retry -= 1
                    length = len(tempbuf)
                    # the received buffer contains at least one useful databyte
                    # (first 2 bytes in each packet represent the current modem
                    # status)
                    if length >= 2:
                        if tempbuf[1] & self.TX_EMPTY_BITS:
                            if request_gen:
                                req_size -= length-2
                                if req_size > 0:
                                    cmd = request_gen(req_size)
                                    if cmd:
                                        self.write_data(cmd)
                    if length > 2:
                        retry = attempt
                        if self._latency_threshold:
                            self._adapt_latency(True)
                        # skip the status bytes
                        chunks = (length+packet_size-1) // packet_size
                        count = packet_size - 2
                        # if you want to show status, use the following code:
                        status = tempbuf[:2]
                        if status[1] & self.ERROR_BITS[1]:
                            self.log.error(
                                'FTDI error: %02x:%02x %s',
                                status[0], status[1], (' '.join(
                                    self.decode_modem_status(status,
                                                             True)).title()))
                        self._readbuffer = bytearray()
                        self._readoffset = 0
                        srcoff = 2
                        for _ in range(chunks):
                            self._readbuffer += tempbuf[srcoff:srcoff+count]
                            srcoff += packet_size
                        length = len(self._readbuffer)
                        break
                    # received buffer only contains the modem status bytes
                    # no data received, may be late, try again
                    if retry > 0:
                        continue
                    # no actual data
                    self._readbuffer = bytearray()
                    self._readoffset = 0
                    if self._latency_threshold:
                        self._adapt_latency(False)
                    # no more data to read?
                    return data
                if length > 0:
                    # data still fits in buf?
                    if (len(data) + length) <= size:
                        data += self._readbuffer[self._readoffset:
                                                 self._readoffset+length]
                        self._readoffset += length
                        # did we read exactly the right amount of bytes?
                        if len(data) == size:
                            return data
                    else:
                        # partial copy, not enough bytes in the local cache to
                        # fulfill the request
                        part_size = min(size-len(data),
                                        len(self._readbuffer)-self._readoffset)
                        if part_size < 0:
                            raise FtdiError("Internal Error")
                        data += self._readbuffer[self._readoffset:
                                                 self._readoffset+part_size]
                        self._readoffset += part_size
                        return data
        except USBError as exc:
            raise FtdiError(f'UsbError: {exc}') from exc
        # never reached
        raise FtdiError("Internal error")

    def read_data(self, size: int) -> bytes:
        """Shortcut to received a bytes buffer instead of the array of bytes.

           Note that output byte buffer may be shorted than the requested
           size.

           :param size: the number of bytes to received from the device
           :return: payload bytes
        """
        return bytes(self.read_data_bytes(size))

    def get_cts(self) -> bool:
        """Read terminal status line: Clear To Send

           :return: CTS line logical level
        """
        status = self.poll_modem_status()
        return bool(status & self.MODEM_CTS)

    def get_dsr(self) -> bool:
        """Read terminal status line: Data Set Ready

           :return: DSR line logical level
        """
        status = self.poll_modem_status()
        return bool(status & self.MODEM_DSR)

    def get_ri(self) -> bool:
        """Read terminal status line: Ring Indicator

           :return: RI line logical level
        """
        status = self.poll_modem_status()
        return bool(status & self.MODEM_RI)

    def get_cd(self) -> bool:
        """Read terminal status line: Carrier Detect

           :return: CD line logical level
        """
        status = self.poll_modem_status()
        return bool(status & self.MODEM_RLSD)

    def set_dynamic_latency(self, lmin: int, lmax: int,
                            threshold: int) -> None:
        """Set up or disable latency values.

           Dynamic latency management is a load balancer to adapt the
           responsiveness of FTDI read request vs. the host CPU load.

           It is mostly useful in UART mode, so that read bandwidth can be
           increased to the maximum achievable throughput, while maintaining
           very low host CPU load when no data is received from the UART.

           There should be no need to tweak the default values. Use with care.

           Minimum latency is limited to 12 or above, at FTDI device starts
           losing bytes when latency is too short...

           Maximum latency value is 255 ms.

           Polling latency is reset to `lmin` each time at least one payload
           byte is received from the FTDI device.

           It doubles, up to `lmax`, every `threshold` times no payload has
           been received from the FTDI device.

           :param lmin: minimum latency level (ms)
           :param lmax: maximum latenty level (ms)
           :param threshold: count to reset latency to maximum level
        """
        if not threshold:
            self._latency_count = 0
            self._latency_threshold = None
        else:
            for lat in (lmin, lmax):
                if not self.LATENCY_MIN <= lat <= self.LATENCY_MAX:
                    raise ValueError(f'Latency out of range: {lat}')
            self._latency_min = lmin
            self._latency_max = lmax
            self._latency_threshold = threshold
            self._latency = lmin
            self.set_latency_timer(self._latency)

    def validate_mpsse(self) -> None:
        """Check that the previous MPSSE request has been accepted by the FTDI
           device.

           :raise FtdiError: if the FTDI device rejected the command.
        """
        # only useful in MPSSE mode
        bytes_ = self.read_data(2)
        if (len(bytes_) >= 2) and (bytes_[0] == '\xfa'):
            raise FtdiError(f'Invalid command @ {bytes_[1]}')

    @classmethod
    def get_error_string(cls) -> str:
        """Wrapper for legacy compatibility.

           :return: a constant, meaningless string
        """
        return "Unknown error"

    # --- Private implementation -------------------------------------------

    def _set_interface(self, config: UsbConfiguration, ifnum: int):
        """Select the interface to use on the FTDI device"""
        if ifnum == 0:
            ifnum = 1
        if ifnum-1 not in range(config.bNumInterfaces):
            raise ValueError("No such interface for this device")
        self._interface = config[(ifnum-1, 0)]
        self._index = self._interface.bInterfaceNumber+1
        endpoints = sorted([ep.bEndpointAddress for ep in self._interface])
        self._in_ep, self._out_ep = endpoints[:2]

        # detach kernel driver from the interface
        try:
            if self._usb_dev.is_kernel_driver_active(self._index - 1):
                self._usb_dev.detach_kernel_driver(self._index - 1)
        except (NotImplementedError, USBError):
            pass

# pylint: disable=protected-access
# need to access private member _ctx of PyUSB device (resource manager)
# until PyUSB #302 is addressed

    def _reset_usb_device(self) -> None:
        """Reset USB device (USB command, not FTDI specific)."""
        self._usb_dev._ctx.backend.reset_device(self._usb_dev._ctx.handle)

    def _is_pyusb_handle_active(self) -> bool:
        # Unfortunately, we need to access pyusb ResourceManager
        # and there is no public API for this.
        return bool(self._usb_dev._ctx.handle)

# pylint: enable-msg=protected-access

    def _reset_device(self):
        """Reset the FTDI device (FTDI vendor command)"""
        if self._ctrl_transfer_out(Ftdi.SIO_REQ_RESET,
                                   Ftdi.SIO_RESET_SIO):
            raise FtdiError('Unable to reset FTDI device')

    def _ctrl_transfer_out(self, reqtype: int, value: int, data: bytes = b''):
        """Send a control message to the device"""
        try:
            return self._usb_dev.ctrl_transfer(
                Ftdi.REQ_OUT, reqtype, value, self._index,
                bytearray(data), self._usb_write_timeout)
        except USBError as exc:
            raise FtdiError(f'UsbError: {exc}') from None

    def _ctrl_transfer_in(self, reqtype: int, length: int):
        """Request for a control message from the device"""
        try:
            return self._usb_dev.ctrl_transfer(
                Ftdi.REQ_IN, reqtype, 0, self._index, length,
                self._usb_read_timeout)
        except USBError as exc:
            raise FtdiError(f'UsbError: {exc}') from None

    def _write(self, data: Union[bytes, bytearray]) -> int:
        if self._debug_log:
            try:
                self.log.debug('> %s', hexlify(data).decode())
            except TypeError as exc:
                self.log.warning('> (invalid output byte sequence: %s)', exc)
        if self._tracer:
            self._tracer.send(self._index, data)
        try:
            return self._usb_dev.write(self._in_ep, data,
                                       self._usb_write_timeout)
        except USBError as exc:
            raise FtdiError(f'UsbError: {exc}') from None

    def _read(self) -> bytes:
        try:
            data = self._usb_dev.read(self._out_ep, self._readbuffer_chunksize,
                                      self._usb_read_timeout)
        except USBError as exc:
            raise FtdiError(f'UsbError: {exc}') from None
        if data:
            if self._debug_log:
                self.log.debug('< %s', hexlify(data).decode())
            if self._tracer and len(data) > 2:
                self._tracer.receive(self._index, data[2:])
        return data

    def _adapt_latency(self, payload_detected: bool) -> None:
        """Dynamic latency adaptation depending on the presence of a
           payload in a RX buffer.

           :param payload_detected: whether a payload has been received
                                    within last RX buffer
        """
        if payload_detected:
            self._latency_count = 0
            if self._latency != self._latency_min:
                self.set_latency_timer(self._latency_min)
                self._latency = self._latency_min
            return
        # no payload received
        self._latency_count += 1
        if self._latency != self._latency_max:
            if self._latency_count > \
                    self._latency_threshold:
                self._latency *= 2
                if self._latency > self._latency_max:
                    self._latency = self._latency_max
                else:
                    self._latency_count = 0
                self.set_latency_timer(self._latency)

    def _check_eeprom_size(self, eeprom_size: Optional[int]) -> int:
        if self.device_version in self.INT_EEPROMS:
            if (eeprom_size and
                    eeprom_size != self.INT_EEPROMS[self.device_version]):
                raise ValueError(f'Invalid EEPROM size: {eeprom_size}')
            eeprom_size = self.INT_EEPROMS[self.device_version]
        else:
            if eeprom_size is None:
                eeprom_size = self.max_eeprom_size
            if eeprom_size not in self.EXT_EEPROM_SIZES:
                raise ValueError(f'Invalid EEPROM size: {eeprom_size}')
        return eeprom_size

    def _write_eeprom_raw(self, addr: int, data: Union[bytes, bytearray],
                          dry_run: bool = True) -> None:
        """Write multiple bytes to the EEPROM starting at byte address,
           addr. Length of data must be a multiple of 2 since the
           EEPROM is 16-bits. So automatically extend data by 1 byte
           if this is not the case.

           :param int addr: starting byte address to start writing
           :param bytes data: data to be written
           :param dry_run: log what should be written, do not actually
                           change the EEPROM content
        """
        if self.device_version == 0x0600:
            # FT232R internal EEPROM is unstable and latency timer seems
            # to have a direct impact on EEPROM programming...
            latency = self.get_latency_timer()
        else:
            latency = 0
        try:
            if latency:
                self.set_latency_timer(self.LATENCY_EEPROM_FT232R)
            length = len(data)
            if addr & 0x1 or length & 0x1:
                raise ValueError('Address/length not even')
            for word in sunpack(f'<{length//2}H', data):
                if not dry_run:
                    out = self._usb_dev.ctrl_transfer(
                        Ftdi.REQ_OUT, Ftdi.SIO_REQ_WRITE_EEPROM,
                        word, addr >> 1, b'', self._usb_write_timeout)
                    if out:
                        raise FtdiEepromError(f'EEPROM Write Error @ {addr}')
                    self.log.debug('Write EEPROM [0x%02x]: 0x%04x', addr, word)
                else:
                    self.log.info('Fake write EEPROM [0x%02x]: 0x%04x',
                                  addr, word)
                addr += 2
        finally:
            if latency:
                self.set_latency_timer(latency)

    def _get_max_packet_size(self) -> int:
        """Retrieve the maximum length of a data packet"""
        if not self.is_connected:
            raise IOError("Device is not yet known", ENODEV)
        if not self._interface:
            raise IOError("Interface is not yet known", ENODEV)
        endpoint = self._interface[0]
        packet_size = endpoint.wMaxPacketSize
        return packet_size

    def _convert_baudrate_legacy(self, baudrate: int) -> Tuple[int, int, int]:
        if baudrate > self.BAUDRATE_REF_BASE:
            raise ValueError('Invalid baudrate (too high)')
        div8 = int(round((8 * self.BAUDRATE_REF_BASE) / baudrate))
        if (div8 & 0x7) == 7:
            div8 += 1
        div = div8 >> 3
        div8 &= 0x7
        if div8 == 1:
            div |= 0xc000
        elif div8 >= 4:
            div |= 0x4000
        elif div8 != 0:
            div |= 0x8000
        elif div == 1:
            div = 0
        value = div & 0xFFFF
        index = (div >> 16) & 0xFFFF
        estimate = int(((8 * self.BAUDRATE_REF_BASE) + (div8//2))//div8)
        return estimate, value, index

    def _convert_baudrate(self, baudrate: int) -> Tuple[int, int, int]:
        """Convert a requested baudrate into the closest possible baudrate
           that can be assigned to the FTDI device

           :param baudrate: the baudrate in bps
           :return: a 3-uple of the apprimated baudrate, the value and index
                    to use as the USB configuration parameter
        """
        if self.device_version == 0x200:
            return self._convert_baudrate_legacy(baudrate)
        if self.is_H_series and baudrate >= 1200:
            hispeed = True
            clock = self.BAUDRATE_REF_HIGH
            bb_ratio = self.BITBANG_BAUDRATE_RATIO_HIGH
        else:
            hispeed = False
            clock = self.BAUDRATE_REF_BASE
            bb_ratio = self.BITBANG_BAUDRATE_RATIO_BASE
        if baudrate > clock:
            raise ValueError('Invalid baudrate (too high)')
        if baudrate < ((clock >> 14) + 1):
            raise ValueError('Invalid baudrate (too low)')
        if self.is_bitbang_enabled:
            baudrate //= bb_ratio
        div8 = int(round((8 * clock) / baudrate))
        div = div8 >> 3
        div |= self.FRAC_DIV_CODE[div8 & 0x7] << 14
        if div == 1:
            div = 0
        elif div == 0x4001:
            div = 1
        if hispeed:
            div |= 0x00020000
        value = div & 0xFFFF
        index = (div >> 16) & 0xFFFF
        if self.device_version >= 0x0700 or self.device_version == 0x0500:
            index <<= 8
            index |= self._index
        estimate = int(((8 * clock) + (div8//2))//div8)
        if self.is_bitbang_enabled:
            estimate *= bb_ratio
        return estimate, value, index

    def _set_baudrate(self, baudrate: int, constrain: bool) -> int:
        if self.is_mpsse:
            raise FtdiFeatureError('Cannot change frequency w/ current mode')
        actual, value, index = self._convert_baudrate(baudrate)
        delta = 100*abs(float(actual-baudrate))/baudrate
        self.log.debug('Actual baudrate: %d %.1f%% div [%04x:%04x]',
                       actual, delta, index, value)
        # return actual
        if constrain and delta > Ftdi.BAUDRATE_TOLERANCE:
            raise ValueError(f'Baudrate tolerance exceeded: {delta:.02f}% '
                             f'(wanted {baudrate}, achievable {actual})')
        try:
            if self._usb_dev.ctrl_transfer(
                    Ftdi.REQ_OUT, Ftdi.SIO_REQ_SET_BAUDRATE, value, index,
                    bytearray(), self._usb_write_timeout):
                raise FtdiError('Unable to set baudrate')
            return actual
        except USBError as exc:
            raise FtdiError('UsbError: {exc}') from exc

    def _set_frequency(self, frequency: float) -> float:
        """Convert a frequency value into a TCK divisor setting"""
        if not self.is_mpsse:
            raise FtdiFeatureError('Cannot change frequency w/ current mode')
        if frequency > self.frequency_max:
            raise FtdiFeatureError(f'Unsupported frequency: {frequency:.0f}')
        # Calculate base speed clock divider
        divcode = Ftdi.ENABLE_CLK_DIV5
        divisor = int((Ftdi.BUS_CLOCK_BASE+frequency/2)/frequency)-1
        divisor = max(0, min(0xFFFF, divisor))
        actual_freq = Ftdi.BUS_CLOCK_BASE/(divisor+1)
        error = (actual_freq/frequency)-1
        # Should we use high speed clock available in H series?
        if self.is_H_series:
            # Calculate high speed clock divider
            divisor_hs = int((Ftdi.BUS_CLOCK_HIGH+frequency/2)/frequency)-1
            divisor_hs = max(0, min(0xFFFF, divisor_hs))
            actual_freq_hs = Ftdi.BUS_CLOCK_HIGH/(divisor_hs+1)
            error_hs = (actual_freq_hs/frequency)-1
            # Enable if closer to desired frequency (percentually)
            if abs(error_hs) < abs(error):
                divcode = Ftdi.DISABLE_CLK_DIV5
                divisor = divisor_hs
                actual_freq = actual_freq_hs
                error = error_hs
        # FTDI expects little endian
        if self.is_H_series:
            cmd = bytearray((divcode,))
        else:
            cmd = bytearray()
        cmd.extend((Ftdi.SET_TCK_DIVISOR, divisor & 0xff,
                    (divisor >> 8) & 0xff))
        self.write_data(cmd)
        self.validate_mpsse()
        # Drain input buffer
        self.purge_rx_buffer()
        # Note that bus frequency may differ from clock frequency, when
        # 3-phase clock is enable, in which case bus frequency = 2/3 clock
        # frequency
        if actual_freq > 1E6:
            self.log.debug('Clock frequency: %.6f MHz (error: %+.1f %%)',
                           (actual_freq/1E6), error*100)
        else:
            self.log.debug('Clock frequency: %.3f KHz (error: %+.1f %%)',
                           (actual_freq/1E3), error*100)
        return actual_freq

    def __get_timeouts(self) -> Tuple[int, int]:
        return self._usb_read_timeout, self._usb_write_timeout

    def __set_timeouts(self, timeouts: Tuple[int, int]):
        (read_timeout, write_timeout) = timeouts
        self._usb_read_timeout = read_timeout
        self._usb_write_timeout = write_timeout

    timeouts = property(__get_timeouts, __set_timeouts)
