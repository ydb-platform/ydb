# Copyright (c) 2017-2025, Emmanuel Blot <emmanuel.blot@free.fr>
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

"""I2C support for PyFdti"""

from binascii import hexlify
from collections import namedtuple
from logging import getLogger
from struct import calcsize as scalc, pack as spack, unpack as sunpack
from threading import Lock
from typing import Any, Iterable, Mapping, Optional, Tuple, Union
from usb.core import Device as UsbDevice
from .ftdi import Ftdi, FtdiFeatureError
from .misc import to_bool


class I2cIOError(IOError):
    """I2c I/O error"""


class I2cNackError(I2cIOError):
    """I2c NACK receive from slave"""


class I2cTimeoutError(TimeoutError):
    """I2c timeout on polling"""


class I2cPort:
    """I2C port.

       An I2C port is never instanciated directly:
       use :py:meth:`I2cController.get_port()` method to obtain an I2C port.

       ``relax`` parameter in I2cPort methods may be used to prevent the master
       from releasing the I2C bus, if some further data should be exchanged
       with the slave device. Note that in case of any error, the I2C bus is
       released and the ``relax`` parameter is ignored in such an event.

       Example:

       >>> ctrl = I2cController()
       >>> ctrl.configure('ftdi://ftdi:232h/1')
       >>> i2c = ctrl.get_port(0x21)
       >>> # send 2 bytes
       >>> i2c.write([0x12, 0x34])
       >>> # send 2 bytes, then receive 2 bytes
       >>> out = i2c.exchange([0x12, 0x34], 2)
    """
    FORMATS = {scalc(fmt): fmt for fmt in 'BHI'}

    def __init__(self, controller: 'I2cController', address: int):
        self._controller = controller
        self._address = address
        self._shift = 0
        self._endian = '<'
        self._format = 'B'

    def configure_register(self,
                           bigendian: bool = False, width: int = 1) -> None:
        """Reconfigure the format of the slave address register (if any)

            :param bigendian: True for a big endian encoding, False otherwise
            :param width: width, in bytes, of the register
        """
        try:
            self._format = self.FORMATS[width]
        except KeyError as exc:
            raise I2cIOError('Unsupported integer width') from exc
        self._endian = '>' if bigendian else '<'

    def shift_address(self, offset: int):
        """Tweak the I2C slave address, as required with some devices
        """
        I2cController.validate_address(self._address+offset)
        self._shift = offset

    def read(self, readlen: int = 0, relax: bool = True,
             start: bool = True) -> bytes:
        """Read one or more bytes from a remote slave

           :param readlen: count of bytes to read out.
           :param relax: whether to relax the bus (emit STOP) or not
           :param start: whether to emit a start sequence (w/ address)
           :return: byte sequence of read out bytes
           :raise I2cIOError: if device is not configured or input parameters
                              are invalid
        """
        return self._controller.read(
            self._address+self._shift if start else None,
            readlen=readlen, relax=relax)

    def write(self, out: Union[bytes, bytearray, Iterable[int]],
              relax: bool = True, start: bool = True) -> None:
        """Write one or more bytes to a remote slave

           :param out: the byte buffer to send
           :param relax: whether to relax the bus (emit STOP) or not
           :param start: whether to emit a start sequence (w/ address)
           :raise I2cIOError: if device is not configured or input parameters
                              are invalid
        """
        return self._controller.write(
            self._address+self._shift if start else None,
            out, relax=relax)

    def read_from(self, regaddr: int, readlen: int = 0,
                  relax: bool = True, start: bool = True) -> bytes:
        """Read one or more bytes from a given register at remote slave

           :param regaddr: slave register address to read from
           :param readlen: count of bytes to read out.
           :param relax: whether to relax the bus (emit STOP) or not
           :param start: whether to emit a start sequence (w/ address)
           :return: data read out from the slave
           :raise I2cIOError: if device is not configured or input parameters
                              are invalid
        """
        return self._controller.exchange(
            self._address+self._shift if start else None,
            out=self._make_buffer(regaddr), readlen=readlen, relax=relax)

    def write_to(self, regaddr: int,
                 out: Union[bytes, bytearray, Iterable[int]],
                 relax: bool = True, start: bool = True):
        """Write one or more bytes to a given register at a remote slave

           :param regaddr: slave register address to write to
           :param out: the byte buffer to send
           :param relax: whether to relax the bus (emit STOP) or not
           :param start: whether to emit a start sequence (w/ address)
           :raise I2cIOError: if device is not configured or input parameters
                              are invalid
        """
        return self._controller.write(
            self._address+self._shift if start else None,
            out=self._make_buffer(regaddr, out), relax=relax)

    def exchange(self, out: Union[bytes, bytearray, Iterable[int]] = b'',
                 readlen: int = 0,
                 relax: bool = True, start: bool = True) -> bytes:
        """Perform an exchange or a transaction with the I2c slave

           :param out: an array of bytes to send to the I2c slave,
                       may be empty to only read out data from the slave
           :param readlen: count of bytes to read out from the slave,
                       may be zero to only write to the slave
           :param relax: whether to relax the bus (emit STOP) or not
           :param start: whether to emit a start sequence (w/ address)
           :return: data read out from the slave
        """
        return self._controller.exchange(
            self._address+self._shift if start else None, out,
            readlen, relax=relax)

    def poll(self, write: bool = False,
             relax: bool = True, start: bool = True) -> bool:
        """Poll a remote slave, expect ACK or NACK.

           :param write: poll in write mode (vs. read)
           :param relax: whether to relax the bus (emit STOP) or not
           :param start: whether to emit a start sequence (w/ address)
           :return: True if the slave acknowledged, False otherwise
        """
        return self._controller.poll(
            self._address+self._shift if start else None, write,
            relax=relax)

    def poll_cond(self, width: int, mask: int, value: int, count: int,
                  relax: bool = True, start: bool = True) -> Optional[bytes]:
        """Poll a remove slave, watching for condition to satisfy.
           On each poll cycle, a repeated start condition is emitted, without
           releasing the I2C bus, and an ACK is returned to the slave.

           If relax is set, this method releases the I2C bus however it leaves.

           :param width: count of bytes to poll for the condition check,
                that is the size of the condition register
           :param mask: binary mask to apply on the condition register
                before testing for the value
           :param value: value to test the masked condition register
                against. Condition is satisfied when register & mask == value
           :param count: maximum poll count before raising a timeout
           :param relax: whether to relax the bus (emit STOP) or not
           :param start: whether to emit a start sequence (w/ address)
           :return: the polled register value
           :raise I2cTimeoutError: if poll condition is not satisified
        """
        try:
            fmt = ''.join((self._endian, self.FORMATS[width]))
        except KeyError as exc:
            raise I2cIOError('Unsupported integer width') from exc
        return self._controller.poll_cond(
            self._address+self._shift if start else None,
            fmt, mask, value, count, relax=relax)

    def flush(self) -> None:
        """Force the flush of the HW FIFOs.
        """
        self._controller.flush()

    @property
    def frequency(self) -> float:
        """Provide the current I2c bus frequency.
        """
        return self._controller.frequency

    @property
    def address(self) -> int:
        """Return the slave address."""
        return self._address

    def _make_buffer(self, regaddr: int,
                     out: Union[bytes, bytearray, Iterable[int],
                                None] = None) -> bytes:
        data = bytearray()
        data.extend(spack(f'{self._endian}{self._format}', regaddr))
        if out:
            data.extend(out)
        return bytes(data)


class I2cGpioPort:
    """GPIO port

       A I2cGpioPort instance enables to drive GPIOs wich are not reserved for
       I2c feature as regular GPIOs.

       GPIO are managed as a bitfield. The LSBs are reserved for the I2c
       feature, which means that the lowest pin that can be used as a GPIO is
       *b3*:

       * *b0*: I2C SCL
       * *b1*: I2C SDA_O
       * *b2*: I2C SDA_I
       * *b3*: first GPIO
       * *b7*: reserved for I2C clock stretching, if this mode is enabled

       There is no offset bias in GPIO bit position, *i.e.* the first available
       GPIO can be reached from as ``0x08``.

       Bitfield size depends on the FTDI device: 4432H series use 8-bit GPIO
       ports, while 232H and 2232H series use wide 16-bit ports.

       An I2cGpio port is never instanciated directly: use
       :py:meth:`I2cController.get_gpio()` method to obtain the GPIO port.
    """
    def __init__(self, controller: 'I2cController'):
        self.log = getLogger('pyftdi.i2c.gpio')
        self._controller = controller

    @property
    def pins(self) -> int:
        """Report the configured GPIOs as a bitfield.

           A true bit represents a GPIO, a false bit a reserved or not
           configured pin.

           :return: the bitfield of configured GPIO pins.
        """
        return self._controller.gpio_pins

    @property
    def all_pins(self) -> int:
        """Report the addressable GPIOs as a bitfield.

           A true bit represents a pin which may be used as a GPIO, a false bit
           a reserved pin (for I2C support)

           :return: the bitfield of configurable GPIO pins.
        """
        return self._controller.gpio_all_pins

    @property
    def width(self) -> int:
        """Report the FTDI count of addressable pins.

           Note that all pins, including reserved I2C ones, are reported.

           :return: the count of IO pins (including I2C ones).
        """
        return self._controller.width

    @property
    def direction(self) -> int:
        """Provide the FTDI GPIO direction.self

           A true bit represents an output GPIO, a false bit an input GPIO.

           :return: the bitfield of direction.
        """
        return self._controller.direction

    def read(self, with_output: bool = False) -> int:
        """Read GPIO port.

           :param with_output: set to unmask output pins
           :return: the GPIO port pins as a bitfield
        """
        return self._controller.read_gpio(with_output)

    def write(self, value: int) -> None:
        """Write GPIO port.

           :param value: the GPIO port pins as a bitfield
        """
        return self._controller.write_gpio(value)

    def set_direction(self, pins: int, direction: int) -> None:
        """Change the direction of the GPIO pins.

           :param pins: which GPIO pins should be reconfigured
           :param direction: direction bitfield (high level for output)
        """
        self._controller.set_gpio_direction(pins, direction)


I2CTimings = namedtuple('I2CTimings', 't_hd_sta t_su_sta t_su_sto t_buf')
"""I2C standard timings.
"""


class I2cController:
    """I2c master.

       An I2c master should be instanciated only once for each FTDI port that
       supports MPSSE (one or two ports, depending on the FTDI device).

       Once configured, :py:func:`get_port` should be invoked to obtain an I2c
       port for each I2c slave to drive. I2c port should handle all I/O
       requests for its associated HW slave.

       It is not recommended to use I2cController :py:func:`read`,
       :py:func:`write` or :py:func:`exchange` directly.

       * ``SCK`` should be connected to ``A*BUS0``, and ``A*BUS7`` if clock
         stretching mode is enabled
       * ``SDA`` should be connected to ``A*BUS1`` **and** ``A*BUS2``
    """

    LOW = 0x00
    HIGH = 0xff
    BIT0 = 0x01
    IDLE = HIGH
    SCL_BIT = 0x01  # AD0
    SDA_O_BIT = 0x02  # AD1
    SDA_I_BIT = 0x04  # AD2
    SCL_FB_BIT = 0x80  # AD7
    PAYLOAD_MAX_LENGTH = 0xFF00  # 16 bits max (- spare for control)
    HIGHEST_I2C_ADDRESS = 0x7F
    DEFAULT_BUS_FREQUENCY = 100000.0
    HIGH_BUS_FREQUENCY = 400000.0
    RETRY_COUNT = 3

    I2C_MASK = SCL_BIT | SDA_O_BIT | SDA_I_BIT
    I2C_MASK_CS = SCL_BIT | SDA_O_BIT | SDA_I_BIT | SCL_FB_BIT
    I2C_DIR = SCL_BIT | SDA_O_BIT

    I2C_100K = I2CTimings(4.0E-6, 4.7E-6, 4.0E-6, 4.7E-6)
    I2C_400K = I2CTimings(0.6E-6, 0.6E-6, 0.6E-6, 1.3E-6)
    I2C_1M = I2CTimings(0.26E-6, 0.26E-6, 0.26E-6, 0.5E-6)

    def __init__(self):
        self._ftdi = Ftdi()
        self._lock = Lock()
        self.log = getLogger('pyftdi.i2c')
        self._gpio_port = None
        self._gpio_dir = 0
        self._gpio_low = 0
        self._gpio_mask = 0
        self._i2c_mask = 0
        self._wide_port = False
        self._slaves = {}
        self._retry_count = self.RETRY_COUNT
        self._frequency = 0.0
        self._immediate = (Ftdi.SEND_IMMEDIATE,)
        self._read_bit = (Ftdi.READ_BITS_PVE_MSB, 0)
        self._read_byte = (Ftdi.READ_BYTES_PVE_MSB, 0, 0)
        self._write_byte = (Ftdi.WRITE_BYTES_NVE_MSB, 0, 0)
        self._nack = (Ftdi.WRITE_BITS_NVE_MSB, 0, self.HIGH)
        self._ack = (Ftdi.WRITE_BITS_NVE_MSB, 0, self.LOW)
        self._ck_delay = 1
        self._fake_tristate = False
        self._tx_size = 1
        self._rx_size = 1
        self._ck_hd_sta = 0
        self._ck_su_sto = 0
        self._ck_idle = 0
        self._read_optim = True
        self._disable_3phase_clock = False
        self._clkstrch = False

    def set_retry_count(self, count: int) -> None:
        """Change the default retry count when a communication error occurs,
           before bailing out.
           :param count: count of retries
        """
        if not isinstance(count, int) or not 0 < count <= 16:
            raise ValueError('Invalid retry count')
        self._retry_count = count

    def configure(self, url: Union[str, UsbDevice],
                  **kwargs: Mapping[str, Any]) -> None:
        """Configure the FTDI interface as a I2c master.

           :param url: FTDI URL string, such as ``ftdi://ftdi:232h/1``
           :param kwargs: options to configure the I2C bus

           Accepted options:

           * ``interface``: when URL is specifed as a USB device, the interface
             named argument can be used to select a specific port of the FTDI
             device, as an integer starting from 1.
           * ``direction`` a bitfield specifying the FTDI GPIO direction,
             where high level defines an output, and low level defines an
             input. Only useful to setup default IOs at start up, use
             :py:class:`I2cGpioPort` to drive GPIOs. Note that pins reserved
             for I2C feature take precedence over any this setting.
           * ``initial`` a bitfield specifying the initial output value. Only
             useful to setup default IOs at start up, use
             :py:class:`I2cGpioPort` to drive GPIOs.
           * ``frequency`` float value the I2C bus frequency in Hz
           * ``clockstretching`` boolean value to enable clockstreching.
             xD7 (GPIO7) pin should be connected back to xD0 (SCK)
           * ``debug`` to increase log verbosity, using MPSSE tracer
        """
        if 'frequency' in kwargs:
            frequency = kwargs['frequency']
            del kwargs['frequency']
        else:
            frequency = self.DEFAULT_BUS_FREQUENCY
        # Fix frequency for 3-phase clock
        if frequency <= 100E3:
            timings = self.I2C_100K
        elif frequency <= 400E3:
            timings = self.I2C_400K
        else:
            timings = self.I2C_1M
        if 'clockstretching' in kwargs:
            self._clkstrch = bool(kwargs['clockstretching'])
            del kwargs['clockstretching']
        else:
            self._clkstrch = False
        if 'direction' in kwargs:
            io_dir = int(kwargs['direction'])
            del kwargs['direction']
        else:
            io_dir = 0
        if 'initial' in kwargs:
            io_out = int(kwargs['initial'])
            del kwargs['initial']
        else:
            io_out = 0
        if 'interface' in kwargs:
            if isinstance(url, str):
                raise I2cIOError('url and interface are mutually exclusive')
            interface = int(kwargs['interface'])
            del kwargs['interface']
        else:
            interface = 1
        if 'rdoptim' in kwargs:
            self._read_optim = to_bool(kwargs['rdoptim'])
            del kwargs['rdoptim']
        with self._lock:
            self._ck_hd_sta = self._compute_delay_cycles(timings.t_hd_sta)
            self._ck_su_sto = self._compute_delay_cycles(timings.t_su_sto)
            ck_su_sta = self._compute_delay_cycles(timings.t_su_sta)
            ck_buf = self._compute_delay_cycles(timings.t_buf)
            self._ck_idle = max(ck_su_sta, ck_buf)
            self._ck_delay = ck_buf
            if self._clkstrch:
                self._i2c_mask = self.I2C_MASK_CS
            else:
                self._i2c_mask = self.I2C_MASK
            # until the device is open, there is no way to tell if it has a
            # wide (16) or narrow port (8). Lower API can deal with any, so
            # delay any truncation till the device is actually open
            self._set_gpio_direction(16, io_out, io_dir)
            # as 3-phase clock frequency mode is required for I2C mode, the
            # FTDI clock should be adapted to match the required frequency.
            kwargs['direction'] = self.I2C_DIR | self._gpio_dir
            kwargs['initial'] = self.IDLE | (io_out & self._gpio_mask)
            kwargs['frequency'] = (3.0*frequency)/2.0
            if not isinstance(url, str):
                frequency = self._ftdi.open_mpsse_from_device(
                    url, interface=interface, **kwargs)
            else:
                frequency = self._ftdi.open_mpsse_from_url(url, **kwargs)
            self._frequency = (2.0*frequency)/3.0
            self._tx_size, self._rx_size = self._ftdi.fifo_sizes
            if not self._disable_3phase_clock:
                self._ftdi.enable_3phase_clock(True)
            try:
                self._ftdi.enable_drivezero_mode(self.SCL_BIT |
                                                 self.SDA_O_BIT |
                                                 self.SDA_I_BIT)
            except FtdiFeatureError:
                # when open collector feature is not available (FT2232, FT4232)
                # SDA line is temporary move to high-z to enable ACK/NACK
                # read back from slave
                self._fake_tristate = True
            self._wide_port = self._ftdi.has_wide_port
            if not self._wide_port:
                self._set_gpio_direction(8, io_out & 0xFF, io_dir & 0xFF)

    def force_clock_mode(self, enable: bool) -> None:
        """Force unsupported I2C clock signalling on devices that have no I2C
           capabilities (i.e. FT2232D). I2cController cowardly refuses to use
           unsupported devices. When this mode is enabled, I2cController can
           drive such devices, but I2C signalling is not compliant with I2C
           specifications and may not work with most I2C slaves.

           :py:meth:`force_clock_mode` should always be called before
           :py:meth:`configure` to be effective.

           This is a fully unsupported feature (bug reports will be ignored).

           :param enable: whether to drive non-I2C capable devices.
        """
        if enable:
            self.log.info('I2C signalling forced to non-I2C compliant mode.')
        self._disable_3phase_clock = enable

    def close(self, freeze: bool = False) -> None:
        """Close the FTDI interface.

           :param freeze: if set, FTDI port is not reset to its default
                          state on close.
        """
        with self._lock:
            if self._ftdi.is_connected:
                self._ftdi.close(freeze)

    def terminate(self) -> None:
        """Close the FTDI interface.

           :note: deprecated API, use close()
        """
        self.close()

    def get_port(self, address: int) -> I2cPort:
        """Obtain an I2cPort to drive an I2c slave.

           :param address: the address on the I2C bus
           :return: an I2cPort instance
        """
        if not self._ftdi.is_connected:
            raise I2cIOError("FTDI controller not initialized")
        self.validate_address(address)
        if address not in self._slaves:
            self._slaves[address] = I2cPort(self, address)
        return self._slaves[address]

    def get_gpio(self) -> I2cGpioPort:
        """Retrieve the GPIO port.

           :return: GPIO port
        """
        with self._lock:
            if not self._ftdi.is_connected:
                raise I2cIOError("FTDI controller not initialized")
            if not self._gpio_port:
                self._gpio_port = I2cGpioPort(self)
            return self._gpio_port

    @property
    def ftdi(self) -> Ftdi:
        """Return the Ftdi instance.

           :return: the Ftdi instance
        """
        return self._ftdi

    @property
    def configured(self) -> bool:
        """Test whether the device has been properly configured.

           :return: True if configured
        """
        return self._ftdi.is_connected and bool(self._start)

    @classmethod
    def validate_address(cls, address: Optional[int]) -> None:
        """Assert an I2C slave address is in the supported range.
           None is a special bypass address.

           :param address: the address on the I2C bus
           :raise I2cIOError: if the I2C slave address is not supported
        """
        if address is None:
            return
        if address > cls.HIGHEST_I2C_ADDRESS:
            raise I2cIOError(f'No such I2c slave: 0x{address:02x}')

    @property
    def frequency_max(self) -> float:
        """Provides the maximum I2C clock frequency in Hz.

           :return: I2C bus clock frequency
        """
        return self._ftdi.frequency_max

    @property
    def frequency(self) -> float:
        """Provides the current I2C clock frequency in Hz.

           :return: the I2C bus clock frequency
        """
        return self._frequency

    @property
    def direction(self) -> int:
        """Provide the FTDI pin direction

           A true bit represents an output pin, a false bit an input pin.

           :return: the bitfield of direction.
        """
        return self.I2C_DIR | self._gpio_dir

    @property
    def gpio_pins(self) -> int:
        """Report the configured GPIOs as a bitfield.

           A true bit represents a GPIO, a false bit a reserved or not
           configured pin.

           :return: the bitfield of configured GPIO pins.
        """
        with self._lock:
            return self._gpio_mask

    @property
    def gpio_all_pins(self) -> int:
        """Report the addressable GPIOs as a bitfield.

           A true bit represents a pin which may be used as a GPIO, a false bit
           a reserved pin (for I2C support)

           :return: the bitfield of configurable GPIO pins.
        """
        mask = (1 << self.width) - 1
        with self._lock:
            return mask & ~self._i2c_mask

    @property
    def width(self) -> int:
        """Report the FTDI count of addressable pins.

           :return: the count of IO pins (including I2C ones).
        """
        return 16 if self._wide_port else 8

    def read(self, address: Optional[int], readlen: int = 1,
             relax: bool = True) -> bytes:
        """Read one or more bytes from a remote slave

           :param address: the address on the I2C bus, or None to discard start
           :param readlen: count of bytes to read out.
           :param relax: not used
           :return: read bytes
           :raise I2cIOError: if device is not configured or input parameters
                              are invalid

           Address is a logical slave address (0x7f max)

           Most I2C devices require a register address to read out
           check out the exchange() method.
        """
        if not self.configured:
            raise I2cIOError("FTDI controller not initialized")
        self.validate_address(address)
        if address is None:
            i2caddress = None
        else:
            i2caddress = (address << 1) & self.HIGH
            i2caddress |= self.BIT0
        retries = self._retry_count
        do_epilog = True
        with self._lock:
            while True:
                try:
                    self._do_prolog(i2caddress)
                    data = self._do_read(readlen)
                    do_epilog = relax
                    return data
                except I2cNackError:
                    retries -= 1
                    if not retries:
                        raise
                    self.log.warning('Retry read')
                finally:
                    if do_epilog:
                        self._do_epilog()

    def write(self, address: Optional[int],
              out: Union[bytes, bytearray, Iterable[int]],
              relax: bool = True) -> None:
        """Write one or more bytes to a remote slave

           :param address: the address on the I2C bus, or None to discard start
           :param out: the byte buffer to send
           :param relax: whether to relax the bus (emit STOP) or not
           :raise I2cIOError: if device is not configured or input parameters
                              are invalid

           Address is a logical slave address (0x7f max)

           Most I2C devices require a register address to write into. It should
           be added as the first (byte)s of the output buffer.
        """
        if not self.configured:
            raise I2cIOError("FTDI controller not initialized")
        self.validate_address(address)
        if address is None:
            i2caddress = None
        else:
            i2caddress = (address << 1) & self.HIGH
        retries = self._retry_count
        do_epilog = True
        with self._lock:
            while True:
                try:
                    self._do_prolog(i2caddress)
                    self._do_write(out)
                    do_epilog = relax
                    return
                except I2cNackError:
                    retries -= 1
                    if not retries:
                        raise
                    self.log.warning('Retry write')
                finally:
                    if do_epilog:
                        self._do_epilog()

    def exchange(self, address: Optional[int],
                 out: Union[bytes, bytearray, Iterable[int]],
                 readlen: int = 0, relax: bool = True) -> bytearray:
        """Send a byte sequence to a remote slave followed with
           a read request of one or more bytes.

           This command is useful to tell the slave what data
           should be read out.

           :param address: the address on the I2C bus, or None to discard start
           :param out: the byte buffer to send
           :param readlen: count of bytes to read out.
           :param relax: whether to relax the bus (emit STOP) or not
           :return: read bytes
           :raise I2cIOError: if device is not configured or input parameters
                              are invalid

           Address is a logical slave address (0x7f max)
        """
        if not self.configured:
            raise I2cIOError("FTDI controller not initialized")
        self.validate_address(address)
        if readlen < 1:
            raise I2cIOError('Nothing to read')
        if readlen > (self.PAYLOAD_MAX_LENGTH/3-1):
            raise I2cIOError("Input payload is too large")
        if address is None:
            i2caddress = None
        else:
            i2caddress = (address << 1) & self.HIGH
        retries = self._retry_count
        do_epilog = True
        data = bytearray()
        with self._lock:
            while True:
                try:
                    self._do_prolog(i2caddress)
                    self._do_write(out)
                    if i2caddress is not None:
                        self._do_prolog(i2caddress | self.BIT0)
                    if readlen:
                        data = self._do_read(readlen)
                    do_epilog = relax
                    return data
                except I2cNackError:
                    retries -= 1
                    if not retries:
                        raise
                    self.log.warning('Retry exchange')
                finally:
                    if do_epilog:
                        self._do_epilog()

    def poll(self, address: int, write: bool = False,
             relax: bool = True) -> bool:
        """Poll a remote slave, expect ACK or NACK.

           :param address: the address on the I2C bus, or None to discard start
           :param write: poll in write mode (vs. read)
           :param relax: whether to relax the bus (emit STOP) or not
           :return: True if the slave acknowledged, False otherwise
        """
        if not self.configured:
            raise I2cIOError("FTDI controller not initialized")
        self.validate_address(address)
        if address is None:
            i2caddress = None
        else:
            i2caddress = (address << 1) & self.HIGH
            if not write:
                i2caddress |= self.BIT0
        do_epilog = True
        with self._lock:
            try:
                self._do_prolog(i2caddress)
                do_epilog = relax
                return True
            except I2cNackError:
                self.log.info('Not ready')
                return False
            finally:
                if do_epilog:
                    self._do_epilog()

    def poll_cond(self, address: int, fmt: str, mask: int, value: int,
                  count: int, relax: bool = True) -> Optional[bytes]:
        """Poll a remove slave, watching for condition to satisfy.
           On each poll cycle, a repeated start condition is emitted, without
           releasing the I2C bus, and an ACK is returned to the slave.

           If relax is set, this method releases the I2C bus however it leaves.

           :param address: the address on the I2C bus, or None to discard start
           :param fmt: struct format for poll register
           :param mask: binary mask to apply on the condition register
                before testing for the value
           :param value: value to test the masked condition register
                against. Condition is satisfied when register & mask == value
           :param count: maximum poll count before raising a timeout
           :param relax: whether to relax the bus (emit STOP) or not
           :return: the polled register value, or None if poll failed
        """
        if not self.configured:
            raise I2cIOError("FTDI controller not initialized")
        self.validate_address(address)
        if address is None:
            i2caddress = None
        else:
            i2caddress = (address << 1) & self.HIGH
            i2caddress |= self.BIT0
        do_epilog = True
        with self._lock:
            try:
                retry = 0
                while retry < count:
                    retry += 1
                    size = scalc(fmt)
                    self._do_prolog(i2caddress)
                    data = self._do_read(size)
                    self.log.debug("Poll data: %s", hexlify(data).decode())
                    cond, = sunpack(fmt, data)
                    if (cond & mask) == value:
                        self.log.debug('Poll condition matched')
                        break
                    data = None
                    self.log.debug('Poll condition not fulfilled: %x/%x',
                                   cond & mask, value)
                do_epilog = relax
                if not data:
                    self.log.warning('Poll condition failed')
                return data
            except I2cNackError:
                self.log.info('Not ready')
                return None
            finally:
                if do_epilog:
                    self._do_epilog()

    def flush(self) -> None:
        """Flush the HW FIFOs.
        """
        if not self.configured:
            raise I2cIOError("FTDI controller not initialized")
        with self._lock:
            self._ftdi.write_data(bytearray(self._immediate))
            self._ftdi.purge_buffers()

    def read_gpio(self, with_output: bool = False) -> int:
        """Read GPIO port.

           :param with_output: set to unmask output pins
           :return: the GPIO port pins as a bitfield
        """
        with self._lock:
            data = self._read_raw(self._wide_port)
        value = data & self._gpio_mask
        if not with_output:
            value &= ~self._gpio_dir
        return value

    def write_gpio(self, value: int) -> None:
        """Write GPIO port.

           :param value: the GPIO port pins as a bitfield
        """
        with self._lock:
            if (value & self._gpio_dir) != value:
                raise I2cIOError(f'No such GPO pins: '
                                 f'{self._gpio_dir:04x}/{value:04x}')
            # perform read-modify-write
            use_high = self._wide_port and (self.direction & 0xff00)
            data = self._read_raw(use_high)
            data &= ~self._gpio_mask
            data |= value
            self._write_raw(data, use_high)
            self._gpio_low = data & 0xFF & ~self._i2c_mask

    def set_gpio_direction(self, pins: int, direction: int) -> None:
        """Change the direction of the GPIO pins.

           :param pins: which GPIO pins should be reconfigured
           :param direction: direction bitfield (on for output)
        """
        with self._lock:
            self._set_gpio_direction(16 if self._wide_port else 8,
                                     pins, direction)

    def _set_gpio_direction(self, width: int, pins: int,
                            direction: int) -> None:
        if pins & self._i2c_mask:
            raise I2cIOError('Cannot access I2C pins as GPIO')
        gpio_mask = (1 << width) - 1
        gpio_mask &= ~self._i2c_mask
        if (pins & gpio_mask) != pins:
            raise I2cIOError('No such GPIO pin(s)')
        self._gpio_dir &= ~pins
        self._gpio_dir |= (pins & direction)
        self._gpio_mask = gpio_mask & pins

    @property
    def _data_lo(self) -> Tuple[int]:
        return (Ftdi.SET_BITS_LOW,
                self.SCL_BIT | self._gpio_low,
                self.I2C_DIR | (self._gpio_dir & 0xFF))

    @property
    def _clk_lo_data_hi(self) -> Tuple[int]:
        return (Ftdi.SET_BITS_LOW,
                self.SDA_O_BIT | self._gpio_low,
                self.I2C_DIR | (self._gpio_dir & 0xFF))

    @property
    def _clk_lo_data_input(self) -> Tuple[int]:
        return (Ftdi.SET_BITS_LOW,
                self.LOW | self._gpio_low,
                self.SCL_BIT | (self._gpio_dir & 0xFF))

    @property
    def _clk_lo_data_lo(self) -> Tuple[int]:
        return (Ftdi.SET_BITS_LOW,
                self._gpio_low,
                self.I2C_DIR | (self._gpio_dir & 0xFF))

    @property
    def _clk_input_data_input(self) -> Tuple[int]:
        return (Ftdi.SET_BITS_LOW,
                self.I2C_DIR | self._gpio_low,
                (self._gpio_dir & 0xFF))

    @property
    def _idle(self) -> Tuple[int]:
        return (Ftdi.SET_BITS_LOW,
                self.I2C_DIR | self._gpio_low,
                self.I2C_DIR | (self._gpio_dir & 0xFF))

    @property
    def _start(self) -> Tuple[int]:
        return self._data_lo * self._ck_hd_sta + \
               self._clk_lo_data_lo * self._ck_hd_sta

    @property
    def _stop(self) -> Tuple[int]:
        return self._clk_lo_data_hi * self._ck_hd_sta + \
               self._clk_lo_data_lo * self._ck_hd_sta + \
               self._data_lo * self._ck_su_sto + \
               self._idle * self._ck_idle

    def _compute_delay_cycles(self, value: Union[int, float]) -> int:
        # approx ceiling without relying on math module
        # the bit delay is far from being precisely known anyway
        bit_delay = self._ftdi.mpsse_bit_delay
        return max(1, int((value + bit_delay) / bit_delay))

    def _read_raw(self, read_high: bool) -> int:
        if read_high:
            cmd = bytes([Ftdi.GET_BITS_LOW,
                         Ftdi.GET_BITS_HIGH,
                         Ftdi.SEND_IMMEDIATE])
            fmt = '<H'
        else:
            cmd = bytes([Ftdi.GET_BITS_LOW,
                         Ftdi.SEND_IMMEDIATE])
            fmt = 'B'
        self._ftdi.write_data(cmd)
        size = scalc(fmt)
        data = self._ftdi.read_data_bytes(size, 4)
        if len(data) != size:
            raise I2cIOError('Cannot read GPIO')
        value, = sunpack(fmt, data)
        return value

    def _write_raw(self, data: int, write_high: bool):
        direction = self.direction
        low_data = data & 0xFF
        low_dir = direction & 0xFF
        if write_high:
            high_data = (data >> 8) & 0xFF
            high_dir = (direction >> 8) & 0xFF
            cmd = bytes([Ftdi.SET_BITS_LOW, low_data, low_dir,
                         Ftdi.SET_BITS_HIGH, high_data, high_dir])
        else:
            cmd = bytes([Ftdi.SET_BITS_LOW, low_data, low_dir])
        self._ftdi.write_data(cmd)

    def _do_prolog(self, i2caddress: Optional[int]) -> None:
        if i2caddress is None:
            return
        self.log.debug('   prolog 0x%x', i2caddress >> 1)
        cmd = bytearray(self._idle * self._ck_delay)
        cmd.extend(self._start)
        cmd.extend(self._write_byte)
        cmd.append(i2caddress)
        try:
            self._send_check_ack(cmd)
        except I2cNackError:
            self.log.warning('NACK @ 0x%02x', (i2caddress >> 1))
            raise

    def _do_epilog(self) -> None:
        self.log.debug('   epilog')
        cmd = bytearray(self._stop)
        if self._fake_tristate:
            # SCL high-Z, SDA high-Z
            cmd.extend(self._clk_input_data_input)
        self._ftdi.write_data(cmd)
        # be sure to purge the MPSSE reply
        self._ftdi.read_data_bytes(1, 1)

    def _send_check_ack(self, cmd: bytearray):
        # note: cmd is modified
        if self._fake_tristate:
            # SCL low, SDA high-Z (input)
            cmd.extend(self._clk_lo_data_input)
            # read SDA (ack from slave)
            cmd.extend(self._read_bit)
        else:
            # SCL low, SDA high-Z
            cmd.extend(self._clk_lo_data_hi)
            # read SDA (ack from slave)
            cmd.extend(self._read_bit)
        cmd.extend(self._immediate)
        self._i2c_write_data(cmd)
        ack = self._i2c_read_data_bytes(1, 4)
        if not ack:
            raise I2cIOError('No answer from FTDI')
        if ack[0] & self.BIT0:
            raise I2cNackError('NACK from slave')

    def _i2c_write_data(self, cmd: bytearray):
        if self._clkstrch:
            cmd.insert(0, self._ftdi.ENABLE_CLK_ADAPTIVE)
            cmd.append(self._ftdi.DISABLE_CLK_ADAPTIVE)
        self._ftdi.write_data(cmd)

    def _i2c_read_data_bytes(self, readlen: int, attempt: int = 1,
                             request_gen=None) -> bytearray:
        data = self._ftdi.read_data_bytes(readlen, attempt, request_gen)
        if not data and self._clkstrch:
            self.log.warning('bus seems wedged')
            self._ftdi.purge_rx_buffer()
            self._ftdi.write_data(
                bytearray((self._ftdi.DISABLE_CLK_ADAPTIVE,)))
        return data

    def _do_read(self, readlen: int) -> bytearray:
        self.log.debug('- read %d byte(s)', readlen)
        if not readlen:
            # force a real read request on device, but discard any result
            cmd = bytearray()
            cmd.extend(self._immediate)
            self._i2c_write_data(cmd)
            self._i2c_read_data_bytes(0, 4)
            return bytearray()
        if self._fake_tristate:
            read_byte = (self._clk_lo_data_input +
                         self._read_byte +
                         self._clk_lo_data_hi)
            read_not_last = (read_byte + self._ack +
                             self._clk_lo_data_lo * self._ck_delay)
            read_last = (read_byte + self._nack +
                         self._clk_lo_data_hi * self._ck_delay)
        else:
            read_not_last = (self._read_byte + self._ack +
                             self._clk_lo_data_hi * self._ck_delay)
            read_last = (self._read_byte + self._nack +
                         self._clk_lo_data_hi * self._ck_delay)
        # maximum RX size to fit in FTDI FIFO, minus 2 status bytes
        chunk_size = self._rx_size-2
        cmd_size = len(read_last)
        # limit RX chunk size to the count of I2C packable commands in the FTDI
        # TX FIFO (minus one byte for the last 'send immediate' command)
        tx_count = (self._tx_size-1) // cmd_size
        chunk_size = min(tx_count, chunk_size)
        chunks = []
        cmd = None
        rem = readlen
        if self._read_optim and rem > chunk_size:
            chunk_size //= 2
            self.log.debug('Use optimized transfer, %d byte at a time',
                           chunk_size)
            cmd_chunk = bytearray()
            cmd_chunk.extend(read_not_last * chunk_size)
            cmd_chunk.extend(self._immediate)

            def write_command_gen(length: int) -> bytearray:
                if length <= 0:
                    # no more data
                    return bytearray()
                if length <= chunk_size:
                    cmd = bytearray()
                    cmd.extend(read_not_last * (length-1))
                    cmd.extend(read_last)
                    cmd.extend(self._immediate)
                    return cmd
                return cmd_chunk

            while rem:
                buf = self._i2c_read_data_bytes(rem, 4, write_command_gen)
                self.log.debug('- read %d bytes, rem: %d', len(buf), rem)
                chunks.append(buf)
                rem -= len(buf)
        else:
            while rem:
                size = rem
                if rem > chunk_size:
                    if not cmd:
                        # build the command sequence only once, as it may be
                        # repeated till the end of the transfer
                        cmd = bytearray()
                        cmd.extend(read_not_last * chunk_size)
                    size = chunk_size
                else:
                    cmd = bytearray()
                    cmd.extend(read_not_last * (rem-1))
                    cmd.extend(read_last)
                    cmd.extend(self._immediate)
                self._i2c_write_data(cmd)
                buf = self._i2c_read_data_bytes(size, 4)
                self.log.debug('- read %d byte(s): %s',
                               len(buf), hexlify(buf).decode())
                chunks.append(buf)
                rem -= size
        return bytearray(b''.join(chunks))

    def _do_write(self, out: Union[bytes, bytearray, Iterable[int]]):
        if not isinstance(out, bytearray):
            out = bytearray(out)
        if not out:
            return
        self.log.debug('- write %d byte(s): %s',
                       len(out), hexlify(out).decode())
        for byte in out:
            if self._fake_tristate:
                # leave SCL low, restore SDA as output
                cmd = bytearray(self._clk_lo_data_hi)
                cmd.extend(self._write_byte)
            else:
                cmd = bytearray(self._write_byte)
            cmd.append(byte)
            self._send_check_ack(cmd)
