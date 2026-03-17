# Copyright (c) 2010-2024, Emmanuel Blot <emmanuel.blot@free.fr>
# Copyright (c) 2016, Emmanuel Bouaziz <ebouaziz@free.fr>
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

"""SPI support for PyFdti"""

# pylint: disable=invalid-name

from logging import getLogger
from struct import calcsize as scalc, pack as spack, unpack as sunpack
from threading import Lock
from typing import Any, Iterable, Mapping, Optional, Set, Union
from usb.core import Device as UsbDevice
from .ftdi import Ftdi, FtdiError


class SpiIOError(FtdiError):
    """SPI I/O error"""


class SpiPort:
    """SPI port

       An SPI port is never instanciated directly: use
       :py:meth:`SpiController.get_port()` method to obtain an SPI port.

       Example:

       >>> ctrl = SpiController()
       >>> ctrl.configure('ftdi://ftdi:232h/1')
       >>> spi = ctrl.get_port(1)
       >>> spi.set_frequency(1000000)
       >>> # send 2 bytes
       >>> spi.exchange([0x12, 0x34])
       >>> # send 2 bytes, then receive 2 bytes
       >>> out = spi.exchange([0x12, 0x34], 2)
       >>> # send 2 bytes, then receive 4 bytes, manage the transaction
       >>> out = spi.exchange([0x12, 0x34], 2, True, False)
       >>> out.extend(spi.exchange([], 2, False, True))
    """

    def __init__(self, controller: 'SpiController', cs: int, cs_hold: int = 3,
                 spi_mode: int = 0):
        self.log = getLogger('pyftdi.spi.port')
        self._controller = controller
        self._frequency = self._controller.frequency
        self._cs = cs
        self._cs_hold = cs_hold
        self.set_mode(spi_mode)

    def exchange(self, out: Union[bytes, bytearray, Iterable[int]] = b'',
                 readlen: int = 0, start: bool = True, stop: bool = True,
                 duplex: bool = False, droptail: int = 0) -> bytes:
        """Perform an exchange or a transaction with the SPI slave

           :param out: data to send to the SPI slave, may be empty to read out
                       data from the slave with no write.
           :param readlen: count of bytes to read out from the slave,
                       may be zero to only write to the slave
           :param start: whether to start an SPI transaction, i.e.
                        activate the /CS line for the slave. Use False to
                        resume a previously started transaction
           :param stop: whether to desactivete the /CS line for the slave.
                       Use False if the transaction should complete with a
                       further call to exchange()
           :param duplex: perform a full-duplex exchange (vs. half-duplex),
                          i.e. bits are clocked in and out at once.
           :param droptail: ignore up to 7 last bits (for non-byte sized SPI
                               accesses)
           :return: an array of bytes containing the data read out from the
                    slave
        """
        return self._controller.exchange(self._frequency, out, readlen,
                                         start and self._cs_prolog,
                                         stop and self._cs_epilog,
                                         self._cpol, self._cpha,
                                         duplex, droptail)

    def read(self, readlen: int = 0, start: bool = True, stop: bool = True,
             droptail: int = 0) -> bytes:
        """Read out bytes from the slave

           :param readlen: count of bytes to read out from the slave,
                           may be zero to only write to the slave
           :param start: whether to start an SPI transaction, i.e.
                        activate the /CS line for the slave. Use False to
                        resume a previously started transaction
           :param stop: whether to desactivete the /CS line for the slave.
                       Use False if the transaction should complete with a
                       further call to exchange()
           :param droptail: ignore up to 7 last bits (for non-byte sized SPI
                               accesses)
           :return: an array of bytes containing the data read out from the
                    slave
        """
        return self._controller.exchange(self._frequency, [], readlen,
                                         start and self._cs_prolog,
                                         stop and self._cs_epilog,
                                         self._cpol, self._cpha, False,
                                         droptail)

    def write(self, out: Union[bytes, bytearray, Iterable[int]],
              start: bool = True, stop: bool = True, droptail: int = 0) \
            -> None:
        """Write bytes to the slave

           :param out: data to send to the SPI slave, may be empty to read out
                       data from the slave with no write.
           :param start: whether to start an SPI transaction, i.e.
                        activate the /CS line for the slave. Use False to
                        resume a previously started transaction
           :param stop: whether to desactivete the /CS line for the slave.
                        Use False if the transaction should complete with a
                        further call to exchange()
           :param droptail: ignore up to 7 last bits (for non-byte sized SPI
                               accesses)
        """
        return self._controller.exchange(self._frequency, out, 0,
                                         start and self._cs_prolog,
                                         stop and self._cs_epilog,
                                         self._cpol, self._cpha, False,
                                         droptail)

    def flush(self) -> None:
        """Force the flush of the HW FIFOs"""
        self._controller.flush()

    def set_frequency(self, frequency: float):
        """Change SPI bus frequency

           :param float frequency: the new frequency in Hz
        """
        self._frequency = min(frequency, self._controller.frequency_max)

    def set_mode(self, mode: int, cs_hold: Optional[int] = None) -> None:
        """Set or change the SPI mode to communicate with the SPI slave.

           :param mode: new SPI mode
           :param cs_hold: change the /CS hold duration (or keep using previous
                           value)
        """
        if not 0 <= mode <= 3:
            raise SpiIOError(f'Invalid SPI mode: {mode}')
        if (mode & 0x2) and not self._controller.is_inverted_cpha_supported:
            raise SpiIOError('SPI with CPHA high is not supported by '
                             'this FTDI device')
        if cs_hold is None:
            cs_hold = self._cs_hold
        else:
            self._cs_hold = cs_hold
        self._cpol = bool(mode & 0x2)
        self._cpha = bool(mode & 0x1)
        cs_clock = 0xFF & ~((int(not self._cpol) and SpiController.SCK_BIT) |
                            SpiController.DO_BIT)
        cs_select = 0xFF & ~((SpiController.CS_BIT << self._cs) |
                             (int(not self._cpol) and SpiController.SCK_BIT) |
                             SpiController.DO_BIT)
        self._cs_prolog = bytes([cs_clock, cs_select])
        self._cs_epilog = bytes([cs_select] + [cs_clock] * int(cs_hold))

    def force_select(self, level: Optional[bool] = None,
                     cs_hold: float = 0) -> None:
        """Force-drive /CS signal.

           This API is not designed for a regular usage, but is reserved to
           very specific slave devices that require non-standard SPI
           signalling. There are very few use cases where this API is required.

           :param level: level to force on /CS output. This is a tri-state
                         value. A boolean value forces the selected signal
                         level; note that SpiPort no longer enforces that
                         following API calls generates valid SPI signalling:
                         use with extreme care. `None` triggers a pulse on /CS
                         output, i.e. /CS is not asserted once the method
                         returns, whatever the actual /CS level when this API
                         is called.
           :param cs_hold: /CS hold duration, as a unitless value. It is not
                         possible to control the exact duration of the pulse,
                         as it depends on the USB bus and the FTDI frequency.
        """
        clk, sel = self._cs_prolog
        if cs_hold:
            hold = max(1, cs_hold)
            if hold > SpiController.PAYLOAD_MAX_LENGTH:
                raise ValueError('cs_hold is too long')
        else:
            hold = self._cs_hold
        if level is None:
            seq = bytearray([clk])
            seq.extend([sel]*(1+hold))
            seq.extend([clk]*self._cs_hold)
        elif level:
            seq = bytearray([clk] * hold)
        else:
            seq = bytearray([clk] * hold)
            seq.extend([sel]*(1+hold))
        self._controller.force_control(self._frequency, bytes(seq))

    @property
    def frequency(self) -> float:
        """Return the current SPI bus block"""
        return self._frequency

    @property
    def cs(self) -> int:
        """Return the /CS index.

          :return: the /CS index (starting from 0)
        """
        return self._cs

    @property
    def mode(self) -> int:
        """Return the current SPI mode.

           :return: the SPI mode
        """
        return (int(self._cpol) << 2) | int(self._cpha)


class SpiGpioPort:
    """GPIO port

       A SpiGpioPort instance enables to drive GPIOs wich are not reserved for
       SPI feature as regular GPIOs.

       GPIO are managed as a bitfield. The LSBs are reserved for the SPI
       feature, which means that the lowest pin that can be used as a GPIO is
       *b4*:

       * *b0*: SPI SCLK
       * *b1*: SPI MOSI
       * *b2*: SPI MISO
       * *b3*: SPI CS0
       * *b4*: SPI CS1 or first GPIO

       If more than one SPI device is used, less GPIO pins are available, see
       the cs_count argument of the SpiController constructor.

       There is no offset bias in GPIO bit position, *i.e.* the first available
       GPIO can be reached from as ``0x10``.

       Bitfield size depends on the FTDI device: 4432H series use 8-bit GPIO
       ports, while 232H and 2232H series use wide 16-bit ports.

       An SpiGpio port is never instanciated directly: use
       :py:meth:`SpiController.get_gpio()` method to obtain the GPIO port.
    """
    def __init__(self, controller: 'SpiController'):
        self.log = getLogger('pyftdi.spi.gpio')
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
           a reserved pin (for SPI support)

           :return: the bitfield of configurable GPIO pins.
        """
        return self._controller.gpio_all_pins

    @property
    def width(self) -> int:
        """Report the FTDI count of addressable pins.

           Note that all pins, including reserved SPI ones, are reported.

           :return: the count of IO pins (including SPI ones).
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


class SpiController:
    """SPI master.

        :param int cs_count: is the number of /CS lines (one per device to
            drive on the SPI bus)
        :param turbo: increase throughput over USB bus, but may not be
                      supported with some specific slaves
    """

    SCK_BIT = 0x01
    DO_BIT = 0x02
    DI_BIT = 0x04
    CS_BIT = 0x08
    SPI_BITS = DI_BIT | DO_BIT | SCK_BIT
    PAYLOAD_MAX_LENGTH = 0xFF00  # 16 bits max (- spare for control)

    def __init__(self, cs_count: int = 1, turbo: bool = True):
        self.log = getLogger('pyftdi.spi.ctrl')
        self._ftdi = Ftdi()
        self._lock = Lock()
        self._gpio_port = None
        self._gpio_dir = 0
        self._gpio_mask = 0
        self._gpio_low = 0
        self._wide_port = False
        self._cs_count = cs_count
        self._turbo = turbo
        self._immediate = bytes((Ftdi.SEND_IMMEDIATE,))
        self._frequency = 0.0
        self._clock_phase = False
        self._cs_bits = 0
        self._spi_ports = []
        self._spi_dir = 0
        self._spi_mask = self.SPI_BITS

    def configure(self, url: Union[str, UsbDevice],
                  **kwargs: Mapping[str, Any]) -> None:
        """Configure the FTDI interface as a SPI master

           :param url: FTDI URL string, such as ``ftdi://ftdi:232h/1``
           :param kwargs: options to configure the SPI bus

           Accepted options:

           * ``interface``: when URL is specifed as a USB device, the interface
             named argument can be used to select a specific port of the FTDI
             device, as an integer starting from 1.
           * ``direction`` a bitfield specifying the FTDI GPIO direction,
             where high level defines an output, and low level defines an
             input. Only useful to setup default IOs at start up, use
             :py:class:`SpiGpioPort` to drive GPIOs. Note that pins reserved
             for SPI feature take precedence over any this setting.
           * ``initial`` a bitfield specifying the initial output value. Only
             useful to setup default IOs at start up, use
             :py:class:`SpiGpioPort` to drive GPIOs.
           * ``frequency`` the SPI bus frequency in Hz. Note that each slave
             may reconfigure the SPI bus with a specialized
             frequency.
           * ``cs_count`` count of chip select signals dedicated to select
             SPI slave devices, starting from A*BUS3 pin
           * ``turbo`` whether to enable or disable turbo mode
           * ``debug`` to increase log verbosity, using MPSSE tracer
        """
        # it is better to specify CS and turbo in configure, but the older
        # API where these parameters are specified at instanciation has been
        # preserved
        if 'cs_count' in kwargs:
            self._cs_count = int(kwargs['cs_count'])
            del kwargs['cs_count']
        if not 1 <= self._cs_count <= 5:
            raise ValueError(f'Unsupported CS line count: {self._cs_count}')
        if 'turbo' in kwargs:
            self._turbo = bool(kwargs['turbo'])
            del kwargs['turbo']
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
                raise SpiIOError('url and interface are mutually exclusive')
            interface = int(kwargs['interface'])
            del kwargs['interface']
        else:
            interface = 1
        with self._lock:
            if self._frequency > 0.0:
                raise SpiIOError('Already configured')
            self._cs_bits = (((SpiController.CS_BIT << self._cs_count) - 1) &
                             ~(SpiController.CS_BIT - 1))
            self._spi_ports = [None] * self._cs_count
            self._spi_dir = (self._cs_bits |
                             SpiController.DO_BIT |
                             SpiController.SCK_BIT)
            self._spi_mask = self._cs_bits | self.SPI_BITS
            # until the device is open, there is no way to tell if it has a
            # wide (16) or narrow port (8). Lower API can deal with any, so
            # delay any truncation till the device is actually open
            self._set_gpio_direction(16, (~self._spi_mask) & 0xFFFF, io_dir)
            kwargs['direction'] = self._spi_dir | self._gpio_dir
            kwargs['initial'] = self._cs_bits | (io_out & self._gpio_mask)
            if not isinstance(url, str):
                self._frequency = self._ftdi.open_mpsse_from_device(
                    url, interface=interface, **kwargs)
            else:
                self._frequency = self._ftdi.open_mpsse_from_url(url, **kwargs)
            self._ftdi.enable_adaptive_clock(False)
            self._wide_port = self._ftdi.has_wide_port
            if not self._wide_port:
                self._set_gpio_direction(8, io_out & 0xFF, io_dir & 0xFF)

    def close(self, freeze: bool = False) -> None:
        """Close the FTDI interface.

           :param freeze: if set, FTDI port is not reset to its default
                          state on close.
        """
        with self._lock:
            if self._ftdi.is_connected:
                self._ftdi.close(freeze)
        self._frequency = 0.0

    def terminate(self) -> None:
        """Close the FTDI interface.

           :note: deprecated API, use close()
        """
        self.close()

    def get_port(self, cs: int, freq: Optional[float] = None,
                 mode: int = 0) -> SpiPort:
        """Obtain a SPI port to drive a SPI device selected by Chip Select.

           :note: SPI mode 1 and 3 are not officially supported.

           :param cs: chip select slot, starting from 0
           :param freq: SPI bus frequency for this slave in Hz
           :param mode: SPI mode [0, 1, 2, 3]
        """
        with self._lock:
            if not self._ftdi.is_connected:
                raise SpiIOError("FTDI controller not initialized")
            if cs >= len(self._spi_ports):
                if cs < 5:
                    # increase cs_count (up to 4) to reserve more /CS channels
                    raise SpiIOError('/CS pin {cs} not reserved for SPI')
                raise SpiIOError(f'No such SPI port: {cs}')
            if not self._spi_ports[cs]:
                freq = min(freq or self._frequency, self.frequency_max)
                hold = freq and (1+int(1E6/freq))
                self._spi_ports[cs] = SpiPort(self, cs, cs_hold=hold,
                                              spi_mode=mode)
                self._spi_ports[cs].set_frequency(freq)
                self._flush()
            return self._spi_ports[cs]

    def get_gpio(self) -> SpiGpioPort:
        """Retrieve the GPIO port.

           :return: GPIO port
        """
        with self._lock:
            if not self._ftdi.is_connected:
                raise SpiIOError("FTDI controller not initialized")
            if not self._gpio_port:
                self._gpio_port = SpiGpioPort(self)
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
        return self._ftdi.is_connected

    @property
    def frequency_max(self) -> float:
        """Provides the maximum SPI clock frequency in Hz.

           :return: SPI bus clock frequency
        """
        return self._ftdi.frequency_max

    @property
    def frequency(self) -> float:
        """Provides the current SPI clock frequency in Hz.

           :return: the SPI bus clock frequency
        """
        return self._frequency

    @property
    def direction(self):
        """Provide the FTDI pin direction

           A true bit represents an output pin, a false bit an input pin.

           :return: the bitfield of direction.
        """
        return self._spi_dir | self._gpio_dir

    @property
    def channels(self) -> int:
        """Provide the maximum count of slaves.


           :return: the count of pins reserved to drive the /CS signal
        """
        return self._cs_count

    @property
    def active_channels(self) -> Set[int]:
        """Provide the set of configured slaves /CS.

           :return: Set of /CS, one for each configured slaves
        """
        return {port[0] for port in enumerate(self._spi_ports) if port[1]}

    @property
    def gpio_pins(self):
        """Report the configured GPIOs as a bitfield.

           A true bit represents a GPIO, a false bit a reserved or not
           configured pin.

           :return: the bitfield of configured GPIO pins.
        """
        with self._lock:
            return self._gpio_mask

    @property
    def gpio_all_pins(self):
        """Report the addressable GPIOs as a bitfield.

           A true bit represents a pin which may be used as a GPIO, a false bit
           a reserved pin (for SPI support)

           :return: the bitfield of configurable GPIO pins.
        """
        mask = (1 << self.width) - 1
        with self._lock:
            return mask & ~self._spi_mask

    @property
    def width(self):
        """Report the FTDI count of addressable pins.

           :return: the count of IO pins (including SPI ones).
        """
        return 16 if self._wide_port else 8

    @property
    def is_inverted_cpha_supported(self) -> bool:
        """Report whether it is possible to supported CPHA=1.

           :return: inverted CPHA supported (with a kludge)
        """
        return self._ftdi.is_H_series

    def exchange(self, frequency: float,
                 out: Union[bytes, bytearray, Iterable[int]], readlen: int,
                 cs_prolog: Optional[bytes] = None,
                 cs_epilog: Optional[bytes] = None,
                 cpol: bool = False, cpha: bool = False,
                 duplex: bool = False, droptail: int = 0) -> bytes:
        """Perform an exchange or a transaction with the SPI slave

           :param out: data to send to the SPI slave, may be empty to read out
                       data from the slave with no write.
           :param readlen: count of bytes to read out from the slave,
                           may be zero to only write to the slave,
           :param cs_prolog: the prolog MPSSE command sequence to execute
                             before the actual exchange.
           :param cs_epilog: the epilog MPSSE command sequence to execute
                             after the actual exchange.
           :param cpol: SPI clock polarity, derived from the SPI mode
           :param cpol: SPI clock phase, derived from the SPI mode
           :param duplex: perform a full-duplex exchange (vs. half-duplex),
                          i.e. bits are clocked in and out at once or
                          in a write-then-read manner.
           :param droptail: ignore up to 7 last bits (for non-byte sized SPI
                             accesses)
           :return: bytes containing the data read out from the slave, if any
        """
        if not 0 <= droptail <= 7:
            raise ValueError('Invalid skip bit count')
        if duplex:
            if readlen > len(out):
                tmp = bytearray(out)
                tmp.extend([0] * (readlen - len(out)))
                out = tmp
            elif not readlen:
                readlen = len(out)
        with self._lock:
            if duplex:
                data = self._exchange_full_duplex(frequency, out,
                                                  cs_prolog, cs_epilog,
                                                  cpol, cpha, droptail)
                return data[:readlen]
            return self._exchange_half_duplex(frequency, out, readlen,
                                              cs_prolog, cs_epilog,
                                              cpol, cpha, droptail)

    def force_control(self, frequency: float, sequence: bytes) -> None:
        """Execution an arbitrary SPI control bit sequence.
           Use with extreme care, as it may lead to unexpected results. Regular
           usage of SPI does not require to invoke this API.

           :param sequence: the bit sequence to execute.
        """
        with self._lock:
            self._force(frequency, sequence)

    def flush(self) -> None:
        """Flush the HW FIFOs.
        """
        with self._lock:
            self._flush()

    def read_gpio(self, with_output: bool = False) -> int:
        """Read GPIO port

           :param  with_output: set to unmask output pins
           :return: the GPIO port pins as a bitfield
        """
        with self._lock:
            data = self._read_raw(self._wide_port)
        value = data & self._gpio_mask
        if not with_output:
            value &= ~self._gpio_dir
        return value

    def write_gpio(self, value: int) -> None:
        """Write GPIO port

           :param value: the GPIO port pins as a bitfield
        """
        with self._lock:
            if (value & self._gpio_dir) != value:
                raise SpiIOError(f'No such GPO pins: '
                                 f'{self._gpio_dir:04x}/{value:04x}')
            # perform read-modify-write
            use_high = self._wide_port and (self.direction & 0xff00)
            data = self._read_raw(use_high)
            data &= ~self._gpio_mask
            data |= value
            self._write_raw(data, use_high)
            self._gpio_low = data & 0xFF & ~self._spi_mask

    def set_gpio_direction(self, pins: int, direction: int) -> None:
        """Change the direction of the GPIO pins

           :param pins: which GPIO pins should be reconfigured
           :param direction: direction bitfield (on for output)
        """
        with self._lock:
            self._set_gpio_direction(16 if self._wide_port else 8,
                                     pins, direction)

    def _set_gpio_direction(self, width: int, pins: int,
                            direction: int) -> None:
        if pins & self._spi_mask:
            raise SpiIOError('Cannot access SPI pins as GPIO')
        gpio_mask = (1 << width) - 1
        gpio_mask &= ~self._spi_mask
        if (pins & gpio_mask) != pins:
            raise SpiIOError('No such GPIO pin(s)')
        self._gpio_dir &= ~pins
        self._gpio_dir |= (pins & direction)
        self._gpio_mask = gpio_mask & pins

    def _read_raw(self, read_high: bool) -> int:
        if not self._ftdi.is_connected:
            raise SpiIOError("FTDI controller not initialized")
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
            raise SpiIOError('Cannot read GPIO')
        value, = sunpack(fmt, data)
        return value

    def _write_raw(self, data: int, write_high: bool) -> None:
        if not self._ftdi.is_connected:
            raise SpiIOError("FTDI controller not initialized")
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

    def _force(self, frequency: float, sequence: bytes):
        if not self._ftdi.is_connected:
            raise SpiIOError("FTDI controller not initialized")
        if len(sequence) > SpiController.PAYLOAD_MAX_LENGTH:
            raise SpiIOError("Output payload is too large")
        if self._frequency != frequency:
            self._ftdi.set_frequency(frequency)
            # store the requested value, not the actual one (best effort),
            # to avoid setting unavailable values on each call.
            self._frequency = frequency
        cmd = bytearray()
        direction = self.direction & 0xFF
        for ctrl in sequence:
            ctrl &= self._spi_mask
            ctrl |= self._gpio_low
            cmd.extend((Ftdi.SET_BITS_LOW, ctrl, direction))
        self._ftdi.write_data(cmd)

    def _exchange_half_duplex(self, frequency: float,
                              out: Union[bytes, bytearray, Iterable[int]],
                              readlen: int, cs_prolog: bytes, cs_epilog: bytes,
                              cpol: bool, cpha: bool,
                              droptail: int) -> bytes:
        if not self._ftdi.is_connected:
            raise SpiIOError("FTDI controller not initialized")
        if len(out) > SpiController.PAYLOAD_MAX_LENGTH:
            raise SpiIOError("Output payload is too large")
        if readlen > SpiController.PAYLOAD_MAX_LENGTH:
            raise SpiIOError("Input payload is too large")
        if cpha:
            # to enable CPHA, we need to use a workaround with FTDI device,
            # that is enable 3-phase clocking (which is usually dedicated to
            # I2C support). This mode use use 3 clock period instead of 2,
            # which implies the FTDI frequency should be fixed to match the
            # requested one.
            frequency = (3*frequency)//2
        if self._frequency != frequency:
            self._ftdi.set_frequency(frequency)
            # store the requested value, not the actual one (best effort),
            # to avoid setting unavailable values on each call.
            self._frequency = frequency
        direction = self.direction & 0xFF  # low bits only
        cmd = bytearray()
        for ctrl in cs_prolog or []:
            ctrl &= self._spi_mask
            ctrl |= self._gpio_low
            cmd.extend((Ftdi.SET_BITS_LOW, ctrl, direction))
        epilog = bytearray()
        if cs_epilog:
            for ctrl in cs_epilog:
                ctrl &= self._spi_mask
                ctrl |= self._gpio_low
                epilog.extend((Ftdi.SET_BITS_LOW, ctrl, direction))
            # Restore idle state
            cs_high = [Ftdi.SET_BITS_LOW, self._cs_bits | self._gpio_low,
                       direction]
            if not self._turbo:
                cs_high.append(Ftdi.SEND_IMMEDIATE)
            epilog.extend(cs_high)
        writelen = len(out)
        if self._clock_phase != cpha:
            self._ftdi.enable_3phase_clock(cpha)
            self._clock_phase = cpha
        if writelen:
            if not droptail:
                wcmd = (Ftdi.WRITE_BYTES_NVE_MSB if not cpol else
                        Ftdi.WRITE_BYTES_PVE_MSB)
                write_cmd = spack('<BH', wcmd, writelen-1)
                cmd.extend(write_cmd)
                cmd.extend(out)
            else:
                bytelen = writelen-1
                if bytelen:
                    wcmd = (Ftdi.WRITE_BYTES_NVE_MSB if not cpol else
                            Ftdi.WRITE_BYTES_PVE_MSB)
                    write_cmd = spack('<BH', wcmd, bytelen-1)
                    cmd.extend(write_cmd)
                    cmd.extend(out[:-1])
                wcmd = (Ftdi.WRITE_BITS_NVE_MSB if not cpol else
                        Ftdi.WRITE_BITS_PVE_MSB)
                write_cmd = spack('<BBB', wcmd, 7-droptail, out[-1])
                cmd.extend(write_cmd)
        if readlen:
            if not droptail:
                rcmd = (Ftdi.READ_BYTES_NVE_MSB if not cpol else
                        Ftdi.READ_BYTES_PVE_MSB)
                read_cmd = spack('<BH', rcmd, readlen-1)
                cmd.extend(read_cmd)
            else:
                bytelen = readlen-1
                if bytelen:
                    rcmd = (Ftdi.READ_BYTES_NVE_MSB if not cpol else
                            Ftdi.READ_BYTES_PVE_MSB)
                    read_cmd = spack('<BH', rcmd, bytelen-1)
                    cmd.extend(read_cmd)
                rcmd = (Ftdi.READ_BITS_NVE_MSB if not cpol else
                        Ftdi.READ_BITS_PVE_MSB)
                read_cmd = spack('<BB', rcmd, 7-droptail)
                cmd.extend(read_cmd)
            cmd.extend(self._immediate)
            if self._turbo:
                if epilog:
                    cmd.extend(epilog)
                self._ftdi.write_data(cmd)
            else:
                self._ftdi.write_data(cmd)
                if epilog:
                    self._ftdi.write_data(epilog)
            # USB read cycle may occur before the FTDI device has actually
            # sent the data, so try to read more than once if no data is
            # actually received
            data = self._ftdi.read_data_bytes(readlen, 4)
            if droptail:
                data[-1] = 0xff & (data[-1] << droptail)
        else:
            if writelen:
                if self._turbo:
                    if epilog:
                        cmd.extend(epilog)
                    self._ftdi.write_data(cmd)
                else:
                    self._ftdi.write_data(cmd)
                    if epilog:
                        self._ftdi.write_data(epilog)
            data = bytearray()
        return data

    def _exchange_full_duplex(self, frequency: float,
                              out: Union[bytes, bytearray, Iterable[int]],
                              cs_prolog: bytes, cs_epilog: bytes,
                              cpol: bool, cpha: bool,
                              droptail: int) -> bytes:
        if not self._ftdi.is_connected:
            raise SpiIOError("FTDI controller not initialized")
        if len(out) > SpiController.PAYLOAD_MAX_LENGTH:
            raise SpiIOError("Output payload is too large")
        if cpha:
            # to enable CPHA, we need to use a workaround with FTDI device,
            # that is enable 3-phase clocking (which is usually dedicated to
            # I2C support). This mode use use 3 clock period instead of 2,
            # which implies the FTDI frequency should be fixed to match the
            # requested one.
            frequency = (3*frequency)//2
        if self._frequency != frequency:
            self._ftdi.set_frequency(frequency)
            # store the requested value, not the actual one (best effort),
            # to avoid setting unavailable values on each call.
            self._frequency = frequency
        direction = self.direction & 0xFF  # low bits only
        cmd = bytearray()
        for ctrl in cs_prolog or []:
            ctrl &= self._spi_mask
            ctrl |= self._gpio_low
            cmd.extend((Ftdi.SET_BITS_LOW, ctrl, direction))
        epilog = bytearray()
        if cs_epilog:
            for ctrl in cs_epilog:
                ctrl &= self._spi_mask
                ctrl |= self._gpio_low
                epilog.extend((Ftdi.SET_BITS_LOW, ctrl, direction))
            # Restore idle state
            cs_high = [Ftdi.SET_BITS_LOW, self._cs_bits | self._gpio_low,
                       direction]
            if not self._turbo:
                cs_high.append(Ftdi.SEND_IMMEDIATE)
            epilog.extend(cs_high)
        exlen = len(out)
        if self._clock_phase != cpha:
            self._ftdi.enable_3phase_clock(cpha)
            self._clock_phase = cpha
        if not droptail:
            wcmd = (Ftdi.RW_BYTES_PVE_NVE_MSB if not cpol else
                    Ftdi.RW_BYTES_NVE_PVE_MSB)
            write_cmd = spack('<BH', wcmd, exlen-1)
            cmd.extend(write_cmd)
            cmd.extend(out)
        else:
            bytelen = exlen-1
            if bytelen:
                wcmd = (Ftdi.RW_BYTES_PVE_NVE_MSB if not cpol else
                        Ftdi.RW_BYTES_NVE_PVE_MSB)
                write_cmd = spack('<BH', wcmd, bytelen-1)
                cmd.extend(write_cmd)
                cmd.extend(out[:-1])
            wcmd = (Ftdi.RW_BITS_PVE_NVE_MSB if not cpol else
                    Ftdi.RW_BITS_NVE_PVE_MSB)
            write_cmd = spack('<BBB', wcmd, 7-droptail, out[-1])
            cmd.extend(write_cmd)
        cmd.extend(self._immediate)
        if self._turbo:
            if epilog:
                cmd.extend(epilog)
            self._ftdi.write_data(cmd)
        else:
            self._ftdi.write_data(cmd)
            if epilog:
                self._ftdi.write_data(epilog)
        # USB read cycle may occur before the FTDI device has actually
        # sent the data, so try to read more than once if no data is
        # actually received
        data = self._ftdi.read_data_bytes(exlen, 4)
        if droptail:
            data[-1] = 0xff & (data[-1] << droptail)
        return data

    def _flush(self) -> None:
        self._ftdi.write_data(self._immediate)
        self._ftdi.purge_buffers()
