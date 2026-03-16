# Copyright (c) 2014-2024, Emmanuel Blot <emmanuel.blot@free.fr>
# Copyright (c) 2016, Emmanuel Bouaziz <ebouaziz@free.fr>
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

"""GPIO/BitBang support for PyFdti"""


from struct import calcsize as scalc, unpack as sunpack
from typing import Iterable, Optional, Tuple, Union
from .ftdi import Ftdi, FtdiError
from .misc import is_iterable


class GpioException(FtdiError):
    """Base class for GPIO errors.
    """


class GpioPort:
    """Duck-type GPIO port for GPIO all controllers.
    """


class GpioBaseController(GpioPort):
    """GPIO controller for an FTDI port, in bit-bang legacy mode.

       GPIO bit-bang mode is limited to the 8 lower pins of each GPIO port.
    """

    def __init__(self):
        self._ftdi = Ftdi()
        self._direction = 0
        self._width = 0
        self._mask = 0
        self._frequency = 0

    @property
    def ftdi(self) -> Ftdi:
        """Return the Ftdi instance.

           :return: the Ftdi instance
        """
        return self._ftdi

    @property
    def is_connected(self) -> bool:
        """Reports whether a connection exists with the FTDI interface.

           :return: the FTDI slave connection status
        """
        return self._ftdi.is_connected

    def configure(self, url: str, direction: int = 0,
                  **kwargs) -> int:
        """Open a new interface to the specified FTDI device in bitbang mode.

           :param str url: a FTDI URL selector
           :param int direction: a bitfield specifying the FTDI GPIO direction,
                where high level defines an output, and low level defines an
                input
           :param initial: optional initial GPIO output value
           :param pace: optional pace in GPIO sample per second
           :return: actual bitbang pace in sample per second
        """
        if self.is_connected:
            raise FtdiError('Already connected')
        kwargs = dict(kwargs)
        frequency = kwargs.get('frequency', None)
        if frequency is None:
            frequency = kwargs.get('baudrate', None)
        for k in ('direction', 'sync', 'frequency', 'baudrate'):
            if k in kwargs:
                del kwargs[k]
        self._frequency = self._configure(url, direction, frequency, **kwargs)

    def close(self, freeze: bool = False) -> None:
        """Close the GPIO port.

           :param freeze: if set, FTDI port is not reset to its default
                          state on close. This means the port is left with
                          its current configuration and output signals.
                          This feature should not be used except for very
                          specific needs.
        """
        if self._ftdi.is_connected:
            self._ftdi.close(freeze)

    def get_gpio(self) -> GpioPort:
        """Retrieve the GPIO port.

           This method is mostly useless, it is a wrapper to duck type other
           GPIO APIs (I2C, SPI, ...)

           :return: GPIO port
        """
        return self

    @property
    def direction(self) -> int:
        """Reports the GPIO direction.

          :return: a bitfield specifying the FTDI GPIO direction, where high
                level reports an output pin, and low level reports an input pin
        """
        return self._direction

    @property
    def pins(self) -> int:
        """Report the configured GPIOs as a bitfield.

           A true bit represents a GPIO, a false bit a reserved or not
           configured pin.

           :return: always 0xFF for GpioController instance.
        """
        return self._mask

    @property
    def all_pins(self) -> int:
        """Report the addressable GPIOs as a bitfield.

           A true bit represents a pin which may be used as a GPIO, a false bit
           a reserved pin

           :return: always 0xFF for GpioController instance.
        """
        return self._mask

    @property
    def width(self) -> int:
        """Report the FTDI count of addressable pins.

           :return: the width of the GPIO port.
        """
        return self._width

    @property
    def frequency(self) -> float:
        """Return the pace at which sequence of GPIO samples are read
           and written.
        """
        return self._frequency

    def set_frequency(self, frequency: Union[int, float]) -> None:
        """Set the frequency at which sequence of GPIO samples are read
           and written.

           :param frequency: the new frequency, in GPIO samples per second
        """
        raise NotImplementedError('GpioBaseController cannot be instanciated')

    def set_direction(self, pins: int, direction: int) -> None:
        """Update the GPIO pin direction.

           :param pins: which GPIO pins should be reconfigured
           :param direction: a bitfield of GPIO pins. Each bit represent a
                GPIO pin, where a high level sets the pin as output and a low
                level sets the pin as input/high-Z.
        """
        if direction > self._mask:
            raise GpioException("Invalid direction mask")
        self._direction &= ~pins
        self._direction |= (pins & direction)
        self._update_direction()

    def _configure(self, url: str, direction: int,
                   frequency: Union[int, float, None] = None, **kwargs) -> int:
        raise NotImplementedError('GpioBaseController cannot be instanciated')

    def _update_direction(self) -> None:
        raise NotImplementedError('Missing implementation')


class GpioAsyncController(GpioBaseController):
    """GPIO controller for an FTDI port, in bit-bang asynchronous mode.

       GPIO accessible pins are limited to the 8 lower pins of each GPIO port.

       Asynchronous bitbang output are updated on write request using the
       :py:meth:`write` method, clocked at the selected frequency.

       Asynchronous bitbang input are sampled at the same rate, as soon as the
       controller is initialized. The GPIO input samples fill in the FTDI HW
       buffer until it is filled up, in which case sampling stops until the
       GPIO samples are read out with the :py:meth:`read` method. It may be
       therefore hard to use, except if peek mode is selected,
       see :py:meth:`read` for details.

       Note that FTDI internal clock divider cannot generate any arbitrary
       frequency, so the closest frequency to the request one that can be
       generated is selected. The actual :py:attr:`frequency` may be tested to
       check if it matches the board requirements.
    """

    def read(self, readlen: int = 1, peek: Optional[bool] = None,
             noflush: bool = False) -> Union[int, bytes]:
        """Read the GPIO input pin electrical level.

           :param readlen: how many GPIO samples to retrieve. Each sample is
                           8-bit wide.
           :param peek: whether to peek/sample the instantaneous GPIO pin
                        values from port, or to use the HW FIFO. The HW FIFO is
                        continously filled up with GPIO sample at the current
                        frequency, until it is full - samples are no longer
                        collected until the FIFO is read. This means than
                        non-peek mode read "old" values, with no way to know at
                        which time they have been sampled. PyFtdi ensures that
                        old sampled values before the completion of a previous
                        GPIO write are discarded. When peek mode is selected,
                        readlen should be 1.
           :param noflush: whether to disable the RX buffer flush before
                           reading out data
           :return: a 8-bit wide integer if peek mode is used, or
                    a bytes buffer otherwise.
        """
        if not self.is_connected:
            raise GpioException('Not connected')
        if peek is None and readlen == 1:
            # compatibility with legacy API
            peek = True
        if peek:
            if readlen != 1:
                raise ValueError('Invalid read length with peek mode')
            return self._ftdi.read_pins()
        # in asynchronous bitbang mode, the FTDI-to-host FIFO is filled in
        # continuously once this mode is activated. This means there is no
        # way to trigger the exact moment where the buffer is filled in, nor
        # to define the write pointer in the buffer. Reading out this buffer
        # at any time is likely to contain a mix of old and new values.
        # Anyway, flushing the FTDI-to-host buffer seems to be a proper
        # to get in sync with the buffer.
        if noflush:
            return self._ftdi.read_data(readlen)
        loop = 10000
        while loop:
            loop -= 1
            # do not attempt to do anything till the FTDI HW buffer has been
            # emptied, i.e. previous write calls have been handled.
            status = self._ftdi.poll_modem_status()
            if status & Ftdi.MODEM_TEMT:
                # TX buffer is now empty, any "write" GPIO rquest has completed
                # so start reading GPIO samples from this very moment.
                break
        else:
            # sanity check to avoid endless loop on errors
            raise FtdiError('FTDI TX buffer error')
        # now flush the FTDI-to-host buffer as it keeps being filled with data
        self._ftdi.purge_tx_buffer()
        # finally perform the actual read out
        return self._ftdi.read_data(readlen)

    def write(self, out: Union[bytes, bytearray, int]) -> None:
        """Set the GPIO output pin electrical level, or output a sequence of
           bytes @ constant frequency to GPIO output pins.

           :param out: a bitfield of GPIO pins, or a sequence of them
        """
        if not self.is_connected:
            raise GpioException('Not connected')
        if isinstance(out, (bytes, bytearray)):
            pass
        else:
            if isinstance(out, int):
                out = bytes([out])
            else:
                if not is_iterable(out):
                    raise TypeError('Invalid output value')
            for val in out:
                if val > self._mask:
                    raise ValueError('Invalid output value')
            out = bytes(out)
        self._ftdi.write_data(out)

    def set_frequency(self, frequency: Union[int, float]) -> None:
        """Set the frequency at which sequence of GPIO samples are read
           and written.

           note: FTDI may update its clock register before it has emptied its
           internal buffer. If the current frequency is "low", some
           yet-to-output bytes may end up being clocked at the new frequency.

           Unfortunately, it seems there is no way to wait for the internal
           buffer to be emptied out. They can be flushed (i.e. discarded), but
           not synchronized :-(

           PyFtdi client should add "some" short delay to ensure a previous,
           long write request has been fully output @ low freq before changing
           the frequency.

           Beware that only some exact frequencies can be generated. Contrary
           to the UART mode, an approximate frequency is always accepted for
           GPIO/bitbang mode. To get the actual frequency, and optionally abort
           if it is out-of-spec, use :py:meth:`frequency` property.

           :param frequency: the new frequency, in GPIO samples per second
        """
        self._frequency = float(self._ftdi.set_baudrate(int(frequency), False))

    def _configure(self, url: str, direction: int,
                   frequency: Union[int, float, None] = None, **kwargs) -> int:
        if 'initial' in kwargs:
            initial = kwargs['initial']
            del kwargs['initial']
        else:
            initial = None
        if 'debug' in kwargs:
            # debug is not implemented
            del kwargs['debug']
        baudrate = int(frequency) if frequency is not None else None
        baudrate = self._ftdi.open_bitbang_from_url(url,
                                                    direction=direction,
                                                    sync=False,
                                                    baudrate=baudrate,
                                                    **kwargs)
        self._width = 8
        self._mask = (1 << self._width) - 1
        self._direction = direction & self._mask
        if initial is not None:
            initial &= self._mask
            self.write(initial)
        return float(baudrate)

    def _update_direction(self) -> None:
        self._ftdi.set_bitmode(self._direction, Ftdi.BitMode.BITBANG)

    # old API names
    open_from_url = GpioBaseController.configure
    read_port = read
    write_port = write


# old API compatibility
GpioController = GpioAsyncController


class GpioSyncController(GpioBaseController):
    """GPIO controller for an FTDI port, in bit-bang synchronous mode.

       GPIO accessible pins are limited to the 8 lower pins of each GPIO port.

       Synchronous bitbang input and output are synchronized. Eveery time GPIO
       output is updated, the GPIO input is sampled and buffered.

       Update and sampling are clocked at the selected frequency. The GPIO
       samples are transfer in both direction with the :py:meth:`exchange`
       method, which therefore always returns as many input samples as output
       bytes.

       Note that FTDI internal clock divider cannot generate any arbitrary
       frequency, so the closest frequency to the request one that can be
       generated is selected. The actual :py:attr:`frequency` may be tested to
       check if it matches the board requirements.
    """

    def exchange(self, out: Union[bytes, bytearray]) -> bytes:
        """Set the GPIO output pin electrical level, or output a sequence of
           bytes @ constant frequency to GPIO output pins.

           :param out: the byte buffer to output as GPIO
           :return: a byte buffer of the same length as out buffer.
        """
        if not self.is_connected:
            raise GpioException('Not connected')
        if isinstance(out, (bytes, bytearray)):
            pass
        else:
            if isinstance(out, int):
                out = bytes([out])
            elif not is_iterable(out):
                raise TypeError('Invalid output value')
            for val in out:
                if val > self._mask:
                    raise GpioException("Invalid value")
        self._ftdi.write_data(out)
        data = self._ftdi.read_data_bytes(len(out), 4)
        return data

    def set_frequency(self, frequency: Union[int, float]) -> None:
        """Set the frequency at which sequence of GPIO samples are read
           and written.

           :param frequency: the new frequency, in GPIO samples per second
        """
        self._frequency = float(self._ftdi.set_baudrate(int(frequency), False))

    def _configure(self, url: str, direction: int,
                   frequency: Union[int, float, None] = None, **kwargs):
        if 'initial' in kwargs:
            initial = kwargs['initial']
            del kwargs['initial']
        else:
            initial = None
        if 'debug' in kwargs:
            # debug is not implemented
            del kwargs['debug']
        baudrate = int(frequency) if frequency is not None else None
        baudrate = self._ftdi.open_bitbang_from_url(url,
                                                    direction=direction,
                                                    sync=True,
                                                    baudrate=baudrate,
                                                    **kwargs)
        self._width = 8
        self._mask = (1 << self._width) - 1
        self._direction = direction & self._mask
        if initial is not None:
            initial &= self._mask
            self.exchange(initial)
        return float(baudrate)

    def _update_direction(self) -> None:
        self._ftdi.set_bitmode(self._direction, Ftdi.BitMode.SYNCBB)


class GpioMpsseController(GpioBaseController):
    """GPIO controller for an FTDI port, in MPSSE mode.

       All GPIO pins are reachable, but MPSSE mode is slower than other modes.

       Beware that LSBs (b0..b7) and MSBs (b8..b15) are accessed with two
       subsequence commands, so a slight delay may occur when sampling or
       changing both groups at once. In other word, it is not possible to
       atomically read to / write from LSBs and MSBs. This might be worth
       checking the board design if atomic access to several lines is required.
    """

    MPSSE_PAYLOAD_MAX_LENGTH = 0xFF00  # 16 bits max (- spare for control)

    def read(self, readlen: int = 1, peek: Optional[bool] = None) \
            -> Union[int, bytes, Tuple[int]]:
        """Read the GPIO input pin electrical level.

           :param readlen: how many GPIO samples to retrieve. Each sample if
                           :py:meth:`width` bit wide.
           :param peek: whether to peak current value from port, or to use
                        MPSSE stream and HW FIFO. When peek mode is selected,
                        readlen should be 1. It is not available with wide
                        ports if some of the MSB pins are configured as input
           :return: a :py:meth:`width` bit wide integer if direct mode is used,
                    a bytes buffer if :py:meth:`width` is a byte,
                    a list of integer otherwise (MPSSE mode only).
        """
        if not self.is_connected:
            raise GpioException('Not connected')
        if peek:
            if readlen != 1:
                raise ValueError('Invalid read length with direct mode')
            if self._width > 8:
                if (0xFFFF & ~self._direction) >> 8:
                    raise ValueError('Peek mode not available with selected '
                                     'input config')
        if peek:
            return self._ftdi.read_pins()
        return self._read_mpsse(readlen)

    def write(self, out: Union[bytes, bytearray, Iterable[int], int]) -> None:
        """Set the GPIO output pin electrical level, or output a sequence of
           bytes @ constant frequency to GPIO output pins.

           :param out: a bitfield of GPIO pins, or a sequence of them
        """
        if not self.is_connected:
            raise GpioException('Not connected')
        if isinstance(out, (bytes, bytearray)):
            pass
        else:
            if isinstance(out, int):
                out = [out]
            elif not is_iterable(out):
                raise TypeError('Invalid output value')
            for val in out:
                if val > self._mask:
                    raise GpioException("Invalid value")
        self._write_mpsse(out)

    def set_frequency(self, frequency: Union[int, float]) -> None:
        if not self.is_connected:
            raise GpioException('Not connected')
        self._frequency = self._ftdi.set_frequency(float(frequency))

    def _update_direction(self) -> None:
        # nothing to do in MPSSE mode, as direction is updated with each
        # GPIO command
        pass

    def _configure(self, url: str, direction: int,
                   frequency: Union[int, float, None] = None, **kwargs):
        frequency = self._ftdi.open_mpsse_from_url(url,
                                                   direction=direction,
                                                   frequency=frequency,
                                                   **kwargs)
        self._width = self._ftdi.port_width
        self._mask = (1 << self._width) - 1
        self._direction = direction & self._mask
        return frequency

    def _read_mpsse(self, count: int) -> Tuple[int]:
        if self._width > 8:
            cmd = bytearray([Ftdi.GET_BITS_LOW, Ftdi.GET_BITS_HIGH] * count)
            fmt = f'<{count}H'
        else:
            cmd = bytearray([Ftdi.GET_BITS_LOW] * count)
            fmt = None
        cmd.append(Ftdi.SEND_IMMEDIATE)
        if len(cmd) > self.MPSSE_PAYLOAD_MAX_LENGTH:
            raise ValueError('Too many samples')
        self._ftdi.write_data(cmd)
        size = scalc(fmt) if fmt else count
        data = self._ftdi.read_data_bytes(size, 4)
        if len(data) != size:
            raise FtdiError(f'Cannot read GPIO, recv {len(data)} '
                            f'out of {size} bytes')
        if fmt:
            return sunpack(fmt, data)
        return data

    def _write_mpsse(self,
                     out: Union[bytes, bytearray, Iterable[int], int]) -> None:
        cmd = []
        low_dir = self._direction & 0xFF
        if self._width > 8:
            high_dir = (self._direction >> 8) & 0xFF
            for data in out:
                low_data = data & 0xFF
                high_data = (data >> 8) & 0xFF
                cmd.extend([Ftdi.SET_BITS_LOW, low_data, low_dir,
                            Ftdi.SET_BITS_HIGH, high_data, high_dir])
        else:
            for data in out:
                cmd.extend([Ftdi.SET_BITS_LOW, data, low_dir])
        self._ftdi.write_data(bytes(cmd))
