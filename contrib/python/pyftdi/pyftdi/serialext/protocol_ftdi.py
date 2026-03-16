# Copyright (c) 2008-2024, Emmanuel Blot <emmanuel.blot@free.fr>
# Copyright (c) 2008-2016, Neotion
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

# this file has not been updated for a while, so coding style needs some love
# pylint: disable=attribute-defined-outside-init
# pylint: disable=invalid-name
# pylint: disable=missing-class-docstring
# pylint: disable=missing-module-docstring

from io import RawIOBase
from time import sleep, time as now
from serial import SerialBase, SerialException, VERSION as pyserialver
from pyftdi.ftdi import Ftdi
from pyftdi.usbtools import UsbToolsError


class FtdiSerial(SerialBase):
    """Base class for Serial port implementation compatible with pyserial API
       using a USB device.
    """

    BAUDRATES = sorted([9600 * (x+1) for x in range(6)] +
                       list(range(115200, 1000000, 115200)) +
                       list(range(1000000, 13000000, 100000)))

    PYSERIAL_VERSION = tuple(int(x) for x in pyserialver.split('.'))

    def open(self):
        """Open the initialized serial port"""
        if self.port is None:
            raise SerialException("Port must be configured before use.")
        try:
            device = Ftdi.create_from_url(self.port)
        except (UsbToolsError, IOError) as exc:
            raise SerialException(f'Unable to open USB port {self.portstr}: '
                                  f'{exc}') from exc
        self.udev = device
        self._set_open_state(True)
        self._reconfigure_port()

    def close(self):
        """Close the open port"""
        self._set_open_state(False)
        if self.udev:
            self.udev.close()
            self.udev = None

    def read(self, size=1):
        """Read size bytes from the serial port. If a timeout is set it may
           return less characters as requested. With no timeout it will block
           until the requested number of bytes is read."""
        data = bytearray()
        start = now()
        while True:
            buf = self.udev.read_data(size)
            data.extend(buf)
            size -= len(buf)
            if size <= 0:
                break
            if self._timeout is not None:
                if buf:
                    break
                ms = now()-start
                if ms > self._timeout:
                    break
            sleep(0.01)
        return bytes(data)

    def write(self, data):
        """Output the given string over the serial port."""
        return self.udev.write_data(data)

    def flush(self):
        """Flush of file like objects. In this case, wait until all data
           is written."""

    def reset_input_buffer(self):
        """Clear input buffer, discarding all that is in the buffer."""
        self.udev.purge_rx_buffer()

    def reset_output_buffer(self):
        """Clear output buffer, aborting the current output and
        discarding all that is in the buffer."""
        self.udev.purge_tx_buffer()

    def send_break(self, duration=0.25):
        """Send break condition."""
        self.udev.set_break(True)
        sleep(duration)
        self.udev.set_break(False)

    def _update_break_state(self):
        """Send break condition. Not supported"""
        self.udev.set_break(self._break_state)

    def _update_rts_state(self):
        """Set terminal status line: Request To Send"""
        self.udev.set_rts(self._rts_state)

    def _update_dtr_state(self):
        """Set terminal status line: Data Terminal Ready"""
        self.udev.set_dtr(self._dtr_state)

    @property
    def ftdi(self) -> Ftdi:
        """Return the Ftdi instance.

           :return: the Ftdi instance
        """
        return self.udev

    @property
    def usb_path(self):
        """Return the physical location as a triplet.
             * bus is the USB bus
             * address is the address on the USB bus
             * interface is the interface number on the FTDI debice

           :return: (bus, address, interface)
           :rtype: tuple(int)
        """
        return self.udev.usb_path

    @property
    def cts(self):
        """Read terminal status line: Clear To Send"""
        return self.udev.get_cts()

    @property
    def dsr(self):
        """Read terminal status line: Data Set Ready"""
        return self.udev.get_dsr()

    @property
    def ri(self):
        """Read terminal status line: Ring Indicator"""
        return self.udev.get_ri()

    @property
    def cd(self):
        """Read terminal status line: Carrier Detect"""
        return self.udev.get_cd()

    @property
    def in_waiting(self):
        """Return the number of characters currently in the input buffer."""
        # not implemented
        return 0

    @property
    def out_waiting(self):
        """Return the number of bytes currently in the output buffer."""
        return 0

    @property
    def fifoSizes(self):
        """Return the (TX, RX) tupple of hardware FIFO sizes"""
        return self.udev.fifo_sizes

    def _reconfigure_port(self):
        try:
            self._baudrate = self.udev.set_baudrate(self._baudrate, True)
            self.udev.set_line_property(self._bytesize,
                                        self._stopbits,
                                        self._parity)
            if self._rtscts:
                self.udev.set_flowctrl('hw')
            elif self._xonxoff:
                self.udev.set_flowctrl('sw')
            else:
                self.udev.set_flowctrl('')
            try:
                self.udev.set_dynamic_latency(12, 200, 50)
            except AttributeError:
                # backend does not support this feature
                pass
        except IOError as exc:
            err = self.udev.get_error_string()
            raise SerialException(f'{exc} ({err})') from exc

    def _set_open_state(self, open_):
        self.is_open = bool(open_)


# assemble Serial class with the platform specific implementation and the base
# for file-like behavior.
class Serial(FtdiSerial, RawIOBase):

    BACKEND = 'pyftdi'

    def __init__(self, *args, **kwargs):
        RawIOBase.__init__(self)
        FtdiSerial.__init__(self, *args, **kwargs)
