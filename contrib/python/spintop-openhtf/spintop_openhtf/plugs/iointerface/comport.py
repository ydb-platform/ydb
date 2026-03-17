import openhtf.plugs as plugs
from openhtf.util import conf

import time

import serial
from serial.threaded import ReaderThread

from .base import IOInterface

class ComportInterface(IOInterface):
    """An interface to a comport.
    
    Allows reading and writing. A background thread reads any data that comes in 
    and those lines can be accessed using the `next_line` function.
    
    """

    def __init__(self, comport, baudrate=115200):
        super().__init__()
        self.comport = comport
        self.baudrate = baudrate

        self._serial = None
        self._reader = None

    def open(self, _serial=None):
        """Opens the serial port using the :attr:`~ComportInterface.comport` and :attr:`~ComportInterface.baudrate`
        object attributes.
        
        Arguments:
            _serial:
                Optionnal underlying :class:`serial.Serial` object to use. Used for mock testing.
        
        """
        self.close()
        if not _serial:
            _serial = serial.Serial(self.comport, self.baudrate, timeout=self.timeout)
        self._serial = _serial
        self._reader = ReaderThread(self._serial, lambda: self)
        self._reader.start()

    def close(self):
        """Attempts to close the serial port if it exists."""
        if self._reader is not None and self._serial.is_open:
            self._reader.close()
            self._reader = None

    def _write(self, string):
        """Write the string into the comport."""
        return self._reader.write(string.encode('utf8'))

    def com_target(self, *args, **kwargs):
        """Alias for message_target"""
        return self.message_target(*args, **kwargs)
