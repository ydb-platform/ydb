"""ModbusMessage layer.

is extending ModbusProtocol to handle receiving and sending of messsagees.

ModbusMessage provides a unified interface to send/receive Modbus requests/responses.
"""
from __future__ import annotations

from binascii import a2b_hex, b2a_hex

from pymodbus.framer.base import FramerBase
from pymodbus.logging import Log


class FramerAscii(FramerBase):
    r"""Modbus ASCII Frame Controller.

    Layout::
        [ Start ][ Dev id ][ Function ][ Data ][ LRC ][ End ]
          1c       2c        2c          N*2c    1c     2c

        * data can be 1 - 2x252 chars
        * end is "\\r\\n" (Carriage return line feed), however the line feed
          character can be changed via a special command
        * start is ":"

    This framer is used for serial transmission.  Unlike the RTU protocol,
    the data in this framer is transferred in plain text ascii.
    """

    START = b':'
    END = b'\r\n'
    MIN_SIZE = 10


    def decode(self, data: bytes) -> tuple[int, int, int, bytes]:
        """Decode ADU."""
        used_len = 0
        data_len = len(data)
        while True:
            if data_len - used_len < self.MIN_SIZE:
                Log.debug("Short frame: {} wait for more data", data, ":hex")
                return used_len, 0, 0, self.EMPTY
            buffer = data[used_len:]
            if buffer[0:1] != self.START:
                if (i := buffer.find(self.START)) == -1:
                    Log.debug("No frame start in data: {}, wait for data", data, ":hex")
                    return data_len, 0, 0, self.EMPTY
                used_len += i
                continue
            if (end := buffer.find(self.END)) == -1:
                Log.debug("Incomplete frame: {} wait for more data", data, ":hex")
                return used_len, 0, 0, self.EMPTY
            dev_id = int(buffer[1:3], 16)
            lrc = int(buffer[end - 2: end], 16)
            msg = a2b_hex(buffer[1 : end - 2])
            used_len += end + 2
            if not self.check_LRC(msg, lrc):
                Log.debug("LRC wrong in frame: {} skipping", data, ":hex")
                continue
            return used_len, dev_id, 0, msg[1:]

    def encode(self, data: bytes, device_id: int, _tid: int) -> bytes:
        """Encode ADU."""
        dev_id = device_id.to_bytes(1,'big')
        checksum = self.compute_LRC(dev_id + data)
        frame = (
            self.START +
            f"{device_id:02x}".encode() +
            b2a_hex(data) +
            f"{checksum:02x}".encode() +
            self.END
        ).upper()
        return frame

    @classmethod
    def compute_LRC(cls, data: bytes) -> int:
        """Use to compute the longitudinal redundancy check against a string."""
        lrc = sum(int(a) for a in data) & 0xFF
        lrc = (lrc ^ 0xFF) + 1
        return lrc & 0xFF

    @classmethod
    def check_LRC(cls, data: bytes, check: int) -> bool:
        """Check if the passed in data matches the LRC."""
        return cls.compute_LRC(data) == check
