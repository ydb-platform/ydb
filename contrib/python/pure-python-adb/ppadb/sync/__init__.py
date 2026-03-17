import struct
import time
import os

from ppadb.protocol import Protocol
from ppadb.sync.stats import S_IFREG

import logging

logger = logging.getLogger(__name__)


class Sync:
    TEMP_PATH = '/data/local/tmp'
    DEFAULT_CHMOD = 0o644
    DATA_MAX_LENGTH = 65536

    def __init__(self, connection):
        self.connection = connection

    @staticmethod
    def temp(path):
        return "{}/{}".format(Sync.TEMP_PATH, os.path.basename(path))

    def push(self, src, dest, mode, progress=None):
        """Push from local path |src| to |dest| on device.

        :param progress: callback, called with (filename, total_size, sent_size)
        """
        if not os.path.exists(src):
            raise FileNotFoundError("Can't find the source file {}".format(src))

        stat = os.stat(src)

        timestamp = int(stat.st_mtime)

        total_size = os.path.getsize(src)
        sent_size = 0

        # SEND
        mode = mode | S_IFREG
        args = "{dest},{mode}".format(
            dest=dest,
            mode=mode
        )
        self._send_str(Protocol.SEND, args)

        # DATA
        with open(src, 'rb') as stream:
            while True:
                chunk = stream.read(self.DATA_MAX_LENGTH)
                if not chunk:
                    break

                sent_size += len(chunk)
                self._send_length(Protocol.DATA, len(chunk))
                self.connection.write(chunk)

                if progress is not None:
                    progress(src, total_size, sent_size)

        # DONE
        self._send_length(Protocol.DONE, timestamp)
        self.connection._check_status()

    def pull(self, src, dest):
        error = None

        # RECV
        self._send_str(Protocol.RECV, src)

        # DATA
        with open(dest, 'wb') as stream:
            while True:
                flag = self.connection.read(4).decode('utf-8')

                if flag == Protocol.DATA:
                    data = self._read_data()
                    stream.write(data)
                elif flag == Protocol.DONE:
                    self.connection.read(4)
                    return
                elif flag == Protocol.FAIL:
                    return self._read_data().decode('utf-8')

    def _integer(self, little_endian):
        return struct.unpack("<I", little_endian)

    def _little_endian(self, n):
        return struct.pack('<I', n)

    def _read_data(self):
        length = self._integer(self.connection.read(4))[0]
        data = bytearray()
        while len(data) < length:
            data += self.connection.read(length - len(data))
        return data

    def _send_length(self, cmd, length):
        le_len = self._little_endian(length)
        data = cmd.encode() + le_len

        logger.debug("Send length: {}".format(data))
        self.connection.write(data)

    def _send_str(self, cmd, args):
        """
        Format:
            {Command}{args length(little endian)}{str}
        Length:
            {4}{4}{str length}
        """
        logger.debug("{} {}".format(cmd, args))
        args = args.encode('utf-8')

        le_args_len = self._little_endian(len(args))
        data = cmd.encode() + le_args_len + args
        logger.debug("Send string: {}".format(data))
        self.connection.write(data)
