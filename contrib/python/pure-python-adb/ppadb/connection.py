import struct
import socket

from ppadb.protocol import Protocol
from ppadb.utils.logger import AdbLogging

logger = AdbLogging.get_logger(__name__)


class Connection:
    def __init__(self, host='localhost', port=5037, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def connect(self):
        logger.debug("Connect to adb server - {}:{}".format(self.host, self.port))

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        l_onoff = 1
        l_linger = 0

        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', l_onoff, l_linger))
        if self.timeout:
            self.socket.settimeout(self.timeout)

        try:
            self.socket.connect((self.host, self.port))
        except socket.error as e:
            self.close()
            raise RuntimeError("ERROR: connecting to {}:{} {}.\nIs adb running on your computer?".format(
                self.host,
                self.port,
                e
            ))

        return self.socket

    def close(self):
        if not self.socket:
            return

        logger.debug("Connection closed...")
        try:
            self.socket.close()
        except OSError:
            pass

    ##############################################################################################################
    #
    # Send command & Receive command result
    #
    ##############################################################################################################
    def _recv(self, length):
        return self.socket.recv(length)

    def _recv_into(self, length):
        recv = bytearray(length)
        view = memoryview(recv)
        self.socket.recv_into(view)
        return recv

    def _send(self, data):
        self.socket.send(data)

    def receive(self):
        nob = int(self._recv(4).decode('utf-8'), 16)
        recv = self._recv_into(nob)

        return recv.decode('utf-8')

    def send(self, msg):
        msg = Protocol.encode_data(msg)
        logger.debug(msg)
        self._send(msg)
        return self._check_status()

    def _check_status(self):
        recv = self._recv(4).decode('utf-8')
        if recv != Protocol.OKAY:
            error = self._recv(1024).decode('utf-8')
            raise RuntimeError("ERROR: {} {}".format(repr(recv), error))

        return True

    def check_status(self):
        return self._check_status()

    ##############################################################################################################
    #
    # Socket read/write
    #
    ##############################################################################################################
    def read_all(self):
        data = bytearray()

        while True:
            recv = self._recv(4096)
            if not recv:
                break
            data += recv

        return data

    def read(self, length=0):
        data = self._recv(length)
        return data

    def write(self, data):
        self._send(data)
