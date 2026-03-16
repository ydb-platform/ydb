import asyncio
import struct
import socket

from ppadb.protocol import Protocol
from ppadb.utils.logger import AdbLogging

logger = AdbLogging.get_logger(__name__)


class ConnectionAsync:
    def __init__(self, host='localhost', port=5037, timeout=None):
        self.host = host
        self.port = int(port)
        self.timeout = timeout

        self.reader = None
        self.writer = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, type, value, traceback):
        await self.close()

    async def connect(self):
        logger.debug("Connect to ADB server - %s:%d", self.host, self.port)

        try:
            if self.timeout:
                self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(self.host, self.port), self.timeout)
            else:
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

        except (OSError, asyncio.TimeoutError) as e:
            raise RuntimeError("ERROR: connecting to {}:{} {}.\nIs adb running on your computer?".format(self.host, self.port, e))

        return self

    async def close(self):
        logger.debug("Connection closed...")
        
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except OSError:
                pass

        self.reader = None
        self.writer = None

    ##############################################################################################################
    #
    # Send command & Receive command result
    #
    ##############################################################################################################
    async def _recv(self, length):
        return await asyncio.wait_for(self.reader.read(length), self.timeout)

    async def _send(self, data):
        self.writer.write(data)
        await asyncio.wait_for(self.writer.drain(), self.timeout)

    async def receive(self):
        nob = int((await self._recv(4)).decode('utf-8'), 16)
        return (await self._recv(nob)).decode('utf-8')

    async def send(self, msg):
        msg = Protocol.encode_data(msg)
        logger.debug(msg)
        await self._send(msg)
        return await self._check_status()

    async def _check_status(self):
        recv = (await self._recv(4)).decode('utf-8')
        if recv != Protocol.OKAY:
            error = (await self._recv(1024)).decode('utf-8')
            raise RuntimeError("ERROR: {} {}".format(repr(recv), error))

        return True

    ##############################################################################################################
    #
    # Socket read/write
    #
    ##############################################################################################################
    async def read_all(self):
        data = bytearray()

        while True:
            recv = await self._recv(4096)
            if not recv:
                break
            data += recv

        return data

    async def read(self, length=0):
        data = await self._recv(length)
        return data

    async def write(self, data):
        await self._send(data)
