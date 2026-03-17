import logging
from dataclasses import dataclass
from itertools import count
from socketserver import ThreadingTCPServer, StreamRequestHandler
from typing import BinaryIO, Dict, Tuple

from fakeredis import FakeRedis
from fakeredis import FakeServer
from fakeredis._server import ServerType

LOGGER = logging.getLogger("fakeredis")
LOGGER.setLevel(logging.DEBUG)


def to_bytes(value) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


@dataclass
class Client:
    connection: FakeRedis
    client_address: int


@dataclass
class Reader:
    reader: BinaryIO

    def load_array(self, length: int):
        array = [None] * length
        for i in range(length):
            array[i] = self.load()
        return array

    def load(self):
        line = self.reader.readline().strip()
        match line[0:1], line[1:]:
            case b"*", length:
                return self.load_array(int(length))
            case b"$", length:
                bulk_string = self.reader.read(int(length) + 2).strip()
                if len(bulk_string) != int(length):
                    raise ValueError()
                return bulk_string
            case b":", value:
                return int(value)
            case b"+", value:
                return value
            case b"-", value:
                return Exception(value)
            case _:
                return None


@dataclass
class Writer:
    writer: BinaryIO

    def dump(self, value, dump_bulk=False):
        if isinstance(value, int):
            self.writer.write(f":{value}\r\n".encode())
        elif isinstance(value, (str, bytes)):
            value = to_bytes(value)
            if dump_bulk or b"\r" in value or b"\n" in value:
                self.writer.write(b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n")
            else:
                self.writer.write(b"+" + value + b"\r\n")
        elif isinstance(value, (list, set)):
            self.writer.write(f"*{len(value)}\r\n".encode())
            for item in value:
                self.dump(item, dump_bulk=True)
        elif value is None:
            self.writer.write("$-1\r\n".encode())
        elif isinstance(value, Exception):
            self.writer.write(f"-{value.args[0]}\r\n".encode())


class TCPFakeRequestHandler(StreamRequestHandler):

    def setup(self) -> None:
        super().setup()
        if self.client_address in self.server.clients:
            self.current_client = self.server.clients[self.client_address]
        else:
            self.current_client = Client(
                connection=FakeRedis(server=self.server.fake_server),
                client_address=self.client_address,
            )
            self.reader = Reader(self.rfile)
            self.writer = Writer(self.wfile)
            self.server.clients[self.client_address] = self.current_client

    def handle(self):
        while True:
            try:
                self.data = self.reader.load()
                LOGGER.debug(f">>> {self.client_address[0]}: {self.data}")
                res = self.current_client.connection.execute_command(*self.data)
                LOGGER.debug(f"<<< {self.client_address[0]}: {res}")
                self.writer.dump(res)
            except Exception as e:
                LOGGER.debug(f"!!! {self.client_address[0]}: {e}")
                self.writer.dump(e)
                break

    def finish(self) -> None:
        del self.server.clients[self.current_client.client_address]
        super().finish()


class TcpFakeServer(ThreadingTCPServer):
    def __init__(
        self,
        server_address: Tuple[str | bytes | bytearray, int],
        bind_and_activate: bool = True,
        server_type: ServerType = "redis",
        server_version: Tuple[int, ...] = (7, 4),
    ):
        super().__init__(server_address, TCPFakeRequestHandler, bind_and_activate)
        self.fake_server = FakeServer(server_type=server_type, version=server_version)
        self.client_ids = count(0)
        self.clients: Dict[int, FakeRedis] = dict()


if __name__ == "__main__":
    server = TcpFakeServer(("localhost", 19000))
    server.serve_forever()
