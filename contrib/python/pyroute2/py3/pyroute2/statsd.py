import ctypes
import os
import socket
from typing import Optional, Union

from pyroute2 import netns
from pyroute2.config import metric_type
from pyroute2.netlink.coredata import CoreConfig, CoreSocketSpec


class StatsDClientSocket(socket.socket):
    '''StatsD client.'''

    def __init__(
        self,
        address: Optional[tuple[str, int]] = None,
        use_socket: Optional[socket.socket] = None,
        flags: int = os.O_CREAT,
        libc: Optional[ctypes.CDLL] = None,
    ):
        self.spec = CoreSocketSpec(
            CoreConfig(
                netns=None, address=address, use_socket=use_socket is not None
            )
        )
        self.status = self.spec.status
        self.buffer: str = ''
        if use_socket is not None:
            fd = use_socket.fileno()
        else:
            prime = netns.create_socket(
                self.spec['netns'], socket.AF_INET, socket.SOCK_DGRAM
            )
            fd = os.dup(prime.fileno())
            prime.close()
        super().__init__(fileno=fd)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def commit(self):
        self.sendto(self.buffer.encode(), self.spec['address'])
        self.buffer = ''

    def put(
        self, name: str, value: Union[int, str], kind: metric_type
    ) -> None:
        self.buffer += f'{name}:{value}|{kind}\n'

    def incr(self, name: str, value: int = 1) -> None:
        self.put(name, value, 'c')
        self.commit()

    def gauge(self, name: str, value: int) -> None:
        self.put(name, value, 'g')
        self.commit()

    def timing(self, name: str, value: int) -> None:
        self.put(name, value, 'ms')
        self.commit()
