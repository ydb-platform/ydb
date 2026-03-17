#
# Author: Angus Lees <gus@inodes.org>
#
# Backported from a Neutron privsep proposal with the
# permission of the author.
#

from __future__ import absolute_import

import functools
import socket
import types
try:
    import cPickle as pickle
except ImportError:
    import pickle

from pyroute2 import config as config


_socketmethods = (
    'bind', 'close', 'connect', 'connect_ex', 'listen',
    'getpeername', 'getsockname', 'getsockopt', 'makefile',
    'recv', 'recvfrom', 'recv_into', 'recvfrom_into',
    'send', 'sendto', 'sendall', 'setsockopt', 'setblocking',
    'settimeout', 'gettimeout', 'shutdown')


def _forward(name, self, *args, **kwargs):
    return getattr(self._sock, name)(*args, **kwargs)


class _SocketWrapper(object):
    """eventlet-monkeypatch friendly socket class"""

    def __init__(self, *args, **kwargs):
        _sock = kwargs.get('_sock', None) or socket.socket(*args, **kwargs)
        self._sock = _sock
        for name in _socketmethods:
            f = functools.partial(_forward, name)
            f.__name__ = name
            setattr(_SocketWrapper, name, types.MethodType(f, self))

    def fileno(self):
        return self._sock.fileno()

    def dup(self):
        return self.__class__(_sock=self._sock.dup())


class _MpConnection(object):
    """Highly limited multiprocessing.Connection alternative"""
    def __init__(self, sock):
        sock.setblocking(True)
        self.sock = sock

    def fileno(self):
        return self.sock.fileno()

    def send(self, obj):
        pickle.dump(obj, self, protocol=-1)

    def write(self, s):
        self.sock.sendall(s)

    def recv(self):
        return pickle.load(self)

    def read(self, n):
        return self.sock.recv(n)

    def readline(self):
        buf = b''
        c = None
        while c != b'\n':
            c = self.sock.recv(1)
            buf += c
        return buf

    def close(self):
        self.sock.close()


def _MultiprocessingPipe():
    """multiprocess.Pipe reimplementation that uses MpConnection wrapper"""

    s1, s2 = socket.socketpair()
    return (_MpConnection(s1), _MpConnection(s2))


def asyncio_config():
    config.SocketBase = _SocketWrapper
    config.MpPipe = _MultiprocessingPipe
    config.ipdb_nl_async = False
