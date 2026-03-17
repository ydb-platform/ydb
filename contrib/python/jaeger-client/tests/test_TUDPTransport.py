# Copyright (c) 2016 Uber Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
import unittest
from unittest import mock

import pytest

from jaeger_client.TUDPTransport import TUDPTransport


class TUDPTransportTests(unittest.TestCase):
    def setUp(self):
        self.t = TUDPTransport('127.0.0.1', 12345)

    def test_constructor_blocking(self):
        t = TUDPTransport('127.0.0.1', 12345, blocking=True)
        assert t.transport_sock.gettimeout() is None

    def test_constructor_nonblocking(self):
        t = TUDPTransport('127.0.0.1', 12345, blocking=False)
        assert t.transport_sock.gettimeout() == 0

    def test_write(self):
        self.t.write(b'hello')

    def test_isopen_when_open(self):
        assert self.t.isOpen() is True

    def test_isopen_when_closed(self):
        self.t.close()
        assert self.t.isOpen() is False

    def test_close(self):
        self.t.close()
        with self.assertRaises(Exception):
            # Something bad should happen if we send on a closed socket..
            self.t.write(b'hello')


def test_created_socket_default_family():
    transport = TUDPTransport('unknown-host', 12345)
    sock = transport._create_socket()
    assert sock.family == TUDPTransport.DEFAULT_SOCKET_FAMILY


@pytest.mark.parametrize('addrinfo,expected_family', (
    (
        (
            socket.AddressFamily.AF_INET6, socket.SocketKind.SOCK_DGRAM,
            17, '', ('aced:a11:7e57', 12345, 0, 0)
        ),
        socket.AF_INET6
    ),
    (
        (
            socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_DGRAM,
            17, '', ('127.0.0.1', 12345)
        ),
        socket.AF_INET
    ),
    (
        None,
        TUDPTransport.DEFAULT_SOCKET_FAMILY
    )
))
def test_created_socket_specified_family(addrinfo, expected_family):
    return_value = [addrinfo] if addrinfo else []
    with mock.patch('socket.getaddrinfo', return_value=return_value):
        transport = TUDPTransport('ipv6-host', 12345)
        sock = transport._create_socket()
        assert sock.family == expected_family
