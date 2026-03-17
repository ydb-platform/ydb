import socket

import pytest

from jupyter_client.ssh.tunnel import open_tunnel, select_random_ports


def test_random_ports():
    for _ in range(4096):
        ports = select_random_ports(10)
        assert len(ports) == 10
        for p in ports:
            assert ports.count(p) == 1


def test_open_tunnel():
    with pytest.raises((RuntimeError, socket.error)):
        open_tunnel("tcp://localhost:1234", "does.not.exist")
