# -*- coding: utf-8 -*-
"""Tests for the SocketOptionsAdapter and TCPKeepAliveAdapter."""
import contextlib
import platform
import socket
import sys

import pytest
try:
    from unittest import mock
except ImportError:
    import mock
import requests
from requests_toolbelt._compat import poolmanager

from requests_toolbelt.adapters import socket_options


@contextlib.contextmanager
def remove_keepidle():
    """A context manager to remove TCP_KEEPIDLE from socket."""
    TCP_KEEPIDLE = getattr(socket, 'TCP_KEEPIDLE', None)
    if TCP_KEEPIDLE is not None:
        del socket.TCP_KEEPIDLE

    yield

    if TCP_KEEPIDLE is not None:
        socket.TCP_KEEPIDLE = TCP_KEEPIDLE


@contextlib.contextmanager
def set_keepidle(value):
    """A context manager to set TCP_KEEPALIVE on socket always."""
    TCP_KEEPIDLE = getattr(socket, 'TCP_KEEPIDLE', None)
    socket.TCP_KEEPIDLE = value

    yield

    if TCP_KEEPIDLE is not None:
        socket.TCP_KEEPIDLE = TCP_KEEPIDLE
    else:
        del socket.TCP_KEEPIDLE


@mock.patch.object(requests, '__build__', 0x020500)
@mock.patch.object(poolmanager, 'PoolManager')
def test_options_passing_on_newer_requests(PoolManager):
    """Show that options are passed for a new enough version of requests."""
    fake_opts = [('test', 'options', 'fake')]
    adapter = socket_options.SocketOptionsAdapter(
        socket_options=fake_opts,
        pool_connections=10,
        pool_maxsize=5,
        pool_block=True,
    )
    PoolManager.assert_called_once_with(
        num_pools=10, maxsize=5, block=True,
        socket_options=fake_opts
    )
    assert adapter.socket_options == fake_opts


@mock.patch.object(requests, '__build__', 0x020300)
@mock.patch.object(poolmanager, 'PoolManager')
def test_options_not_passed_on_older_requests(PoolManager):
    """Show that options are not passed for older versions of requests."""
    fake_opts = [('test', 'options', 'fake')]
    socket_options.SocketOptionsAdapter(
        socket_options=fake_opts,
        pool_connections=10,
        pool_maxsize=5,
        pool_block=True,
    )
    assert PoolManager.called is False


@pytest.mark.xfail(sys.version_info.major == 2 and platform.system() == "Windows",
                   reason="Windows does not have TCP_KEEPINTVL in Python 2")
@mock.patch.object(requests, '__build__', 0x020500)
@mock.patch.object(poolmanager, 'PoolManager')
def test_keep_alive_on_newer_requests_no_idle(PoolManager):
    """Show that options are generated correctly from kwargs."""
    socket_opts = [
        (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10),
        (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 10),
    ]
    with remove_keepidle():
        adapter = socket_options.TCPKeepAliveAdapter(
            idle=30, interval=10, count=10,
            pool_connections=10,
            pool_maxsize=5,
            pool_block=True,
        )
    PoolManager.assert_called_once_with(
        num_pools=10, maxsize=5, block=True,
        socket_options=socket_opts
    )
    assert adapter.socket_options == socket_opts


@pytest.mark.xfail(sys.version_info.major == 2 and platform.system() == "Windows",
                   reason="Windows does not have TCP_KEEPINTVL in Python 2")
@mock.patch.object(requests, '__build__', 0x020500)
@mock.patch.object(poolmanager, 'PoolManager')
def test_keep_alive_on_newer_requests_with_idle(PoolManager):
    """Show that options are generated correctly from kwargs with KEEPIDLE."""
    with set_keepidle(3000):
        socket_opts = [
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10),
            (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 10),
            (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30),
        ]
        adapter = socket_options.TCPKeepAliveAdapter(
            idle=30, interval=10, count=10,
            pool_connections=10,
            pool_maxsize=5,
            pool_block=True,
        )

    PoolManager.assert_called_once_with(
        num_pools=10, maxsize=5, block=True,
        socket_options=socket_opts
    )
    assert adapter.socket_options == socket_opts
