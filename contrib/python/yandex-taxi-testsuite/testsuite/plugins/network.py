import platform
import socket
import typing

import pytest


class BaseError(Exception):
    """Base class for errors from this module."""


class NoEnabledPorts(BaseError):
    """Raised if there are not free ports for worker"""


@pytest.fixture(scope='session')
def get_free_port(
    _get_free_port_sock_storing,
    _get_free_port_range_based,
) -> typing.Callable[[], int]:
    """
    Returns an ephemeral TCP port that is free for IPv4 and for IPv6.
    """
    if platform.system() == 'Linux':
        return _get_free_port_sock_storing
    return _get_free_port_range_based


@pytest.fixture(scope='session')
def _get_free_port_sock_storing(
    _testsuite_default_af,
    _testsuite_socket_cleanup,
) -> typing.Callable[[], int]:
    family, address = _testsuite_default_af

    # Relies on https://github.com/torvalds/linux/commit/aacd9289af8b82f5fb01b
    def _get_free_port():
        sock = socket.socket(family, socket.SOCK_STREAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((address, 0))
            _testsuite_socket_cleanup(sock)  # shared variable
            return sock.getsockname()[1]
        except OSError:
            raise NoEnabledPorts()

    return _get_free_port


@pytest.fixture(scope='session')
def _get_free_port_range_based(
    _testsuite_default_af,
) -> typing.Callable[[], int]:
    family, address = _testsuite_default_af
    port_seq = iter(range(61000, 2048, -1))

    def _get_free_port():
        for port in port_seq:
            if _is_port_free(port, family, address):
                return port
        raise NoEnabledPorts()

    return _get_free_port


@pytest.fixture(scope='session')
def _testsuite_socket_cleanup():
    sock_list: list[socket.socket] = []
    try:
        yield sock_list.append
    finally:
        for sock in sock_list:
            sock.close()


@pytest.fixture(scope='session')
def _testsuite_default_af():
    for family, address in _get_inet_families():
        return family, address
    raise RuntimeError('No suitable address families available')


def _is_port_free(port_num: int, family: int, address: str) -> bool:
    try:
        with socket.socket(family, socket.SOCK_STREAM) as sock:
            sock.bind((address, port_num))
    except OSError:
        return False
    else:
        return True


def _is_af_available(family: int, address: str):
    return _is_port_free(0, family, address)


def _get_inet_families():
    for family_str, address in (('AF_INET6', '::'), ('AF_INET', '127.0.0.1')):
        family = getattr(socket, family_str, None)
        if family and _is_af_available(family, address):
            yield family, address
