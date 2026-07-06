# coding=utf-8

import os
import errno
import socket
import random
import logging
import platform
import threading

import six

UI16MAXVAL = (1 << 16) - 1
logger = logging.getLogger(__name__)


class PortManagerException(Exception):
    pass


class _PortReservation(object):
    __slots__ = ('filelock', 'socket')

    def __init__(self, filelock, sock):
        self.filelock = filelock
        self.socket = sock


class PortManager(object):
    """
    Port manager for dynamic port allocation.

    Allocates free TCP/UDP ports while avoiding system ephemeral port ranges.
    Supports file-based synchronization for multi-process environments.

    Example:
        with PortManager() as pm:
            port = pm.get_port()

    For yatest usage documentation:
    https://wiki.yandex-team.ru/yatool/test/#python-acquire-ports
    """

    def __init__(self, sync_dir=None):
        self._sync_dir = sync_dir or os.environ.get('PORT_SYNC_PATH')
        if self._sync_dir:
            _makedirs(self._sync_dir)

        self._valid_range = get_valid_port_range()
        self._valid_port_count = self._count_valid_ports()
        self._filelocks = {}
        self._lock = threading.Lock()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.release()

    def get_port(self, port=0, hold_socket=False):
        '''
        Gets free TCP port
        '''
        return self.get_tcp_port(port, hold_socket=hold_socket)

    def get_tcp_port(self, port=0, hold_socket=False):
        '''
        Gets free TCP port
        '''
        return self._get_port(port, socket.SOCK_STREAM, hold_socket=hold_socket)

    def get_udp_port(self, port=0, hold_socket=False):
        '''
        Gets free UDP port
        '''
        return self._get_port(port, socket.SOCK_DGRAM, hold_socket=hold_socket)

    def get_tcp_and_udp_port(self, port=0):
        '''
        Gets one free port for use in both TCP and UDP protocols
        '''
        if port and self._no_random_ports():
            return port

        retries = 20
        while retries > 0:
            retries -= 1

            result_port = self.get_tcp_port()
            if not self.is_port_free(result_port, socket.SOCK_DGRAM):
                self.release_port(result_port)
            # Don't try to _capture_port(), it's already captured in the get_tcp_port()
            return result_port
        raise Exception('Failed to find port')

    def release_port(self, port):
        with self._lock:
            self._release_port_no_lock(port)

    def unbind_port(self, port):
        '''
        Closes the held socket for the port but keeps the reservation lock
        until release_port() / release().
        Call this immediately before the process that will bind the port.
        '''
        with self._lock:
            reservation = self._filelocks.get(port)
            if reservation is not None and reservation.socket is not None:
                reservation.socket.close()
                reservation.socket = None

    def bind_port(self, port, sock_type=socket.SOCK_STREAM):
        '''
        Re-binds a held socket for an already reserved port.
        Used to restore OS-level port holding after unbind_port().
        '''
        with self._lock:
            reservation = self._filelocks.get(port)
            if reservation is None:
                return False
            if reservation.socket is not None:
                return True
            sock = self._try_bind_port(port, sock_type)
            if sock is None:
                return False
            reservation.socket = sock
            return True

    def _release_port_no_lock(self, port):
        reservation = self._filelocks.pop(port, None)
        if reservation is not None:
            self._close_reservation(reservation)

    def _close_reservation(self, reservation):
        if reservation.socket is not None:
            reservation.socket.close()
            reservation.socket = None
        if reservation.filelock:
            reservation.filelock.release()

    def release(self):
        with self._lock:
            while self._filelocks:
                _, reservation = self._filelocks.popitem()
                self._close_reservation(reservation)

    def _reserved_ports(self):
        return sorted(self._filelocks.keys())

    def get_port_range(self, start_port, count, random_start=True):
        assert count > 0
        if start_port and self._no_random_ports():
            return start_port

        candidates = []

        def drop_candidates():
            for port in candidates:
                self._release_port_no_lock(port)
            candidates[:] = []

        with self._lock:
            for attempts in six.moves.range(128):
                for left, right in self._valid_range:
                    if right - left < count:
                        continue

                    if random_start:
                        start = random.randint(left, right - ((right - left) // 2))
                    else:
                        start = left
                    for probe_port in six.moves.range(start, right):
                        if self._capture_port_no_lock(probe_port, socket.SOCK_STREAM):
                            candidates.append(probe_port)
                        else:
                            drop_candidates()

                        if len(candidates) == count:
                            return candidates[0]
                    # Can't find required number of ports without gap in the current range
                    drop_candidates()

            raise PortManagerException(
                "Failed to find valid port range (start_port: {} count: {}) (range: {} used: {})".format(
                    start_port, count, self._valid_range, self._reserved_ports()
                )
            )

    def _count_valid_ports(self):
        res = 0
        for left, right in self._valid_range:
            res += right - left
        assert res, ('There are no available valid ports', self._valid_range)
        return res

    def _get_port(self, port, sock_type, hold_socket=False):
        if port and self._no_random_ports():
            return port

        if len(self._filelocks) >= self._valid_port_count:
            raise PortManagerException(
                "All valid ports are taken ({}): {}".format(self._valid_range, self._reserved_ports())
            )

        salt = random.randint(0, UI16MAXVAL)
        for attempt in six.moves.range(self._valid_port_count):
            probe_port = (salt + attempt) % self._valid_port_count

            for left, right in self._valid_range:
                if probe_port >= (right - left):
                    probe_port -= right - left
                else:
                    probe_port += left
                    break
            if not self._capture_port(probe_port, sock_type, hold_socket=hold_socket):
                continue
            return probe_port

        raise PortManagerException(
            "Failed to find valid port (range: {} used: {})".format(self._valid_range, self._reserved_ports())
        )

    def _capture_port(self, port, sock_type, hold_socket=False):
        with self._lock:
            return self._capture_port_no_lock(port, sock_type, hold_socket=hold_socket)

    def _make_socket(self, sock_type):
        return socket.socket(socket.AF_INET6, sock_type)

    def _configure_socket(self, sock):
        if os.name == 'nt' and hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(sock, 'set_inheritable'):
            sock.set_inheritable(False)

    def _try_bind_port(self, port, sock_type):
        sock = self._make_socket(sock_type)
        self._configure_socket(sock)
        try:
            sock.bind(('::', port))
        except socket.error as e:
            sock.close()
            if e.errno == errno.EADDRINUSE:
                return None
            raise
        return sock

    def is_port_free(self, port, sock_type=socket.SOCK_STREAM):
        sock = self._try_bind_port(port, sock_type)
        if sock is None:
            return False
        sock.close()
        return True

    def _capture_port_no_lock(self, port, sock_type, hold_socket=False):
        if port in self._filelocks:
            return False

        filelock = None
        try:
            if self._sync_dir:
                # Lazy import to keep module hermetic and work without Arcadia Python
                # (PYTEST_SCRIPT mode with USE_ARCADIA_PYTHON=no)
                import library.python.filelock

                filelock = library.python.filelock.FileLock(os.path.join(self._sync_dir, str(port)))
                if not filelock.acquire(blocking=False):
                    return False

            if hold_socket:
                sock = self._try_bind_port(port, sock_type)
                if sock is None:
                    return False
                self._filelocks[port] = _PortReservation(filelock, sock)
                filelock = None
                return True

            if not self.is_port_free(port, sock_type):
                return False

            self._filelocks[port] = _PortReservation(filelock, None)
            filelock = None
            return True
        finally:
            if filelock:
                filelock.release()

    def _no_random_ports(self):
        return os.environ.get("NO_RANDOM_PORTS")


def get_valid_port_range():
    first_valid = 1025
    last_valid = UI16MAXVAL

    given_range = os.environ.get('VALID_PORT_RANGE')
    if given_range and ':' in given_range:
        return [list(int(x) for x in given_range.split(':', 2))]

    first_eph, last_eph = get_ephemeral_range()
    first_invalid = max(first_eph, first_valid)
    last_invalid = min(last_eph, last_valid)

    ranges = []
    if first_invalid > first_valid:
        ranges.append((first_valid, first_invalid - 1))
    if last_invalid < last_valid:
        ranges.append((last_invalid + 1, last_valid))
    return ranges


def get_ephemeral_range():
    if platform.system() == 'Linux':
        filename = "/proc/sys/net/ipv4/ip_local_port_range"
        if os.path.exists(filename):
            with open(filename) as afile:
                data = afile.read(1024)  # fix for musl
            port_range = tuple(map(int, data.strip().split()))
            if len(port_range) == 2:
                return port_range
            else:
                logger.warning("Bad ip_local_port_range format: '%s'. Going to use IANA suggestion", data)
    elif platform.system() == 'Darwin':
        first = _sysctlbyname_uint("net.inet.ip.portrange.first")
        last = _sysctlbyname_uint("net.inet.ip.portrange.last")
        if first and last:
            return first, last
    # IANA suggestion
    return (1 << 15) + (1 << 14), UI16MAXVAL


def _sysctlbyname_uint(name):
    try:
        from ctypes import CDLL, c_uint, byref
        from ctypes.util import find_library
    except ImportError:
        return

    libc = CDLL(find_library("c"))
    size = c_uint(0)
    res = c_uint(0)
    libc.sysctlbyname(name, None, byref(size), None, 0)
    libc.sysctlbyname(name, byref(res), byref(size), None, 0)
    return res.value


def _makedirs(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            return
        raise
