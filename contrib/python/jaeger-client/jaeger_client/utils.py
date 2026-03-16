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
import struct
from logging import Logger

import time
from typing import Any, Optional


class ErrorReporter(object):
    """
    Reports errors by emitting metrics, and if logger is provided,
    logging the error message once every log_interval_minutes

    N.B. metrics will be deprecated in the future
    """

    def __init__(
        self, metrics: Any, logger: Optional[Logger] = None, log_interval_minutes: int = 15
    ):
        self.logger = logger
        self.log_interval_minutes = log_interval_minutes
        self._last_error_reported_at = time.time()

    def error(self, *args: Any) -> None:
        if self.logger is None:
            return

        next_logging_deadline = \
            self._last_error_reported_at + (self.log_interval_minutes * 60)
        current_time = time.time()
        if next_logging_deadline >= current_time:
            # If we aren't yet at the next logging deadline
            return

        self.logger.error(*args)
        self._last_error_reported_at = current_time


def get_boolean(string: str, default: bool) -> bool:
    string = str(string).lower()
    if string in ['false', '0', 'none']:
        return False
    elif string in ['true', '1']:
        return True
    else:
        return default


def local_ip() -> Optional[str]:
    """Get the local network IP of this machine"""
    ip: Optional[str]

    try:
        ip = socket.gethostbyname(socket.gethostname())
    except IOError:
        ip = socket.gethostbyname('localhost')
    if ip.startswith('127.'):
        ip = get_local_ip_by_interfaces()
        if ip is None:
            ip = get_local_ip_by_socket()
    return ip


def get_local_ip_by_socket() -> Optional[str]:
    # Explanation : https://stackoverflow.com/questions/166506
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except IOError:
        ip = None
    finally:
        s.close()
    return ip


def get_local_ip_by_interfaces() -> Optional[str]:
    ip = None
    # Check eth0, eth1, eth2, en0, ...
    interfaces = [
        i + bytes(n) for i in (b'eth', b'en', b'wlan') for n in range(3)
    ]  # :(
    for interface in interfaces:
        try:
            ip = interface_ip(interface)
            if ip is not None:
                break
        except IOError:
            pass
    return ip


def interface_ip(interface: bytes) -> Optional[str]:
    try:
        import fcntl
        """Determine the IP assigned to us by the given network interface."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(
            fcntl.ioctl(
                sock.fileno(), 0x8915, struct.pack('256s', interface[:15])
            )[20:24]
        )
    except ImportError:
        return None
    # Explanation:
    # http://stackoverflow.com/questions/11735821/python-get-localhost-ip
    # http://stackoverflow.com/questions/24196932/how-can-i-get-the-ip-address-of-eth0-in-python
