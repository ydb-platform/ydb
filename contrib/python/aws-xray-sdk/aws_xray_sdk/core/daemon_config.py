import os

from .exceptions.exceptions import InvalidDaemonAddressException

DAEMON_ADDRESS_KEY = "AWS_XRAY_DAEMON_ADDRESS"
DEFAULT_ADDRESS = '127.0.0.1:2000'


class DaemonConfig:
    """The class that stores X-Ray daemon configuration about
    the ip address and port for UDP and TCP port. It gets the address
    string from ``AWS_TRACING_DAEMON_ADDRESS`` and then from recorder's
    configuration for ``daemon_address``.
    A notation of '127.0.0.1:2000' or 'tcp:127.0.0.1:2000 udp:127.0.0.2:2001'
    are both acceptable. The former one means UDP and TCP are running at
    the same address.
    By default it assumes a X-Ray daemon running at 127.0.0.1:2000
    listening to both UDP and TCP traffic.
    """
    def __init__(self, daemon_address=DEFAULT_ADDRESS):
        if daemon_address is None:
            daemon_address = DEFAULT_ADDRESS

        val = os.getenv(DAEMON_ADDRESS_KEY, daemon_address)
        configs = val.split(' ')
        if len(configs) == 1:
            self._parse_single_form(configs[0])
        elif len(configs) == 2:
            self._parse_double_form(configs[0], configs[1], val)
        else:
            raise InvalidDaemonAddressException('Invalid daemon address %s specified.' % val)

    def _parse_single_form(self, val):
        try:
            configs = val.split(':')
            self._udp_ip = configs[0]
            self._udp_port = int(configs[1])
            self._tcp_ip = configs[0]
            self._tcp_port = int(configs[1])
        except Exception:
            raise InvalidDaemonAddressException('Invalid daemon address %s specified.' % val)

    def _parse_double_form(self, val1, val2, origin):
        try:
            configs1 = val1.split(':')
            configs2 = val2.split(':')
            mapping = {
                configs1[0]: configs1,
                configs2[0]: configs2,
            }

            tcp_info = mapping.get('tcp')
            udp_info = mapping.get('udp')

            self._tcp_ip = tcp_info[1]
            self._tcp_port = int(tcp_info[2])
            self._udp_ip = udp_info[1]
            self._udp_port = int(udp_info[2])
        except Exception:
            raise InvalidDaemonAddressException('Invalid daemon address %s specified.' % origin)

    @property
    def udp_ip(self):
        return self._udp_ip

    @property
    def udp_port(self):
        return self._udp_port

    @property
    def tcp_ip(self):
        return self._tcp_ip

    @property
    def tcp_port(self):
        return self._tcp_port
