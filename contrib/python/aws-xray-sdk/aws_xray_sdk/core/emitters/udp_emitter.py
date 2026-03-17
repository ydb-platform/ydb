import logging
import socket

from aws_xray_sdk.core.daemon_config import DaemonConfig
from ..exceptions.exceptions import InvalidDaemonAddressException

log = logging.getLogger(__name__)


PROTOCOL_HEADER = "{\"format\":\"json\",\"version\":1}"
PROTOCOL_DELIMITER = '\n'
DEFAULT_DAEMON_ADDRESS = '127.0.0.1:2000'


class UDPEmitter:
    """
    The default emitter the X-Ray recorder uses to send segments/subsegments
    to the X-Ray daemon over UDP using a non-blocking socket. If there is an
    exception on the actual data transfer between the socket and the daemon,
    it logs the exception and continue.
    """
    def __init__(self, daemon_address=DEFAULT_DAEMON_ADDRESS):

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setblocking(0)
        self.set_daemon_address(daemon_address)

    def send_entity(self, entity):
        """
        Serializes a segment/subsegment and sends it to the X-Ray daemon
        over UDP. By default it doesn't retry on failures.

        :param entity: a trace entity to send to the X-Ray daemon
        """
        try:
            message = "%s%s%s" % (PROTOCOL_HEADER,
                                  PROTOCOL_DELIMITER,
                                  entity.serialize())

            log.debug("sending: %s to %s:%s." % (message, self._ip, self._port))
            self._send_data(message)
        except Exception:
            log.exception("Failed to send entity to Daemon.")

    def set_daemon_address(self, address):
        """
        Set up UDP ip and port from the raw daemon address
        string using ``DaemonConfig`` class utlities.
        """
        if address:
            daemon_config = DaemonConfig(address)
            self._ip, self._port = daemon_config.udp_ip, daemon_config.udp_port

    @property
    def ip(self):
        return self._ip

    @property
    def port(self):
        return self._port

    def _send_data(self, data):
        self._socket.sendto(data.encode('utf-8'), (self._ip, self._port))

    def _parse_address(self, daemon_address):
        try:
            val = daemon_address.split(':')
            return val[0], int(val[1])
        except Exception:
            raise InvalidDaemonAddressException('Invalid daemon address %s specified.' % daemon_address)
