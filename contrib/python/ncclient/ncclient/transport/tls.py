# Copyright (c) Siemens AG, 2022
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import socket
import sys
import threading

from io import BytesIO as StringIO
from socket import AF_INET, SOCK_STREAM
from ssl import CERT_REQUIRED, SSLContext, SSLError

from ncclient.capabilities import Capabilities
from ncclient.logging_ import SessionLoggerAdapter
from ncclient.transport.errors import TLSError
from ncclient.transport.session import Session
from ncclient.transport.parser import DefaultXMLParser

logger = logging.getLogger("ncclient.transport.tls")

DEFAULT_TLS_NETCONF_PORT = 6513
DEFAULT_TLS_TIMEOUT = 120

BUF_SIZE = 4096


class TLSSession(Session):

    def __init__(self, device_handler):
        capabilities = Capabilities(device_handler.get_capabilities())
        Session.__init__(self, capabilities)
        self._host = None
        self._connected = False
        self._socket = None
        self._buffer = StringIO()
        self._device_handler = device_handler
        self._message_list = []
        self._closing = threading.Event()
        self.parser = DefaultXMLParser(self)
        self.logger = SessionLoggerAdapter(logger, {'session': self})

    def _dispatch_message(self, raw):
        self.logger.info("Received message from host")
        self.logger.debug("Received:\n%s", raw)
        return super(TLSSession, self)._dispatch_message(raw)

    def close(self):
        self._closing.set()
        self._socket.close()
        self._connected = False

    def connect(self, host=None, port=DEFAULT_TLS_NETCONF_PORT,
                keyfile=None, certfile=None, ca_certs=None,
                protocol=None, check_hostname=True, server_hostname=None,
                timeout=DEFAULT_TLS_TIMEOUT):
        """Establish NETCONF session via TLS.

        :param host: Hostname or IP address to connect to.
        :param port: Port number which would be used for connection, the
            default value is 6513 (`DEFAULT_TLS_NETCONF_PORT`).
        :param certfile: Points to file in PEM format with client's certificate
            and optionally with client's private key.
        :param keyfile: Points to file with client's private key in PEM format,
            if the latter is not in certfile.
        :param ca_certs: Points to file containing a list of certificates,
            which should be used to verify server certificate.
        :param protocol: Protocol version to use. Should be either
            ssl.PROTOCOL_TLS_CLIENT for Python 3.6+, or ssl.PROTOCOL_TLS
            for older versions.
        :param check_hostname: If set to True, perform verification of the
            hostname during the handshake.
        :param server_hostname: If set, is used upon hostname checking instead
            of the `host` parameter.
        :param timeout: Specifies the connection timeout, defaults to
            120 seconds (`DEFAULT_TLS_TIMEOUT`).

        :raise TLSError if the connection can not be established.
        """
        if host is None:
            raise TLSError('Missing host')
        if certfile is None:
            raise TLSError('Missing client certificate file')
        if protocol is None:
            raise TLSError('Missing TLS protocol')

        ssl_context = SSLContext(protocol)
        ssl_context.verify_mode = CERT_REQUIRED
        ssl_context.check_hostname = check_hostname
        try:
            ssl_context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        except SSLError:
            raise TLSError('Bad client private key / certificate pair')
        except IOError:
            raise TLSError('Private key / certificate pair not found')

        if ca_certs:
            try:
                ssl_context.load_verify_locations(cafile=ca_certs)
            except SSLError:
                raise TLSError('Bad Certification Authority file')
            except IOError:
                raise TLSError('CA certificate file not found')

        sock = socket.socket(AF_INET, SOCK_STREAM)
        ssl_sock = ssl_context.wrap_socket(
            sock, do_handshake_on_connect=False,
            server_hostname=server_hostname or host)
        ssl_sock.settimeout(timeout)

        try:
            ssl_sock.connect((host, port))
        except Exception:
            raise TLSError("Could not connect to %s:%s" % (host, port))

        try:
            ssl_sock.do_handshake()
        except Exception:
            raise TLSError("Unsuccessful TLS handshake with %s:%s" % (host, port))

        self._host = host
        self._socket = ssl_sock
        self._connected = True
        self._post_connect()

    def _transport_read(self):
        return self._socket.recv(BUF_SIZE)

    def _transport_write(self, data):
        return self._socket.send(data)

    def _transport_register(self, selector, event):
        selector.register(self._socket, event)

    def _send_ready(self):
        # In contrast to Paramiko's `Channel`, pure python sockets do not
        # expose `send_ready()` function.
        return True

    @property
    def host(self):
        """Host this session is connected to, or None if not connected."""
        if hasattr(self, '_host'):
            return self._host
        return None
