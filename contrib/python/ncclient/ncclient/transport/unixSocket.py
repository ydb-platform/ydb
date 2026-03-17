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
import threading
from io import BytesIO as StringIO

from socket import AF_UNIX, SOCK_STREAM

from ncclient.capabilities import Capabilities
from ncclient.logging_ import SessionLoggerAdapter
from ncclient.transport.errors import UnixSocketError
from ncclient.transport.session import Session
from ncclient.transport.parser import DefaultXMLParser

logger = logging.getLogger("ncclient.transport.unix")

DEFAULT_TIMEOUT = 120

BUF_SIZE = 4096

class UnixSocketSession(Session):

    "Implements a NETCONF Session over Unix Socket on local machine."

    def __init__(self, device_handler):
        capabilities = Capabilities(device_handler.get_capabilities())
        Session.__init__(self, capabilities)
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
        return super(UnixSocketSession, self)._dispatch_message(raw)

    def close(self):
        self._closing.set()
        self._socket.close()
        self._connected = False

    def connect(self, path=None, timeout=DEFAULT_TIMEOUT):
        sock = socket.socket(AF_UNIX, SOCK_STREAM)
        sock.settimeout(timeout)

        try:
            sock.connect(path)
        except Exception:
            raise UnixSocketError("Could not connect to %s" % path)

        self._socket = sock
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
