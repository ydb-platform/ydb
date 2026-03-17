# Copyright (c) 2023, Menno Smits
# Released subject to the New BSD License
# Please see http://en.wikipedia.org/wiki/BSD_licenses

"""
This module contains IMAPClient's functionality related to Transport
Layer Security (TLS a.k.a. SSL).
"""

import imaplib
import socket
import ssl
from typing import Optional


def wrap_socket(
    sock: socket.socket, ssl_context: Optional[ssl.SSLContext], host: str
) -> socket.socket:
    if ssl_context is None:
        ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)

    return ssl_context.wrap_socket(sock, server_hostname=host)


class IMAP4_TLS(imaplib.IMAP4):
    """IMAP4 client class for TLS/SSL connections.

    Adapted from imaplib.IMAP4_SSL.
    """

    def __init__(
        self,
        host: str,
        port: int = 993,
        ssl_context: Optional[ssl.SSLContext] = None,
        timeout: Optional[float] = None,
    ):
        self.ssl_context = ssl_context
        self._timeout = timeout
        super().__init__(host, port)

    def _create_socket(self, timeout: Optional[float]) -> socket.socket:
        sock = socket.create_connection((self.host, self.port), timeout=timeout)

        return wrap_socket(sock, self.ssl_context, self.host)
