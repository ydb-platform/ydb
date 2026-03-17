#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pysnmp.proto import errind, error


class AbstractAuthenticationService:
    """Abstract base class for SNMPv3 authentication services."""

    SERVICE_ID = None

    def hash_passphrase(self, authKey):
        """Hash authentication key."""
        raise error.ProtocolError(errind.noAuthentication)

    def localize_key(self, authKey, snmpEngineID):
        """Localize authentication key."""
        raise error.ProtocolError(errind.noAuthentication)

    @property
    def digest_length(self):
        """Return length of the digest produced by this service."""
        raise error.ProtocolError(errind.noAuthentication)

    # 7.2.4.1
    def authenticate_outgoing_message(self, authKey, wholeMsg):
        """Authenticate outgoing message."""
        raise error.ProtocolError(errind.noAuthentication)

    # 7.2.4.2
    def authenticate_incoming_message(self, authKey, authParameters, wholeMsg):
        """Authenticate incoming message."""
        raise error.ProtocolError(errind.noAuthentication)
