#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
#
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pysnmp.proto import errind, error
from pysnmp.proto.secmod.rfc3414.auth import base


class NoAuth(base.AbstractAuthenticationService):
    """NoAuth authentication service.

    This service does not provide any authentication.
    """

    SERVICE_ID = (1, 3, 6, 1, 6, 3, 10, 1, 1, 1)  # usmNoAuthProtocol

    def hash_passphrase(self, authKey):
        """Hash a passphrase."""
        return

    def localize_key(self, authKey, snmpEngineID):
        """Localize a key."""
        return

    # 7.2.4.2
    def authenticate_outgoing_message(self, authKey, wholeMsg):
        """Authenticate outgoing message."""
        raise error.StatusInformation(errorIndication=errind.noAuthentication)

    def authenticate_incoming_message(self, authKey, authParameters, wholeMsg):
        """Authenticate incoming message."""
        raise error.StatusInformation(errorIndication=errind.noAuthentication)
