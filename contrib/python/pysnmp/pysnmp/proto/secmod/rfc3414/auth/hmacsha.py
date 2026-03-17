#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from hashlib import sha1

from pyasn1.type import univ
from pysnmp.proto import errind, error
from pysnmp.proto.secmod.rfc3414 import localkey
from pysnmp.proto.secmod.rfc3414.auth import base

TWELVE_ZEROS = univ.OctetString((0,) * 12).asOctets()
FORTY_FOUR_ZEROS = (0,) * 44


# 7.2.4


class HmacSha(base.AbstractAuthenticationService):
    """The HMAC-SHA-1-96 authentication protocol.

    https://tools.ietf.org/html/rfc3414#section-7.2.4
    """

    SERVICE_ID = (1, 3, 6, 1, 6, 3, 10, 1, 1, 3)  # usmHMACSHAAuthProtocol
    IPAD = [0x36] * 64
    OPAD = [0x5C] * 64

    def hash_passphrase(self, authKey) -> univ.OctetString:
        """Hash a passphrase."""
        return localkey.hash_passphrase_sha(authKey)

    def localize_key(self, authKey, snmpEngineID) -> univ.OctetString:
        """Localize a key."""
        return localkey.localize_key_sha(authKey, snmpEngineID)

    @property
    def digest_length(self):
        """Return digest length."""
        return 12

    # 7.3.1
    def authenticate_outgoing_message(self, authKey, wholeMsg):
        """Authenticate outgoing message."""
        # 7.3.1.1
        # Here we expect calling secmod to indicate where the digest
        # should be in the substrate. Also, it pre-sets digest placeholder
        # so we hash wholeMsg out of the box.
        # Yes, that's ugly but that's rfc...
        value = wholeMsg.find(TWELVE_ZEROS)
        if value == -1:
            raise error.ProtocolError("Cant locate digest placeholder")
        wholeHead = wholeMsg[:value]
        wholeTail = wholeMsg[value + 12 :]

        # 7.3.1.2a
        extendedAuthKey = authKey.asNumbers() + FORTY_FOUR_ZEROS

        # 7.3.1.2b -- no-op

        # 7.3.1.2c
        k1 = univ.OctetString(map(lambda x, y: x ^ y, extendedAuthKey, self.IPAD))

        # 7.3.1.2d -- no-op

        # 7.3.1.2e
        k2 = univ.OctetString(map(lambda x, y: x ^ y, extendedAuthKey, self.OPAD))

        # 7.3.1.3
        d1 = sha1(k1.asOctets() + wholeMsg).digest()

        # 7.3.1.4
        d2 = sha1(k2.asOctets() + d1).digest()
        mac = d2[:12]

        # 7.3.1.5 & 6
        return wholeHead + mac + wholeTail

    # 7.3.2
    def authenticate_incoming_message(self, authKey, authParameters, wholeMsg):
        """Authenticate incoming message."""
        # 7.3.2.1 & 2
        if len(authParameters) != 12:
            raise error.StatusInformation(errorIndication=errind.authenticationError)

        # 7.3.2.3
        value = wholeMsg.find(authParameters.asOctets())
        if value == -1:
            raise error.ProtocolError("Cant locate digest in wholeMsg")
        wholeHead = wholeMsg[:value]
        wholeTail = wholeMsg[value + 12 :]
        authenticatedWholeMsg = wholeHead + TWELVE_ZEROS + wholeTail

        # 7.3.2.4a
        extendedAuthKey = authKey.asNumbers() + FORTY_FOUR_ZEROS

        # 7.3.2.4b --> no-op

        # 7.3.2.4c
        k1 = univ.OctetString(map(lambda x, y: x ^ y, extendedAuthKey, self.IPAD))

        # 7.3.2.4d --> no-op

        # 7.3.2.4e
        k2 = univ.OctetString(map(lambda x, y: x ^ y, extendedAuthKey, self.OPAD))

        # 7.3.2.5a
        d1 = sha1(k1.asOctets() + authenticatedWholeMsg).digest()

        # 7.3.2.5b
        d2 = sha1(k2.asOctets() + d1).digest()

        # 7.3.2.5c
        mac = d2[:12]

        # 7.3.2.6
        if mac != authParameters:
            raise error.StatusInformation(errorIndication=errind.authenticationFailure)

        return authenticatedWholeMsg
