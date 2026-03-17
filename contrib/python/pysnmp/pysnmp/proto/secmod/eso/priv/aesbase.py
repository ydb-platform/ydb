#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from hashlib import md5, sha1
from math import ceil

from pysnmp.proto import error
from pysnmp.proto.secmod.rfc3414 import localkey
from pysnmp.proto.secmod.rfc3414.auth import hmacmd5, hmacsha
from pysnmp.proto.secmod.rfc3826.priv import aes
from pysnmp.proto.secmod.rfc7860.auth import hmacsha2


class AbstractAesBlumenthal(aes.Aes):
    """AES encryption with standard key localization."""

    SERVICE_ID = ()
    KEY_SIZE = 0

    # 3.1.2.1
    def localize_key(self, authProtocol, privKey, snmpEngineID) -> bytes:
        """AES key localization algorithm.

        This algorithm is used to localize an AES key to an authoritative
        engine ID.

        Parameters
        ----------
        authProtocol : tuple
            The authentication protocol OID.
        privKey : bytes
            The privacy key.
        snmpEngineID : bytes
            The authoritative engine ID.

        Returns
        -------
        bytes
            The localized key.
        """
        if authProtocol == hmacmd5.HmacMd5.SERVICE_ID:
            hashAlgo = md5
        elif authProtocol == hmacsha.HmacSha.SERVICE_ID:
            hashAlgo = sha1
        elif authProtocol in hmacsha2.HmacSha2.HASH_ALGORITHM:
            hashAlgo = hmacsha2.HmacSha2.HASH_ALGORITHM[authProtocol]
        else:
            raise error.ProtocolError(f"Unknown auth protocol {authProtocol}")

        localPrivKey = localkey.localize_key(privKey, snmpEngineID, hashAlgo)

        # now extend this key if too short by repeating steps that includes the hashPassphrase step
        for count in range(1, int(ceil(self.KEY_SIZE * 1.0 / len(localPrivKey)))):
            localPrivKey += localPrivKey.clone(
                hashAlgo(localPrivKey.asOctets()).digest()
            )

        return localPrivKey[: self.KEY_SIZE]


class AbstractAesReeder(aes.Aes):
    """AES encryption with non-standard key localization.

    Many vendors (including Cisco) do not use:

    https://tools.itef.org/pdf/draft_bluementhal-aes-usm-04.txt

    for key localization instead, they use the procedure for 3DES key localization
    specified in:

    https://tools.itef.org/pdf/draft_reeder_snmpv3-usm-3desede-00.pdf

    The difference between the two is that the Reeder draft does key extension by repeating
    the steps in the password to key algorithm (hash phrase, then localize with SNMPEngine ID).
    """

    SERVICE_ID = ()
    KEY_SIZE = 0

    # 2.1 of https://tools.itef.org/pdf/draft_bluementhal-aes-usm-04.txt
    def localize_key(self, authProtocol, privKey, snmpEngineID) -> bytes:
        """AES key localization algorithm.

        This algorithm is used to localize an AES key to an authoritative
        engine ID.

        Parameters
        ----------
        authProtocol : tuple
            The authentication protocol OID.
        privKey : bytes
            The privacy key.
        snmpEngineID : bytes
            The authoritative engine ID.

        Returns
        -------
        bytes
            The localized key.
        """
        if authProtocol == hmacmd5.HmacMd5.SERVICE_ID:
            hashAlgo = md5
        elif authProtocol == hmacsha.HmacSha.SERVICE_ID:
            hashAlgo = sha1
        elif authProtocol in hmacsha2.HmacSha2.HASH_ALGORITHM:
            hashAlgo = hmacsha2.HmacSha2.HASH_ALGORITHM[authProtocol]
        else:
            raise error.ProtocolError(f"Unknown auth protocol {authProtocol}")

        localPrivKey = localkey.localize_key(privKey, snmpEngineID, hashAlgo)

        # now extend this key if too short by repeating steps that includes the hashPassphrase step
        while len(localPrivKey) < self.KEY_SIZE:
            # this is the difference between reeder and bluementhal
            newKey = localkey.hash_passphrase(localPrivKey, hashAlgo)
            localPrivKey += localkey.localize_key(newKey, snmpEngineID, hashAlgo)

        return localPrivKey[: self.KEY_SIZE]
