#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import random
from hashlib import md5, sha1

from pyasn1.type import univ
from pysnmp.proto import errind, error
from pysnmp.proto.secmod.rfc3414 import localkey
from pysnmp.proto.secmod.rfc3414.auth import hmacmd5, hmacsha
from pysnmp.proto.secmod.rfc3414.priv import base
from pysnmp.proto.secmod.rfc7860.auth import hmacsha2

PysnmpCryptoError = False
try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives.ciphers import algorithms, Cipher, modes

except ImportError:
    PysnmpCryptoError = True


random.seed()


# RFC3826

#


class Aes(base.AbstractEncryptionService):
    """AES encryption protocol.

    https://tools.ietf.org/html/rfc3826
    """

    SERVICE_ID = (1, 3, 6, 1, 6, 3, 10, 1, 2, 4)  # usmAesCfb128Protocol
    KEY_SIZE = 16
    local_int = random.randrange(0, 0xFFFFFFFFFFFFFFFF)

    # 3.1.2.1
    def __get_encryption_key(self, privKey, snmpEngineBoots, snmpEngineTime):
        salt = [
            self.local_int >> 56 & 0xFF,
            self.local_int >> 48 & 0xFF,
            self.local_int >> 40 & 0xFF,
            self.local_int >> 32 & 0xFF,
            self.local_int >> 24 & 0xFF,
            self.local_int >> 16 & 0xFF,
            self.local_int >> 8 & 0xFF,
            self.local_int & 0xFF,
        ]

        if self.local_int == 0xFFFFFFFFFFFFFFFF:
            self.local_int = 0
        else:
            self.local_int += 1

        return self.__get_decryption_key(
            privKey, snmpEngineBoots, snmpEngineTime, salt
        ) + (univ.OctetString(salt).asOctets(),)

    def __get_decryption_key(self, privKey, snmpEngineBoots, snmpEngineTime, salt):
        snmpEngineBoots, snmpEngineTime, salt = (
            int(snmpEngineBoots),
            int(snmpEngineTime),
            salt,
        )

        iv = [
            snmpEngineBoots >> 24 & 0xFF,
            snmpEngineBoots >> 16 & 0xFF,
            snmpEngineBoots >> 8 & 0xFF,
            snmpEngineBoots & 0xFF,
            snmpEngineTime >> 24 & 0xFF,
            snmpEngineTime >> 16 & 0xFF,
            snmpEngineTime >> 8 & 0xFF,
            snmpEngineTime & 0xFF,
        ] + salt

        return privKey[: self.KEY_SIZE].asOctets(), univ.OctetString(iv).asOctets()

    def hash_passphrase(self, authProtocol, privKey) -> univ.OctetString:
        """Hash a passphrase."""
        if authProtocol == hmacmd5.HmacMd5.SERVICE_ID:
            hashAlgo = md5
        elif authProtocol == hmacsha.HmacSha.SERVICE_ID:
            hashAlgo = sha1
        elif authProtocol in hmacsha2.HmacSha2.HASH_ALGORITHM:
            hashAlgo = hmacsha2.HmacSha2.HASH_ALGORITHM[authProtocol]
        else:
            raise error.ProtocolError(f"Unknown auth protocol {authProtocol}")
        return localkey.hash_passphrase(privKey, hashAlgo)

    def localize_key(self, authProtocol, privKey, snmpEngineID) -> univ.OctetString:
        """Localize a key."""
        if authProtocol == hmacmd5.HmacMd5.SERVICE_ID:
            hashAlgo = md5
        elif authProtocol == hmacsha.HmacSha.SERVICE_ID:
            hashAlgo = sha1
        elif authProtocol in hmacsha2.HmacSha2.HASH_ALGORITHM:
            hashAlgo = hmacsha2.HmacSha2.HASH_ALGORITHM[authProtocol]
        else:
            raise error.ProtocolError(f"Unknown auth protocol {authProtocol}")
        localPrivKey = localkey.localize_key(privKey, snmpEngineID, hashAlgo)
        return localPrivKey[: self.KEY_SIZE]

    # 3.2.4.1
    def encrypt_data(self, encryptKey, privParameters, dataToEncrypt):
        """Encrypt data."""
        if PysnmpCryptoError:
            raise error.StatusInformation(errorIndication=errind.encryptionError)

        snmpEngineBoots, snmpEngineTime, salt = privParameters

        # 3.3.1.1
        aesKey, iv, salt = self.__get_encryption_key(
            encryptKey, snmpEngineBoots, snmpEngineTime
        )

        # 3.3.1.3
        try:
            aes = _cryptography_cipher(aesKey, iv).encryptor()
            ciphertext = aes.update(dataToEncrypt) + aes.finalize()

        except AttributeError:
            raise error.StatusInformation(
                errorIndication=errind.unsupportedPrivProtocol
            )

        # 3.3.1.4
        return univ.OctetString(ciphertext), univ.OctetString(salt)

    # 3.2.4.2
    def decrypt_data(self, decryptKey, privParameters, encryptedData):
        """Decrypt data."""
        if PysnmpCryptoError:
            raise error.StatusInformation(errorIndication=errind.decryptionError)

        snmpEngineBoots, snmpEngineTime, salt = privParameters

        # 3.3.2.1
        if len(salt) != 8:
            raise error.StatusInformation(errorIndication=errind.decryptionError)

        # 3.3.2.3
        aesKey, iv = self.__get_decryption_key(
            decryptKey, snmpEngineBoots, snmpEngineTime, salt
        )

        try:
            # 3.3.2.4-6
            aes = _cryptography_cipher(aesKey, iv).decryptor()
            return aes.update(encryptedData.asOctets()) + aes.finalize()

        except AttributeError:
            raise error.StatusInformation(
                errorIndication=errind.unsupportedPrivProtocol
            )


def _cryptography_cipher(key, iv):
    """Build a cryptography AES Cipher object.

    :param bytes key: Encryption key
    :param bytes iv: Initialization vector
    :returns: AES Cipher instance
    :rtype: cryptography.hazmat.primitives.ciphers.Cipher
    """
    return Cipher(
        algorithm=algorithms.AES(key), mode=modes.CFB(iv), backend=default_backend()
    )
