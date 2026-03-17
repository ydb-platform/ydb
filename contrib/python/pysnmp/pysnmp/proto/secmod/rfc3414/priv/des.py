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
    from cryptography.hazmat.decrepit.ciphers import algorithms
    from cryptography.hazmat.primitives.ciphers import Cipher, modes
except ImportError:
    PysnmpCryptoError = True

random.seed()


# 8.2.4


class Des(base.AbstractEncryptionService):
    """DES for USM (RFC3414)."""

    SERVICE_ID = (1, 3, 6, 1, 6, 3, 10, 1, 2, 2)  # usmDESPrivProtocol
    KEY_SIZE = 16

    local_int = random.randrange(0, 0xFFFFFFFF)

    def hash_passphrase(self, authProtocol, privKey) -> univ.OctetString:
        """Hash a passphrase.

        This method hashes a passphrase using the authentication protocol.

        Parameters
        ----------
        authProtocol : tuple
            The authentication protocol OID.
        privKey : bytes
            The privacy key.

        Returns
        -------
        bytes
            The hashed passphrase.
        """
        if authProtocol == hmacmd5.HmacMd5.SERVICE_ID:
            hashAlgo = md5
        elif authProtocol == hmacsha.HmacSha.SERVICE_ID:
            hashAlgo = sha1
        elif authProtocol in hmacsha2.HmacSha2.HASH_ALGORITHM:
            hashAlgo = hmacsha2.HmacSha2.HASH_ALGORITHM[authProtocol]
        else:
            raise error.ProtocolError(f"Unknown auth protocol {authProtocol}")
        return localkey.hash_passphrase(privKey, hashAlgo)

    def localize_key(self, authProtocol, privKey, snmpEngineID) -> bytes:
        """Localize privacy key.

        This method localizes privacy key using the authentication protocol
        and the authoritative SNMP engine ID.

        Parameters
        ----------
        authProtocol : tuple
            The authentication protocol OID.
        privKey : bytes
            The privacy key.
        snmpEngineID : bytes
            The authoritative SNMP engine ID.

        Returns
        -------
        bytes
            The localized privacy key.
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
        return localPrivKey[: self.KEY_SIZE]

    # 8.1.1.1
    def __get_encryption_key(self, privKey, snmpEngineBoots):
        desKey = privKey[:8]
        preIV = privKey[8:16]

        securityEngineBoots = int(snmpEngineBoots)

        salt = [
            securityEngineBoots >> 24 & 0xFF,
            securityEngineBoots >> 16 & 0xFF,
            securityEngineBoots >> 8 & 0xFF,
            securityEngineBoots & 0xFF,
            self.local_int >> 24 & 0xFF,
            self.local_int >> 16 & 0xFF,
            self.local_int >> 8 & 0xFF,
            self.local_int & 0xFF,
        ]
        if self.local_int == 0xFFFFFFFF:
            self.local_int = 0
        else:
            self.local_int += 1

        return (
            desKey.asOctets(),
            univ.OctetString(salt).asOctets(),
            univ.OctetString(
                map(lambda x, y: x ^ y, salt, preIV.asNumbers())
            ).asOctets(),
        )

    @staticmethod
    def __get_decryption_key(privKey, salt):
        return (
            privKey[:8].asOctets(),
            univ.OctetString(
                map(lambda x, y: x ^ y, salt.asNumbers(), privKey[8:16].asNumbers())
            ).asOctets(),
        )

    # 8.2.4.1
    def encrypt_data(self, encryptKey, privParameters, dataToEncrypt):
        """Encrypt data."""
        if PysnmpCryptoError:
            raise error.StatusInformation(errorIndication=errind.encryptionError)

        snmpEngineBoots, snmpEngineTime, salt = privParameters

        # 8.3.1.1
        desKey, salt, iv = self.__get_encryption_key(encryptKey, snmpEngineBoots)

        # 8.3.1.2
        privParameters = univ.OctetString(salt)

        # 8.1.1.2
        plaintext = (
            dataToEncrypt
            + univ.OctetString((0,) * (8 - len(dataToEncrypt) % 8)).asOctets()
        )

        try:
            des = _cryptography_cipher(desKey, iv).encryptor()
            ciphertext = des.update(plaintext) + des.finalize()

        except AttributeError:
            raise error.StatusInformation(
                errorIndication=errind.unsupportedPrivProtocol
            )

        # 8.3.1.3 & 4
        return univ.OctetString(ciphertext), privParameters

    # 8.2.4.2
    def decrypt_data(self, decryptKey, privParameters, encryptedData):
        """Decrypt data."""
        if PysnmpCryptoError:
            raise error.StatusInformation(errorIndication=errind.decryptionError)

        snmpEngineBoots, snmpEngineTime, salt = privParameters

        # 8.3.2.1
        if len(salt) != 8:
            raise error.StatusInformation(errorIndication=errind.decryptionError)

        # 8.3.2.2 no-op

        # 8.3.2.3
        desKey, iv = self.__get_decryption_key(decryptKey, salt)

        # 8.3.2.4 -> 8.1.1.3
        if len(encryptedData) % 8 != 0:
            raise error.StatusInformation(errorIndication=errind.decryptionError)

        try:
            # 8.3.2.6
            des = _cryptography_cipher(desKey, iv).decryptor()
            return des.update(encryptedData.asOctets()) + des.finalize()

        except AttributeError:
            raise error.StatusInformation(
                errorIndication=errind.unsupportedPrivProtocol
            )


def _cryptography_cipher(key, iv):
    """Build a cryptography DES(-like) Cipher object.

    .. note::

        pyca/cryptography does not support DES directly because it is a
        seriously old, insecure, and deprecated algorithm. However,
        triple DES is just three rounds of DES (encrypt, decrypt, encrypt)
        done by taking a key three times the size of a DES key and breaking
        it into three pieces. So triple DES with des_key * 3 is equivalent
        to DES.

    :param bytes key: Encryption key
    :param bytes iv: Initialization vector
    :returns: TripleDES Cipher instance providing DES behavior by using
        provided DES key
    :rtype: cryptography.hazmat.primitives.ciphers.Cipher
    """
    return Cipher(
        algorithm=algorithms.TripleDES(key * 3),
        mode=modes.CBC(iv),
        backend=default_backend(),
    )
