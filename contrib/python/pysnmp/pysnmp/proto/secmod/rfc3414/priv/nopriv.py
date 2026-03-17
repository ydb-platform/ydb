#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pysnmp.proto import errind, error
from pysnmp.proto.secmod.rfc3414.priv import base


class NoPriv(base.AbstractEncryptionService):
    """No privacy protocol.

    This class implements no privacy protocol.
    """

    SERVICE_ID = (1, 3, 6, 1, 6, 3, 10, 1, 2, 1)  # usmNoPrivProtocol

    def hash_passphrase(self, authProtocol, privKey):
        """Hash a passphrase."""
        return

    def localize_key(self, authProtocol, privKey, snmpEngineID):
        """Localize key."""
        return

    def encrypt_data(self, encryptKey, privParameters, dataToEncrypt):
        """Encrypt data."""
        raise error.StatusInformation(errorIndication=errind.noEncryption)

    def decrypt_data(self, decryptKey, privParameters, encryptedData):
        """Decrypt data."""
        raise error.StatusInformation(errorIndication=errind.noEncryption)
