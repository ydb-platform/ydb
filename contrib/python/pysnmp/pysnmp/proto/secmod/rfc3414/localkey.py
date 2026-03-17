#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from hashlib import md5, sha1

from pyasn1.type import univ


def hash_passphrase(passphrase, hashFunc) -> univ.OctetString:
    """Return hash of passphrase using hashFunc hash function."""
    passphrase = univ.OctetString(passphrase).asOctets()
    # noinspection PyDeprecation,PyCallingNonCallable
    hasher = hashFunc()
    ringBuffer = passphrase * (64 // len(passphrase) + 1)
    # noinspection PyTypeChecker
    ringBufferLen = len(ringBuffer)
    count = 0
    mark = 0
    while count < 16384:
        e = mark + 64
        if e < ringBufferLen:
            hasher.update(ringBuffer[mark:e])
            mark = e
        else:
            hasher.update(
                ringBuffer[mark:ringBufferLen] + ringBuffer[0 : e - ringBufferLen]
            )
            mark = e - ringBufferLen
        count += 1
    digest = hasher.digest()
    return univ.OctetString(digest)


def password_to_key(passphrase, snmpEngineId, hashFunc) -> univ.OctetString:
    """Return key from password."""
    return localize_key(hash_passphrase(passphrase, hashFunc), snmpEngineId, hashFunc)


def localize_key(passKey, snmpEngineId, hashFunc) -> univ.OctetString:
    """Localize passKey with snmpEngineId using hashFunc hash function."""
    passKey = univ.OctetString(passKey).asOctets()
    # noinspection PyDeprecation,PyCallingNonCallable
    digest = hashFunc(passKey + snmpEngineId.asOctets() + passKey).digest()
    return univ.OctetString(digest)


# RFC3414: A.2.1
def hash_passphrase_md5(passphrase) -> univ.OctetString:
    """Return MD5 hash of passphrase."""
    return hash_passphrase(passphrase, md5)


# RFC3414: A.2.2
def hash_passphrase_sha(passphrase) -> univ.OctetString:
    """Return SHA-1 hash of passphrase."""
    return hash_passphrase(passphrase, sha1)


def password_to_key_md5(passphrase, snmpEngineId) -> univ.OctetString:
    """Return MD5 key from password."""
    return localize_key(hash_passphrase_md5(passphrase), snmpEngineId, md5)


def password_to_key_sha(passphrase, snmpEngineId) -> univ.OctetString:
    """Return SHA-1 key from password."""
    return localize_key(hash_passphrase_sha(passphrase), snmpEngineId, sha1)


def localize_key_md5(passKey, snmpEngineId) -> univ.OctetString:
    """Localize passKey with snmpEngineId using MD5 hash function."""
    return localize_key(passKey, snmpEngineId, md5)


def localize_key_sha(passKey, snmpEngineId) -> univ.OctetString:
    """Localize passKey with snmpEngineId using SHA1 hash function."""
    return localize_key(passKey, snmpEngineId, sha1)
