# SPDX-FileCopyrightText: 2013 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

"""
All supported PEM object types.
"""

from __future__ import annotations

import hashlib

from abc import ABCMeta
from base64 import b64decode
from functools import cached_property
from typing import ClassVar


class AbstractPEMObject(metaclass=ABCMeta):
    """
    Base class for parsed objects.
    """

    _pattern: ClassVar[tuple[bytes, ...]] = NotImplemented

    _pem_bytes: bytes

    def __init__(self, pem_bytes: bytes | str):
        self._pem_bytes = (
            pem_bytes.encode("ascii")
            if isinstance(pem_bytes, str)
            else pem_bytes
        )

        self._sha1_hexdigest = None

    def __str__(self) -> str:
        """
        Return the PEM-encoded content as a native :obj:`str`.
        """
        return self._pem_bytes.decode("ascii")

    def __repr__(self) -> str:
        return "<{}(PEM string with SHA-1 digest {!r})>".format(
            self.__class__.__name__, self.sha1_hexdigest
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented

        return (
            type(self) == type(other) and self._pem_bytes == other._pem_bytes
        )

    def __hash__(self) -> int:
        return hash(self._pem_bytes)

    @cached_property
    def sha1_hexdigest(self) -> str:
        """
        A SHA-1 digest of the whole object for easy differentiation.

        .. versionadded:: 18.1.0
        .. versionchanged:: 20.1.0

           Carriage returns are removed before hashing to give the same hashes
           on Windows and UNIX-like operating systems.
        """
        return hashlib.sha1(  # noqa[S324]
            self._pem_bytes.replace(b"\r", b"")
        ).hexdigest()

    def as_bytes(self) -> bytes:
        """
        Return the PEM-encoded content as :obj:`bytes`.

        .. versionadded:: 16.1.0
        """
        return self._pem_bytes

    def as_text(self) -> str:
        """
        Return the PEM-encoded content as text.

        .. versionadded:: 18.1.0
        """
        return self._pem_bytes.decode("utf-8")

    @cached_property
    def bytes_payload(self) -> bytes:
        """
        The payload of the PEM-encoded content.

        Possible PEM headers are removed.

        .. versionadded:: 23.1.0
        """
        return b"".join(
            line
            for line in self._pem_bytes.splitlines()[1:-1]
            if b":" not in line  # remove headers
        )

    @cached_property
    def text_payload(self) -> str:
        """
        The payload of the PEM-encoded content.

        Possible PEM headers are removed.

        .. versionadded:: 23.1.0
        """
        return self.bytes_payload.decode("utf-8")

    @cached_property
    def decoded_payload(self) -> bytes:
        """
        The base64-decoded payload of the PEM-encoded content.

        Possible PEM headers are removed.

        .. versionadded:: 23.1.0
        """
        return b64decode(self.bytes_payload)

    @cached_property
    def meta_headers(self) -> dict[str, str]:
        """
        Return a dictionary of payload headers.

        If the value of a header is quoted, the quotes are removed.

        .. versionadded:: 23.1.0
        """
        expl = {}
        for line in self._pem_bytes.decode().splitlines()[1:-1]:
            if ":" not in line:
                break

            key, val = line.split(": ", 1)

            # Strip quotes if they're only at the beginning and end.
            if val.count('"') == 2 and val[0] == '"' and val[-1] == '"':
                val = val[1:-1]

            expl[key] = val
        else:
            # XXX: necessary for Coverage.py!? This can't happen with non-empty
            # PEM objects.
            pass  # pragma: no cover

        return expl


class Certificate(AbstractPEMObject):
    """
    A certificate.
    """

    _pattern = (b"CERTIFICATE",)


class OpenSSLTrustedCertificate(Certificate):
    """
    An OpenSSL "trusted certificate".

    .. versionadded:: 21.2.0
    """

    _pattern = (b"TRUSTED CERTIFICATE",)


class CertificateRequest(AbstractPEMObject):
    """
    A certificate signing request.

    .. versionadded:: 17.1.0
    """

    _pattern = (b"NEW CERTIFICATE REQUEST", b"CERTIFICATE REQUEST")


class CertificateRevocationList(AbstractPEMObject):
    """
    A certificate revocation list.

    .. versionadded:: 18.2.0
    """

    _pattern = (b"X509 CRL",)


class Key(AbstractPEMObject):
    """
    A key of unknown type.
    """

    # Key is special-cased later and is kind of abstract.


class PrivateKey(Key):
    """
    A private key of unknown type.

    .. versionadded:: 19.1.0
    """

    _pattern: ClassVar[tuple[bytes, ...]] = (
        b"PRIVATE KEY",
        b"ENCRYPTED PRIVATE KEY",
    )


class PublicKey(Key):
    """
    A public key of unknown type.

    .. versionadded:: 19.1.0
    """

    _pattern = (b"PUBLIC KEY",)


class RSAPrivateKey(PrivateKey):
    """
    A private RSA key.
    """

    _pattern = (b"RSA PRIVATE KEY",)


class RSAPublicKey(PublicKey):
    """
    A public RSA key.

    .. versionadded:: 19.1.0
    """

    _pattern = (b"RSA PUBLIC KEY",)


class ECPrivateKey(PrivateKey):
    """
    A private EC key.

    .. versionadded:: 19.2.0
    """

    _pattern = (b"EC PRIVATE KEY",)


class DSAPrivateKey(PrivateKey):
    """
    A private DSA key.

    Also private DSA key in OpenSSH legacy PEM format.

    .. versionadded:: 21.1.0
    """

    _pattern = (b"DSA PRIVATE KEY",)


class DHParameters(AbstractPEMObject):
    """
    Diffie-Hellman parameters for DHE.
    """

    _pattern = (b"DH PARAMETERS",)


class OpenSSHPrivateKey(PrivateKey):
    """
    OpenSSH private key format

    .. versionadded:: 19.3.0
    """

    _pattern = (b"OPENSSH PRIVATE KEY",)


class SSHPublicKey(PublicKey):
    """
    A public key in SSH :rfc:`4716` format.

    The Secure Shell (SSH) Public Key File Format.

    .. versionadded:: 21.1.0
    """

    _pattern = (b"SSH2 PUBLIC KEY",)


class SSHCOMPrivateKey(PrivateKey):
    """
    A private key in SSH.COM / Tectia format.

    .. versionadded:: 21.1.0
    """

    _pattern = (b"SSH2 ENCRYPTED PRIVATE KEY",)


class OpenPGPPublicKey(PublicKey):
    """
    An :rfc:`4880` armored OpenPGP public key.

    .. versionadded:: 23.1.0
    """

    _pattern = (b"PGP PUBLIC KEY BLOCK",)


class OpenPGPPrivateKey(PrivateKey):
    """
    An :rfc:`4880` armored OpenPGP private key.

    .. versionadded:: 23.1.0
    """

    _pattern = (b"PGP PRIVATE KEY BLOCK",)
