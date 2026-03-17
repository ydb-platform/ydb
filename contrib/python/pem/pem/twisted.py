# SPDX-FileCopyrightText: 2013 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

"""
Twisted-specific convenience helpers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from OpenSSL.crypto import FILETYPE_PEM
from twisted.internet import ssl

from ._core import parse_file
from ._object_types import Certificate, DHParameters, Key


if TYPE_CHECKING:
    from ._object_types import AbstractPEMObject


def certificateOptionsFromPEMs(
    pemObjects: list[AbstractPEMObject], **kw: object
) -> ssl.CertificateOptions:
    """
    Load a CertificateOptions from the given collection of PEM objects
    (already-loaded private keys and certificates).

    In those PEM objects, identify one private key and its corresponding
    certificate to use as the primary certificate.  Then use the rest of the
    certificates found as chain certificates.  Raise a ValueError if no
    certificate matching a private key is found.

    Args:
        pemObjects (list[AbstractPEMObject]): A list of PEM objects to load.

    Returns:
        `twisted.internet.ssl.CertificateOptions`_: A TLS context factory using *pemObjects*

    .. _`twisted.internet.ssl.CertificateOptions`: https://docs.twistedmatrix.com/en/stable/api/twisted.internet.ssl.CertificateOptions.html
    """
    keys = [key for key in pemObjects if isinstance(key, Key)]
    if not len(keys):
        msg = "Supplied PEM file(s) does *not* contain a key."
        raise ValueError(msg)
    if len(keys) > 1:
        msg = "Supplied PEM file(s) contains *more* than one key."
        raise ValueError(msg)

    privateKey = ssl.KeyPair.load(str(keys[0]), FILETYPE_PEM)  # type: ignore[no-untyped-call]

    certs = [cert for cert in pemObjects if isinstance(cert, Certificate)]
    if not len(certs):
        msg = "*At least one* certificate is required."
        raise ValueError(msg)
    certificates = [
        ssl.Certificate.loadPEM(str(certPEM))  # type: ignore[no-untyped-call]
        for certPEM in certs
    ]

    certificatesByFingerprint = {
        certificate.getPublicKey().keyHash(): certificate
        for certificate in certificates
    }

    if privateKey.keyHash() not in certificatesByFingerprint:
        msg = f"No certificate matching {privateKey.keyHash()} found."
        raise ValueError(msg)

    primaryCertificate = certificatesByFingerprint.pop(privateKey.keyHash())

    if "dhParameters" in kw:
        msg = "Passing DH parameters as a keyword argument instead of a PEM object is not supported anymore."
        raise TypeError(msg)

    dhparams = [o for o in pemObjects if isinstance(o, DHParameters)]
    if len(dhparams) > 1:
        msg = "Supplied PEM file(s) contain(s) *more* than one set of DH parameters."
        raise ValueError(msg)

    if len(dhparams) == 1:
        kw["dhParameters"] = ssl.DiffieHellmanParameters(  # type: ignore[no-untyped-call]
            str(dhparams[0])
        )

    return ssl.CertificateOptions(  # type: ignore[no-any-return]
        privateKey=privateKey.original,
        certificate=primaryCertificate.original,
        extraCertChain=[
            chain.original for chain in certificatesByFingerprint.values()
        ],
        **kw,
    )


def certificateOptionsFromFiles(
    *pemFiles: str, **kw: object
) -> ssl.CertificateOptions:
    """
    Read all files named by *pemFiles*, and parse them using
    :func:`certificateOptionsFromPEMs`.

    Args:
        pemFiles (str): All positional arguments are used as filenames to
            read.

    Returns:
        `twisted.internet.ssl.CertificateOptions`_: A TLS context factory using
         PEM objects from *pemFiles*.
    """
    pems: list[AbstractPEMObject] = []
    for pemFile in pemFiles:
        pems += parse_file(pemFile)

    return certificateOptionsFromPEMs(pems, **kw)
