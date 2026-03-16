# SPDX-FileCopyrightText: 2013 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

from ._core import parse, parse_file
from ._object_types import (
    AbstractPEMObject,
    Certificate,
    CertificateRequest,
    CertificateRevocationList,
    DHParameters,
    DSAPrivateKey,
    ECPrivateKey,
    Key,
    OpenPGPPrivateKey,
    OpenPGPPublicKey,
    OpenSSHPrivateKey,
    OpenSSLTrustedCertificate,
    PrivateKey,
    PublicKey,
    RSAPrivateKey,
    RSAPublicKey,
    SSHCOMPrivateKey,
    SSHPublicKey,
)


try:
    from . import twisted
except ImportError:
    twisted = None  # type: ignore[assignment]

__all__ = [
    "AbstractPEMObject",
    "Certificate",
    "CertificateRequest",
    "CertificateRevocationList",
    "DHParameters",
    "DSAPrivateKey",
    "ECPrivateKey",
    "Key",
    "OpenPGPPrivateKey",
    "OpenPGPPublicKey",
    "OpenSSHPrivateKey",
    "OpenSSLTrustedCertificate",
    "parse_file",
    "parse",
    "PrivateKey",
    "PublicKey",
    "RSAPrivateKey",
    "RSAPublicKey",
    "SSHCOMPrivateKey",
    "SSHPublicKey",
    "twisted",
]

__author__ = "Hynek Schlawack"
__license__ = "MIT"


def __getattr__(name: str) -> str:
    dunder_to_metadata = {
        "__version__": "version",
        "__description__": "summary",
        "__uri__": "",
        "__url__": "",
        "__email__": "",
    }
    if name not in dunder_to_metadata.keys():
        msg = f"module {__name__} has no attribute {name}"
        raise AttributeError(msg)

    import warnings

    from importlib.metadata import metadata

    if name != "__version__":
        warnings.warn(
            f"Accessing pem.{name} is deprecated and will be "
            "removed in a future release. Use importlib.metadata directly "
            "to query packaging metadata.",
            DeprecationWarning,
            stacklevel=2,
        )

    meta = metadata("pem")

    if name in ("__uri__", "__url__"):
        return meta["Project-URL"].split(" ", 1)[-1]

    if name == "__email__":
        return meta["Author-email"].split("<", 1)[1].rstrip(">")

    return meta[dunder_to_metadata[name]]
