from dataclasses import dataclass
from typing import List, Optional

from asn1crypto import x509

from pyhanko.config import api
from pyhanko.keys import load_certs_from_pemder

__all__ = [
    'PKCS12SignatureConfig',
    'PemDerSignatureConfig',
]


@dataclass(frozen=True)
class PKCS12SignatureConfig(api.ConfigurableMixin):
    """
    Configuration for a signature using key material on disk, contained
    in a PKCS#12 bundle.
    """

    pfx_file: str
    """Path to the PKCS#12 file."""

    other_certs: Optional[List[x509.Certificate]] = None
    """Other relevant certificates."""

    pfx_passphrase: Optional[bytes] = None
    """PKCS#12 passphrase (if relevant)."""

    prompt_passphrase: bool = True
    """
    Prompt for the PKCS#12 passphrase. Default is ``True``.

    .. note::
        If :attr:`key_passphrase` is not ``None``, this setting has no effect.
    """

    prefer_pss: bool = False
    """
    Prefer PSS to PKCS#1 v1.5 padding when creating RSA signatures.
    """

    @classmethod
    def process_entries(cls, config_dict):
        super().process_entries(config_dict)

        other_certs = config_dict.get('other_certs', ())
        if isinstance(other_certs, str):
            other_certs = (other_certs,)
        config_dict['other_certs'] = list(load_certs_from_pemder(other_certs))

        try:
            passphrase = config_dict['pfx_passphrase']
            if passphrase is not None:
                config_dict['pfx_passphrase'] = passphrase.encode('utf8')
        except KeyError:
            pass


@dataclass(frozen=True)
class PemDerSignatureConfig(api.ConfigurableMixin):
    """
    Configuration for a signature using PEM or DER-encoded key material on disk.
    """

    key_file: str
    """Signer's private key."""

    cert_file: str
    """Signer's certificate."""

    other_certs: Optional[List[x509.Certificate]] = None
    """Other relevant certificates."""

    key_passphrase: Optional[bytes] = None
    """Signer's key passphrase (if relevant)."""

    prompt_passphrase: bool = True
    """
    Prompt for the key passphrase. Default is ``True``.

    .. note::
        If :attr:`key_passphrase` is not ``None``, this setting has no effect.
    """

    prefer_pss: bool = False
    """
    Prefer PSS to PKCS#1 v1.5 padding when creating RSA signatures.
    """

    @classmethod
    def process_entries(cls, config_dict):
        super().process_entries(config_dict)

        other_certs = config_dict.get('other_certs', ())
        if isinstance(other_certs, str):
            other_certs = (other_certs,)
        config_dict['other_certs'] = list(load_certs_from_pemder(other_certs))

        try:
            passphrase = config_dict['key_passphrase']
            if passphrase is not None:
                config_dict['key_passphrase'] = passphrase.encode('utf8')
        except KeyError:
            pass
