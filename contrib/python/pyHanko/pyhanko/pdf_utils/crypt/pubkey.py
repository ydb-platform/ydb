import abc
import enum
import logging
import re
import secrets
import struct
from dataclasses import dataclass
from hashlib import sha1, sha256
from typing import Dict, List, Optional, Set, Tuple, Union

from asn1crypto import algos, cms, core, x509
from asn1crypto.algos import RSAESOAEPParams
from asn1crypto.cms import KeyEncryptionAlgorithm, KeyEncryptionAlgorithmId
from asn1crypto.keys import PrivateKeyInfo, PublicKeyAlgorithm, PublicKeyInfo
from cryptography.hazmat.primitives import hashes, keywrap, serialization
from cryptography.hazmat.primitives.asymmetric.ec import (
    ECDH,
    EllipticCurvePrivateKey,
    EllipticCurvePublicKey,
)
from cryptography.hazmat.primitives.asymmetric.ec import (
    generate_private_key as generate_ec_private_key,
)
from cryptography.hazmat.primitives.asymmetric.padding import (
    MGF1,
    OAEP,
    AsymmetricPadding,
    PKCS1v15,
)
from cryptography.hazmat.primitives.asymmetric.rsa import (
    RSAPrivateKey,
    RSAPublicKey,
)
from cryptography.hazmat.primitives.asymmetric.x448 import (
    X448PrivateKey,
    X448PublicKey,
)
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.kdf import KeyDerivationFunction
from cryptography.hazmat.primitives.kdf.x963kdf import X963KDF
from cryptography.hazmat.primitives.serialization import pkcs12

from .. import generic, misc
from ._util import aes_cbc_decrypt, aes_cbc_encrypt, rc4_encrypt
from .api import (
    AuthResult,
    AuthStatus,
    CryptFilter,
    CryptFilterBuilder,
    CryptFilterConfiguration,
    IdentityCryptFilter,
    SecurityHandler,
    SecurityHandlerVersion,
    build_crypt_filter,
)
from .cred_ser import SerialisableCredential, SerialisedCredential
from .filter_mixins import (
    AESCryptFilterMixin,
    AESGCMCryptFilterMixin,
    RC4CryptFilterMixin,
)
from .permissions import PubKeyPermissions

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RecipientEncryptionPolicy:
    ignore_key_usage: bool = False
    """
    Ignore key usage bits in the recipient's certificate.
    """

    prefer_oaep: bool = False
    """
    For RSA recipients, encrypt with RSAES-OAEP.

    .. warning::
        This is not widely supported.
    """


class PubKeyCryptFilter(CryptFilter, abc.ABC):
    """
    Crypt filter for use with public key security handler.
    These are a little more independent than their counterparts for
    the standard security handlers, since different crypt filters
    can cater to different sets of recipients.

    :param recipients:
        List of CMS objects encoding recipient information for this crypt
        filters.
    :param acts_as_default:
        Indicates whether this filter is intended to be used in
        ``/StrF`` or ``/StmF``.
    :param encrypt_metadata:
        Whether this crypt filter should encrypt document-level metadata.

        .. warning::
            See :class:`.SecurityHandler` for some background on the
            way pyHanko interprets this value.
    """

    _handler: Optional['PubKeySecurityHandler'] = None

    def __init__(
        self,
        *,
        recipients=None,
        acts_as_default=False,
        encrypt_metadata=True,
        **kwargs,
    ):
        self.recipients = recipients
        self.acts_as_default = acts_as_default
        self.encrypt_metadata = encrypt_metadata
        self._pubkey_auth_failed = False
        self._shared_key = self._recp_key_seed = None
        super().__init__(**kwargs)

    @property
    def _auth_failed(self) -> bool:
        return self._pubkey_auth_failed

    def _set_security_handler(self, handler):
        if not isinstance(handler, PubKeySecurityHandler):
            raise TypeError  # pragma: nocover
        super()._set_security_handler(handler)
        self._shared_key = self._recp_key_seed = None

    def add_recipients(
        self,
        certs: List[x509.Certificate],
        policy: RecipientEncryptionPolicy,
        perms: PubKeyPermissions = PubKeyPermissions.allow_everything(),
    ):
        """
        Add recipients to this crypt filter.
        This always adds one full CMS object to the Recipients array

        :param certs:
            A list of recipient certificates.
        :param policy:
            Encryption policy choices for the chosen set of recipients.
        :param perms:
            The permission bits to assign to the listed recipients.
        """

        if not self.acts_as_default and self.recipients:
            raise misc.PdfError(
                "A non-default crypt filter cannot have multiple sets of "
                "recipients."
            )
        if self.recipients is None:
            # assume that this is a freshly created pubkey crypt filter,
            # so set up the shared seed
            self._recp_key_seed = secrets.token_bytes(20)
            self.recipients = []

        if self._shared_key is not None or self._recp_key_seed is None:
            raise misc.PdfError(
                "Adding recipients after deriving the shared key or "
                "before authenticating is not possible."
            )
        new_cms = construct_recipient_cms(
            certs,
            self._recp_key_seed,
            perms,
            policy=policy,
            include_permissions=self.acts_as_default,
        )
        self.recipients.append(new_cms)

    def authenticate(self, credential) -> AuthResult:
        """
        Authenticate to this crypt filter in particular.
        If used in ``/StmF`` or ``/StrF``, you don't need to worry about
        calling this method directly.

        :param credential:
            The :class:`.EnvelopeKeyDecrypter` to authenticate with.
        :return:
            An :class:`AuthResult` object indicating the level of access
            obtained.
        """
        for recp in self.recipients:
            seed, perms = read_seed_from_recipient_cms(recp, credential)
            if seed is not None:
                self._recp_key_seed = seed
                return AuthResult(AuthStatus.USER, perms)
        return AuthResult(AuthStatus.FAILED)

    def derive_shared_encryption_key(self) -> bytes:
        assert self._handler is not None
        if self._recp_key_seed is None:
            raise misc.PdfError("No seed available; authenticate first.")
        if self._handler.version >= SecurityHandlerVersion.AES256:
            md = sha256()
        else:
            md = sha1()
        md.update(self._recp_key_seed)
        for recp in self.recipients:
            md.update(recp.dump())
        if not self.encrypt_metadata and self.acts_as_default:
            md.update(b'\xff\xff\xff\xff')
        return md.digest()[: self.keylen]

    def as_pdf_object(self):
        result = super().as_pdf_object()
        result['/Length'] = generic.NumberObject(self.keylen * 8)
        recipients = generic.ArrayObject(
            generic.ByteStringObject(recp.dump()) for recp in self.recipients
        )
        if self.acts_as_default:
            result['/Recipients'] = recipients
        else:
            # non-default crypt filters can only have one recipient object
            result['/Recipients'] = recipients[0]
        result['/EncryptMetadata'] = generic.BooleanObject(
            self.encrypt_metadata
        )
        return result


class PubKeyAESCryptFilter(PubKeyCryptFilter, AESCryptFilterMixin):
    """
    AES crypt filter for public key security handlers.
    """

    pass


class PubKeyAESGCMCryptFilter(PubKeyCryptFilter, AESGCMCryptFilterMixin):
    """
    AES-GCM crypt filter for public key security handlers.
    """

    pass


class PubKeyRC4CryptFilter(PubKeyCryptFilter, RC4CryptFilterMixin):
    """
    RC4 crypt filter for public key security handlers.
    """

    pass


"""
Default name to use for the default crypt filter in the standard security
handler.
"""

DEFAULT_CRYPT_FILTER = generic.NameObject('/DefaultCryptFilter')
"""
Default name to use for the default crypt filter in public key security
handlers.
"""

DEF_EMBEDDED_FILE = generic.NameObject('/DefEmbeddedFile')
"""
Default name to use for the EFF crypt filter in public key security
handlers for documents where only embedded files are encrypted.
"""

"""
Name of the identity crypt filter.
"""


def _pubkey_rc4_config(keylen, recipients=None, encrypt_metadata=True):
    return CryptFilterConfiguration(
        {
            DEFAULT_CRYPT_FILTER: PubKeyRC4CryptFilter(
                keylen=keylen,
                acts_as_default=True,
                recipients=recipients,
                encrypt_metadata=encrypt_metadata,
            )
        },
        default_stream_filter=DEFAULT_CRYPT_FILTER,
        default_string_filter=DEFAULT_CRYPT_FILTER,
    )


def _pubkey_aes_config(keylen, recipients=None, encrypt_metadata=True):
    return CryptFilterConfiguration(
        {
            DEFAULT_CRYPT_FILTER: PubKeyAESCryptFilter(
                keylen=keylen,
                acts_as_default=True,
                recipients=recipients,
                encrypt_metadata=encrypt_metadata,
            )
        },
        default_stream_filter=DEFAULT_CRYPT_FILTER,
        default_string_filter=DEFAULT_CRYPT_FILTER,
    )


def _pubkey_gcm_config(recipients=None, encrypt_metadata=True):
    return CryptFilterConfiguration(
        {
            DEFAULT_CRYPT_FILTER: PubKeyAESGCMCryptFilter(
                acts_as_default=True,
                recipients=recipients,
                encrypt_metadata=encrypt_metadata,
            )
        },
        default_stream_filter=DEFAULT_CRYPT_FILTER,
        default_string_filter=DEFAULT_CRYPT_FILTER,
    )


@enum.unique
class PubKeyAdbeSubFilter(enum.Enum):
    """
    Enum describing the different subfilters that can be used for public key
    encryption in the PDF specification.
    """

    S3 = generic.NameObject('/adbe.pkcs7.s3')
    S4 = generic.NameObject('/adbe.pkcs7.s4')
    S5 = generic.NameObject('/adbe.pkcs7.s5')


def construct_envelope_content(
    seed: bytes, perms: PubKeyPermissions, include_permissions=True
):
    assert len(seed) == 20
    return seed + (perms.as_bytes() if include_permissions else b'')


def _rsaes_pkcs1v15_recipient(
    pub_key_info: PublicKeyInfo,
    rid: cms.RecipientIdentifier,
    envelope_key: bytes,
):
    padding = PKCS1v15()

    algo = cms.KeyEncryptionAlgorithm(
        {'algorithm': cms.KeyEncryptionAlgorithmId('rsaes_pkcs1v15')}
    )

    pub_key = serialization.load_der_public_key(pub_key_info.dump())
    assert isinstance(pub_key, RSAPublicKey)
    encrypted_data = pub_key.encrypt(envelope_key, padding=padding)
    return _format_ktri(rid=rid, algo=algo, encrypted_data=encrypted_data)


def _rsaes_oaep_recipient(
    pub_key_info: PublicKeyInfo,
    rid: cms.RecipientIdentifier,
    envelope_key: bytes,
):
    # recycle these routines
    from pyhanko.sign.general import get_pyca_cryptography_hash
    from pyhanko.sign.signers.pdf_cms import select_suitable_signing_md

    digest_function_name = select_suitable_signing_md(pub_key_info)
    digest_spec = get_pyca_cryptography_hash(digest_function_name)
    algo = cms.KeyEncryptionAlgorithm(
        {
            'algorithm': cms.KeyEncryptionAlgorithmId('rsaes_oaep'),
            'parameters': RSAESOAEPParams(
                {
                    'hash_algorithm': {'algorithm': digest_function_name},
                    'mask_gen_algorithm': {
                        'algorithm': 'mgf1',
                        'parameters': {'algorithm': digest_function_name},
                    },
                }
            ),
        }
    )
    padding = OAEP(mgf=MGF1(digest_spec), algorithm=digest_spec, label=None)

    pub_key = serialization.load_der_public_key(pub_key_info.dump())
    assert isinstance(pub_key, RSAPublicKey)
    encrypted_data = pub_key.encrypt(envelope_key, padding=padding)
    return _format_ktri(rid=rid, algo=algo, encrypted_data=encrypted_data)


def _format_ktri(
    rid: cms.RecipientIdentifier,
    algo: cms.KeyEncryptionAlgorithm,
    encrypted_data: bytes,
):
    return cms.RecipientInfo(
        {
            'ktri': cms.KeyTransRecipientInfo(
                {
                    'version': 0,
                    'rid': rid,
                    'key_encryption_algorithm': algo,
                    'encrypted_key': encrypted_data,
                }
            )
        }
    )


def _choose_ecdh_settings(
    pub_key_info: PublicKeyInfo,
) -> Tuple[
    hashes.HashAlgorithm, KeyEncryptionAlgorithmId, KeyEncryptionAlgorithmId
]:
    algo_name = pub_key_info.algorithm
    if algo_name == 'ec':
        approx_sec_level = pub_key_info.bit_size // 2
    elif algo_name == 'x25519':
        approx_sec_level = 128
    else:
        approx_sec_level = 256
    # All of these are dhSinglePass-stdDH-sha*kdf
    if approx_sec_level <= 128:
        return (
            hashes.SHA256(),
            KeyEncryptionAlgorithmId('aes128_wrap'),
            KeyEncryptionAlgorithmId('1.3.132.1.11.1'),
        )
    elif approx_sec_level <= 192:
        return (
            hashes.SHA384(),
            KeyEncryptionAlgorithmId('aes192_wrap'),
            KeyEncryptionAlgorithmId('1.3.132.1.11.2'),
        )
    else:
        return (
            hashes.SHA512(),
            KeyEncryptionAlgorithmId('aes256_wrap'),
            KeyEncryptionAlgorithmId('1.3.132.1.11.3'),
        )


def _ecdh_recipient(
    pub_key_info: PublicKeyInfo,
    rid: cms.KeyAgreementRecipientIdentifier,
    envelope_key: bytes,
):
    # static-ephemeral ECDH (standard) with X9.63 key derivation
    digest_spec, key_wrap_algo_id, key_exch_algo_id = _choose_ecdh_settings(
        pub_key_info
    )
    key_wrap_algo = KeyEncryptionAlgorithm({'algorithm': key_wrap_algo_id})
    pub_key = serialization.load_der_public_key(pub_key_info.dump())

    originator_key: Union[
        X25519PrivateKey, X448PrivateKey, EllipticCurvePrivateKey
    ]
    if isinstance(pub_key, EllipticCurvePublicKey):
        originator_key = generate_ec_private_key(pub_key.curve)
        ecdh_value = originator_key.exchange(ECDH(), pub_key)
    elif isinstance(pub_key, X25519PublicKey):
        originator_key = X25519PrivateKey.generate()
        ecdh_value = originator_key.exchange(pub_key)
    elif isinstance(pub_key, X448PublicKey):
        originator_key = X448PrivateKey.generate()
        ecdh_value = originator_key.exchange(pub_key)
    else:
        raise NotImplementedError

    originator_key_info = PublicKeyInfo.load(
        originator_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )

    ukm = secrets.token_bytes(16)
    kdf = _kdf_for_exchange(
        kdf_digest=digest_spec,
        key_wrap_algo=key_wrap_algo,
        user_keying_material=ukm,
    )
    kek = kdf.derive(ecdh_value)
    encrypted_data = keywrap.aes_key_wrap(
        wrapping_key=kek, key_to_wrap=envelope_key
    )
    return _format_kari(
        rid=rid,
        originator_key=originator_key_info,
        algo=cms.KeyEncryptionAlgorithm(
            {
                'algorithm': key_exch_algo_id,
                'parameters': key_wrap_algo,
            }
        ),
        ukm=ukm,
        encrypted_data=encrypted_data,
    )


def _format_kari(
    rid: cms.KeyAgreementRecipientIdentifier,
    originator_key: PublicKeyInfo,
    algo: cms.KeyEncryptionAlgorithm,
    ukm: bytes,
    encrypted_data: bytes,
):
    return cms.RecipientInfo(
        name='kari',
        value=cms.KeyAgreeRecipientInfo(
            {
                'version': 3,
                'originator': cms.OriginatorIdentifierOrKey(
                    name='originator_key', value=originator_key
                ),
                'ukm': ukm,
                'key_encryption_algorithm': algo,
                'recipient_encrypted_keys': [
                    cms.RecipientEncryptedKey(
                        {'rid': rid, 'encrypted_key': encrypted_data}
                    )
                ],
            }
        ),
    )


def _recipient_info(
    envelope_key: bytes,
    cert: x509.Certificate,
    policy: RecipientEncryptionPolicy,
):
    pub_key_info = cert.public_key
    pubkey_algo_info: PublicKeyAlgorithm = pub_key_info['algorithm']
    algorithm_name = pubkey_algo_info['algorithm'].native

    assert len(envelope_key) == 32

    if not policy.ignore_key_usage:
        # TODO for ECC: reject ECDSA-only keys
        key_usage = cert.key_usage_value
        if key_usage is None or 'key_encipherment' not in key_usage.native:
            raise misc.PdfWriteError(
                f"Certificate for subject {cert.subject.human_friendly} does "
                f"not have the 'key_encipherment' key usage bit set."
            )

    # TODO support subjectKeyIdentifier here (requiring version 2)
    iss_serial_rid = cms.IssuerAndSerialNumber(
        {'issuer': cert.issuer, 'serial_number': cert.serial_number}
    )
    if algorithm_name == 'rsa':
        rid = cms.RecipientIdentifier(
            {'issuer_and_serial_number': iss_serial_rid}
        )
        if policy.prefer_oaep:
            return _rsaes_oaep_recipient(pub_key_info, rid, envelope_key)
        else:
            return _rsaes_pkcs1v15_recipient(pub_key_info, rid, envelope_key)
    elif algorithm_name in ('ec', 'x25519', 'x448'):
        ka_rid = cms.KeyAgreementRecipientIdentifier(
            {'issuer_and_serial_number': iss_serial_rid}
        )
        return _ecdh_recipient(pub_key_info, ka_rid, envelope_key)
    else:
        raise NotImplementedError(
            f"Cannot encrypt for key type '{algorithm_name}'"
        )


def construct_recipient_cms(
    certificates: List[x509.Certificate],
    seed: bytes,
    perms: PubKeyPermissions,
    policy: RecipientEncryptionPolicy,
    include_permissions=True,
) -> cms.ContentInfo:
    # The content of the generated ContentInfo object
    # is an object of type EnvelopedData, containing a 20 byte seed (+ perms).
    #
    # This seed is shared among all recipients (including those occurring in
    # other CMS objects, if relevant), and is the only secret part of the
    # key derivation procedure used to obtain the file encryption key.
    #
    # The envelope content is then encrypted using an envelope key,
    # which is in turn encrypted using the public key of each recipient and
    # stored in a RecipientInfo object (more precisely, a
    # KeyTransRecipientInfo object). PyHanko always uses AES-256 to encrypt
    # the envelope content, even if the chosen PDF encryption is weaker.
    #
    # The RecipientInfo objects, algorithm specification and envelope content
    # are then bundled into an EnvelopedData object.
    envelope_content = construct_envelope_content(
        seed, perms, include_permissions=include_permissions
    )
    # 256-bit key used to encrypt the envelope
    envelope_key = secrets.token_bytes(32)
    # encrypt the envelope content with the envelope key
    iv, encrypted_envelope_content = aes_cbc_encrypt(
        envelope_key, envelope_content, iv=None
    )

    # encrypt the envelope key for each recipient
    rec_infos = [
        _recipient_info(envelope_key, cert, policy=policy)
        for cert in certificates
    ]

    algo = cms.EncryptionAlgorithm(
        {
            'algorithm': algos.EncryptionAlgorithmId('aes256_cbc'),
            'parameters': iv,
        }
    )
    encrypted_content_info = cms.EncryptedContentInfo(
        {
            'content_type': cms.ContentType('data'),
            'content_encryption_algorithm': algo,
            'encrypted_content': encrypted_envelope_content,
        }
    )

    # version 0 because no originatorInfo, no attribute certs
    # and all recipientinfo structures have version 0 (and aren't pwri)
    enveloped_data = cms.EnvelopedData(
        {
            'version': 0,
            'recipient_infos': rec_infos,
            'encrypted_content_info': encrypted_content_info,
        }
    )

    # finally, package up the whole thing into a ContentInfo object
    return cms.ContentInfo(
        {
            'content_type': cms.ContentType('enveloped_data'),
            'content': enveloped_data,
        }
    )


class InappropriateCredentialError(TypeError):
    pass


# TODO implement a PKCS#11 version of this interface
class EnvelopeKeyDecrypter:
    """
    General credential class for use with public key security handlers.

    This allows the key decryption process to happen offline, e.g. on a smart
    card.
    """

    @property
    def cert(self) -> x509.Certificate:
        """
        :return:
            Return the recipient's certificate
        """
        raise NotImplementedError

    def decrypt(
        self, encrypted_key: bytes, algo_params: cms.KeyEncryptionAlgorithm
    ) -> bytes:
        """
        Invoke the actual key decryption algorithm.
        Used with key transport.

        :param encrypted_key:
            Payload to decrypt.
        :param algo_params:
            Specification of the encryption algorithm as a CMS object.
        :raises InappropriateCredentialError:
            if the credential cannot be used for key transport.
        :return:
            The decrypted payload.
        """
        raise NotImplementedError

    def decrypt_with_exchange(
        self,
        encrypted_key: bytes,
        algo_params: cms.KeyEncryptionAlgorithm,
        originator_identifier: cms.OriginatorIdentifierOrKey,
        user_keying_material: bytes,
    ) -> bytes:
        """
        Decrypt an envelope key using a key derived from a key exchange.

        :param encrypted_key:
            Payload to decrypt.
        :param algo_params:
            Specification of the encryption algorithm as a CMS object.
        :param originator_identifier:
            Information about the originator necessary to complete the key
            exchange.
        :param user_keying_material:
            The user keying material that will be used in the key derivation.
        :return:
            The decrypted payload.
        """
        raise NotImplementedError


class _PrivKeyAndCert(core.Sequence):
    _fields = [('key', PrivateKeyInfo), ('cert', x509.Certificate)]


class ECCCMSSharedInfo(core.Sequence):
    _fields = [
        ('key_info', KeyEncryptionAlgorithm),
        (
            'entityUInfo',
            core.OctetString,
            {'explicit': 0, 'optional': True},
        ),
        ('suppPubInfo', core.OctetString, {'explicit': 2}),
    ]


AES_WRAP_PATTERN = re.compile(r'aes(\d+)_wrap(_pad)?')


def _kdf_for_exchange(
    *,
    kdf_digest: hashes.HashAlgorithm,
    key_wrap_algo: cms.KeyEncryptionAlgorithm,
    user_keying_material: Optional[bytes],
) -> KeyDerivationFunction:
    key_wrap_algo_id: str = key_wrap_algo['algorithm'].native
    wrap_match = AES_WRAP_PATTERN.fullmatch(key_wrap_algo_id)
    if not wrap_match:
        raise NotImplementedError(
            f"{key_wrap_algo_id} is not a supported key wrapping algorithm"
        )
    kek_bit_len = int(wrap_match.group(1))
    return X963KDF(
        algorithm=kdf_digest,
        length=kek_bit_len // 8,
        sharedinfo=ECCCMSSharedInfo(
            {
                'key_info': key_wrap_algo,
                'entityUInfo': user_keying_material,
                'suppPubInfo': struct.pack('>I', kek_bit_len),
            }
        ).dump(),
    )


class SimpleEnvelopeKeyDecrypter(EnvelopeKeyDecrypter, SerialisableCredential):
    """
    Implementation of :class:`.EnvelopeKeyDecrypter` where the private key
    is an RSA or ECC key residing in memory.

    :param cert:
        The recipient's certificate.
    :param private_key:
        The recipient's private key.
    """

    dhsinglepass_stddh_arc_pattern = re.compile(r'1\.3\.132\.1\.11\.(\d+)')

    @classmethod
    def get_name(cls) -> str:
        return 'raw_privkey'

    def _ser_value(self) -> bytes:
        values = {'key': self.private_key, 'cert': self.cert}
        return _PrivKeyAndCert(values).dump()

    @classmethod
    def _deser_value(cls, data: bytes):
        try:
            decoded = _PrivKeyAndCert.load(data)
            key = decoded['key']
            cert = decoded['cert']
        except ValueError as e:
            raise misc.PdfReadError(
                "Failed to decode serialised pubkey credential"
            ) from e
        return SimpleEnvelopeKeyDecrypter(cert=cert, private_key=key)

    def __init__(self, cert: x509.Certificate, private_key: PrivateKeyInfo):
        self.private_key: PrivateKeyInfo = private_key
        self._cert = cert

    @property
    def cert(self) -> x509.Certificate:
        return self._cert

    @staticmethod
    def load(key_file, cert_file, key_passphrase=None):
        """
        Load a key decrypter using key material from files on disk.

        :param key_file:
            File containing the recipient's private key.
        :param cert_file:
            File containing the recipient's certificate.
        :param key_passphrase:
            Passphrase for the key file, if applicable.
        :return:
            An instance of :class:`.SimpleEnvelopeKeyDecrypter`.
        """
        from ...keys import load_private_key_from_pemder

        try:
            private_key = load_private_key_from_pemder(
                key_file, passphrase=key_passphrase
            )
            from ...keys import load_cert_from_pemder

            cert = load_cert_from_pemder(cert_file)
        except (IOError, ValueError, TypeError) as e:  # pragma: nocover
            logger.error('Could not load cryptographic material', exc_info=e)
            return None
        return SimpleEnvelopeKeyDecrypter(cert=cert, private_key=private_key)

    @classmethod
    def load_pkcs12(cls, pfx_file, passphrase=None):
        """
        Load a key decrypter using key material from a PKCS#12 file on disk.

        :param pfx_file:
            Path to the PKCS#12 file containing the key material.
        :param passphrase:
            Passphrase for the private key, if applicable.
        :return:
            An instance of :class:`.SimpleEnvelopeKeyDecrypter`.
        """

        try:
            with open(pfx_file, 'rb') as f:
                pfx_bytes = f.read()
            (private_key, cert, other_certs) = pkcs12.load_key_and_certificates(
                pfx_bytes, passphrase
            )

            from ...keys import (
                _translate_pyca_cryptography_cert_to_asn1,
                _translate_pyca_cryptography_key_to_asn1,
            )

            cert = _translate_pyca_cryptography_cert_to_asn1(cert)
            private_key = _translate_pyca_cryptography_key_to_asn1(private_key)
        except (IOError, ValueError, TypeError) as e:  # pragma: nocover
            logger.error(f'Could not open PKCS#12 file {pfx_file}.', exc_info=e)
            return None

        return SimpleEnvelopeKeyDecrypter(cert=cert, private_key=private_key)

    def decrypt(
        self, encrypted_key: bytes, algo_params: cms.KeyEncryptionAlgorithm
    ) -> bytes:
        """
        Decrypt the payload using RSA with PKCS#1 v1.5 padding or OAEP.
        Other schemes are not (currently) supported by this implementation.

        :param encrypted_key:
            Payload to decrypt.
        :param algo_params:
            Specification of the encryption algorithm as a CMS object.
            Must use ``rsaes_pkcs1v15`` or ``rsaes_oaep``.
        :return:
            The decrypted payload.
        """
        algo_name = algo_params['algorithm'].native
        padding: AsymmetricPadding
        if algo_name == 'rsaes_pkcs1v15':
            padding = PKCS1v15()
        elif algo_name == 'rsaes_oaep':
            from pyhanko.sign.general import get_pyca_cryptography_hash

            oaep_params: RSAESOAEPParams = algo_params['parameters']
            mgf = oaep_params['mask_gen_algorithm']
            mgf_name = mgf['algorithm'].native
            if mgf_name != 'mgf1':
                raise NotImplementedError(
                    f"Only MGF1 is implemented, but got '{mgf_name}'"
                )
            padding = OAEP(
                mgf=MGF1(
                    algorithm=get_pyca_cryptography_hash(
                        mgf['parameters']['algorithm'].native
                    )
                ),
                algorithm=get_pyca_cryptography_hash(
                    oaep_params['hash_algorithm']['algorithm'].native
                ),
                label=None,
            )
        else:
            raise NotImplementedError(
                f"Only 'rsaes_pkcs1v15' and 'rsaes_oaep' are supported for "
                f"envelope decryption, not '{algo_name}'."
            )
        priv_key = serialization.load_der_private_key(
            self.private_key.dump(), password=None
        )
        if not isinstance(priv_key, RSAPrivateKey):
            raise InappropriateCredentialError(
                "The loaded key does not seem to be an RSA private key"
            )
        return priv_key.decrypt(encrypted_key, padding=padding)

    def decrypt_with_exchange(
        self,
        encrypted_key: bytes,
        algo_params: cms.KeyEncryptionAlgorithm,
        originator_identifier: cms.OriginatorIdentifierOrKey,
        user_keying_material: Optional[bytes],
    ) -> bytes:
        """
        Decrypt the payload using a key agreed via ephemeral-static
        standard (non-cofactor) ECDH with X9.63 key derivation.
        Other schemes aer not supported at this time.


        :param encrypted_key:
            Payload to decrypt.
        :param algo_params:
            Specification of the encryption algorithm as a CMS object.
        :param originator_identifier:
            The originator info, which must be an EC key.
        :param user_keying_material:
            The user keying material that will be used in the key derivation.
        :return:
            The decrypted payload.
        """
        # TODO get the relevant OIDs added to asn1crypto
        oid = algo_params['algorithm']

        match = self.dhsinglepass_stddh_arc_pattern.fullmatch(oid.dotted)
        kdf_digest: Optional[hashes.HashAlgorithm] = None
        if match:
            kdf_digest = {
                '0': hashes.SHA224(),
                '1': hashes.SHA256(),
                '2': hashes.SHA384(),
                '3': hashes.SHA512(),
            }.get(match.group(1), None)
        if not kdf_digest:
            raise NotImplementedError(
                "Only dhSinglePass-stdDH algorithms from SEC 1 / RFC 5753 are "
                "supported."
            )

        key_wrap_algo = cms.KeyEncryptionAlgorithm.load(
            algo_params['parameters'].dump()
        )
        key_wrap_algo_id: str = key_wrap_algo['algorithm'].native

        if not AES_WRAP_PATTERN.fullmatch(key_wrap_algo_id):
            raise NotImplementedError(
                f"{key_wrap_algo_id} is not a supported key wrapping algorithm"
            )
        unwrap_key = (
            keywrap.aes_key_unwrap_with_padding
            if key_wrap_algo_id.endswith('_pad')
            else keywrap.aes_key_unwrap
        )
        kdf = _kdf_for_exchange(
            kdf_digest=kdf_digest,
            key_wrap_algo=key_wrap_algo,
            user_keying_material=user_keying_material,
        )

        if originator_identifier.name != 'originator_key':
            raise NotImplementedError("Only originator_key is supported")

        originator_pub_key_info: PublicKeyInfo = (
            originator_identifier.chosen.untag()
        )
        originator_pub_key = serialization.load_der_public_key(
            originator_pub_key_info.dump()
        )

        priv_key = serialization.load_der_private_key(
            self.private_key.dump(), password=None
        )

        mismatch_msg = (
            "Originator's public key is not compatible "
            "with selected private key"
        )
        if isinstance(priv_key, EllipticCurvePrivateKey):
            # we could rely on pyca/cryptography to perform this check for us,
            #  but then mypy complains
            if (
                not isinstance(originator_pub_key, EllipticCurvePublicKey)
                or originator_pub_key.curve.name != priv_key.curve.name
            ):
                raise ValueError(mismatch_msg)
            ecdh_value = priv_key.exchange(ECDH(), originator_pub_key)
        elif isinstance(priv_key, X25519PrivateKey):
            if not isinstance(originator_pub_key, X25519PublicKey):
                raise ValueError(mismatch_msg)
            ecdh_value = priv_key.exchange(originator_pub_key)
        elif isinstance(priv_key, X448PrivateKey):
            if not isinstance(originator_pub_key, X448PublicKey):
                raise ValueError(mismatch_msg)
            ecdh_value = priv_key.exchange(originator_pub_key)
        else:
            raise InappropriateCredentialError(
                "The loaded key does not seem to be an EC private key"
            )
        derived_kek = kdf.derive(ecdh_value)
        return unwrap_key(wrapping_key=derived_kek, wrapped_key=encrypted_key)


SerialisableCredential.register(SimpleEnvelopeKeyDecrypter)


def read_envelope_key(
    ed: cms.EnvelopedData, decrypter: EnvelopeKeyDecrypter
) -> Optional[bytes]:
    rec_info: cms.RecipientInfo
    for rec_info in ed['recipient_infos']:
        if rec_info.name == 'ktri':
            ktri = rec_info.chosen
            issuer_and_serial = ktri['rid'].chosen
            if not isinstance(issuer_and_serial, cms.IssuerAndSerialNumber):
                raise NotImplementedError(
                    "Recipient identifier must be of type IssuerAndSerialNumber."
                )
            issuer = issuer_and_serial['issuer']
            serial = issuer_and_serial['serial_number'].native
            if (
                decrypter.cert.issuer == issuer
                and decrypter.cert.serial_number == serial
            ):
                try:
                    return decrypter.decrypt(
                        ktri['encrypted_key'].native,
                        ktri['key_encryption_algorithm'],
                    )
                except InappropriateCredentialError as e:
                    raise e
                except Exception as e:
                    raise misc.PdfReadError(
                        "Failed to decrypt envelope key"
                    ) from e
        elif rec_info.name == 'kari':
            kari = rec_info.chosen
            for recipient_enc_key in kari['recipient_encrypted_keys']:
                issuer_and_serial = recipient_enc_key['rid'].chosen
                # TODO make this DRY and support key identifier
                if not isinstance(issuer_and_serial, cms.IssuerAndSerialNumber):
                    raise NotImplementedError(
                        "Recipient identifier must be of type IssuerAndSerialNumber."
                    )
                issuer = issuer_and_serial['issuer']
                serial = issuer_and_serial['serial_number'].native
                if (
                    decrypter.cert.issuer == issuer
                    and decrypter.cert.serial_number == serial
                ):
                    try:
                        return decrypter.decrypt_with_exchange(
                            recipient_enc_key['encrypted_key'].native,
                            kari['key_encryption_algorithm'],
                            originator_identifier=kari['originator'],
                            user_keying_material=kari['ukm'].native,
                        )
                    except InappropriateCredentialError as e:
                        raise e
                    except Exception as e:
                        raise misc.PdfReadError(
                            "Failed to decrypt envelope key"
                        ) from e
        else:
            raise NotImplementedError(
                "RecipientInfo must be of type KeyTransRecipientInfo "
                "or KeyAgreeRecipientInfo"
            )

    return None


def read_seed_from_recipient_cms(
    recipient_cms: cms.ContentInfo, decrypter: EnvelopeKeyDecrypter
) -> Tuple[Optional[bytes], Optional[PubKeyPermissions]]:
    content_type = recipient_cms['content_type'].native
    if content_type != 'enveloped_data':
        raise misc.PdfReadError(
            "Recipient CMS content type must be enveloped data, not "
            + content_type
        )
    ed: cms.EnvelopedData = recipient_cms['content']
    encrypted_content_info = ed['encrypted_content_info']
    envelope_key = read_envelope_key(ed, decrypter)
    if envelope_key is None:
        return None, None

    # we have the envelope key
    # next up: decrypting the envelope

    algo: cms.EncryptionAlgorithm = encrypted_content_info[
        'content_encryption_algorithm'
    ]
    encrypted_envelope_content = encrypted_content_info[
        'encrypted_content'
    ].native

    # the spec says that we have to support rc4 (<=256 bits),
    # des, triple des, rc2 (<=128 bits)
    # and AES-CBC (128, 192, 256 bits)
    try:
        cipher_name = algo.encryption_cipher
    except (ValueError, KeyError):
        cipher_name = algo['algorithm'].native

    with_iv = {'aes': aes_cbc_decrypt}
    try:
        # noinspection PyUnresolvedReferences
        from oscrypto import symmetric

        # The spec mandates that we support these, but pyca/cryptography
        # doesn't offer implementations.
        # (DES and 3DES have fortunately gone out of style, but some libraries
        #  still rely on RC2)
        with_iv.update(
            {
                'des': symmetric.des_cbc_pkcs5_decrypt,
                'tripledes': symmetric.tripledes_cbc_pkcs5_decrypt,
                'rc2': symmetric.rc2_cbc_pkcs5_decrypt,
            }
        )
    except Exception as e:  # pragma: nocover
        if cipher_name in ('des', 'tripledes', 'rc2'):
            raise NotImplementedError(
                "DES, 3DES and RC2 require oscrypto to be present"
            ) from e

    if cipher_name in with_iv:
        decryption_fun = with_iv[cipher_name]
        iv = algo.encryption_iv
        content = decryption_fun(envelope_key, encrypted_envelope_content, iv)
    elif cipher_name == 'rc4':
        content = rc4_encrypt(envelope_key, encrypted_envelope_content)
    else:
        raise misc.PdfReadError(
            f"Cipher {cipher_name} is not allowed in PDF 2.0."
        )

    seed = content[:20]
    perms: Optional[PubKeyPermissions] = None
    if len(content) == 24:
        # permissions are included
        perms = PubKeyPermissions.from_bytes(content[20:])
    return seed, perms


def _read_generic_pubkey_cf_info(cfdict: generic.DictionaryObject):
    try:
        recipients = cfdict['/Recipients']
    except KeyError:
        raise misc.PdfReadError(
            "PubKey CF dictionary must have /Recipients key"
        )
    if isinstance(recipients, generic.ByteStringObject):
        recipients = (recipients,)
    recipient_objs = [
        cms.ContentInfo.load(x.original_bytes) for x in recipients
    ]
    encrypt_metadata = cfdict.get('/EncryptMetadata', True)
    return {'recipients': recipient_objs, 'encrypt_metadata': encrypt_metadata}


def _build_legacy_pubkey_cf(cfdict, acts_as_default):
    keylen_bits = cfdict.get('/Length', 40)
    return PubKeyRC4CryptFilter(
        keylen=keylen_bits // 8,
        acts_as_default=acts_as_default,
        **_read_generic_pubkey_cf_info(cfdict),
    )


def _build_aes128_pubkey_cf(cfdict, acts_as_default):
    return PubKeyAESCryptFilter(
        keylen=16,
        acts_as_default=acts_as_default,
        **_read_generic_pubkey_cf_info(cfdict),
    )


def _build_aes256_pubkey_cf(cfdict, acts_as_default):
    return PubKeyAESCryptFilter(
        keylen=32,
        acts_as_default=acts_as_default,
        **_read_generic_pubkey_cf_info(cfdict),
    )


def _build_aesgcm_pubkey_cf(cfdict, acts_as_default):
    return PubKeyAESGCMCryptFilter(
        acts_as_default=acts_as_default, **_read_generic_pubkey_cf_info(cfdict)
    )


@SecurityHandler.register
class PubKeySecurityHandler(SecurityHandler):
    """
    Security handler for public key encryption in PDF.

    As with the standard security handler, you essentially shouldn't ever
    have to instantiate these yourself (see :meth:`build_from_certs`).
    """

    _known_crypt_filters: Dict[generic.NameObject, CryptFilterBuilder] = {
        generic.NameObject('/V2'): _build_legacy_pubkey_cf,
        generic.NameObject('/AESV2'): _build_aes128_pubkey_cf,
        generic.NameObject('/AESV3'): _build_aes256_pubkey_cf,
        generic.NameObject('/AESV4'): _build_aesgcm_pubkey_cf,
        generic.NameObject('/Identity'): lambda _, __: IdentityCryptFilter(),
    }

    @classmethod
    def build_from_certs(
        cls,
        certs: List[x509.Certificate],
        keylen_bytes=16,
        version=SecurityHandlerVersion.AES256,
        use_aes=True,
        use_crypt_filters=True,
        perms: PubKeyPermissions = PubKeyPermissions.allow_everything(),
        encrypt_metadata=True,
        policy: RecipientEncryptionPolicy = RecipientEncryptionPolicy(),
        pdf_mac: bool = True,
        **kwargs,
    ) -> 'PubKeySecurityHandler':
        """
        Create a new public key security handler.

        This method takes many parameters, but only ``certs`` is mandatory.
        The default behaviour is to create a public key encryption handler
        where the underlying symmetric encryption is provided by AES-256.
        Any remaining keyword arguments will be passed to the constructor.

        :param certs:
            The recipients' certificates.
        :param keylen_bytes:
            The key length (in bytes). This is only relevant for legacy
            security handlers.
        :param version:
            The security handler version to use.
        :param use_aes:
            Use AES-128 instead of RC4 (only meaningful if the ``version``
            parameter is :attr:`~.SecurityHandlerVersion.RC4_OR_AES128`).
        :param use_crypt_filters:
            Whether to use crypt filters. This is mandatory for security
            handlers of version :attr:`~.SecurityHandlerVersion.RC4_OR_AES128`
            or higher.
        :param perms:
            Permission flags.
        :param encrypt_metadata:
            Whether to encrypt document metadata.

            .. warning::
                See :class:`.SecurityHandler` for some background on the
                way pyHanko interprets this value.
        :param pdf_mac:
            Include an ISO 32004 MAC.

            .. warning::
                Only works for PDF 2.0 security handlers.
        :param policy:
            Encryption policy choices for the chosen set of recipients.
        :return:
            An instance of :class:`.PubKeySecurityHandler`.
        """
        subfilter = (
            PubKeyAdbeSubFilter.S5
            if use_crypt_filters
            else PubKeyAdbeSubFilter.S4
        )
        cfc = None
        if version == SecurityHandlerVersion.RC4_OR_AES128:
            # only in this case we need a CFC, otherwise the constructor
            # takes care of it
            if use_aes:
                cfc = _pubkey_aes_config(
                    16, encrypt_metadata=encrypt_metadata, recipients=None
                )
            else:
                cfc = _pubkey_rc4_config(
                    keylen_bytes,
                    recipients=None,
                    encrypt_metadata=encrypt_metadata,
                )
        if pdf_mac and version >= SecurityHandlerVersion.AES256:
            perms &= ~PubKeyPermissions.TOLERATE_MISSING_PDF_MAC
            kdf_salt = secrets.token_bytes(32)
        else:
            kdf_salt = None
        # noinspection PyArgumentList
        sh = cls(
            version,
            subfilter,
            keylen_bytes,
            encrypt_metadata=encrypt_metadata,
            crypt_filter_config=cfc,
            recipient_objs=None,
            kdf_salt=kdf_salt,
            **kwargs,
        )
        sh.add_recipients(certs, perms=perms, policy=policy)
        return sh

    def __init__(
        self,
        version: SecurityHandlerVersion,
        pubkey_handler_subfilter: PubKeyAdbeSubFilter,
        legacy_keylen,
        encrypt_metadata=True,
        crypt_filter_config: Optional['CryptFilterConfiguration'] = None,
        recipient_objs: Optional[list] = None,
        compat_entries=True,
        kdf_salt: Optional[bytes] = None,
    ):
        # I don't see how it would be possible to handle V4 without
        # crypt filters in an unambiguous way. V5 should be possible in
        # principle, but Adobe Reader rejects that combination, so meh.
        if (
            version >= SecurityHandlerVersion.RC4_OR_AES128
            and pubkey_handler_subfilter != PubKeyAdbeSubFilter.S5
        ):
            raise misc.PdfError(
                "Subfilter /adbe.pkcs7.s5 is required for security handlers "
                "beyond V4."
            )

        if crypt_filter_config is None:
            if version == SecurityHandlerVersion.RC4_40:
                crypt_filter_config = _pubkey_rc4_config(
                    keylen=5,
                    encrypt_metadata=encrypt_metadata,
                    recipients=recipient_objs,
                )
            elif version == SecurityHandlerVersion.RC4_LONGER_KEYS:
                crypt_filter_config = _pubkey_rc4_config(
                    keylen=legacy_keylen,
                    encrypt_metadata=encrypt_metadata,
                    recipients=recipient_objs,
                )
            elif version == SecurityHandlerVersion.AES_GCM:
                crypt_filter_config = _pubkey_gcm_config(
                    recipients=recipient_objs, encrypt_metadata=encrypt_metadata
                )
            elif version >= SecurityHandlerVersion.AES256:
                # there's a reasonable default config that we can fall back to
                # here
                # NOTE: we _don't_ use GCM by default. With the way PDF
                # encryption works, the authentication guarantees are not
                # worth much anyhow (need ISO 32004-style solution).
                crypt_filter_config = _pubkey_aes_config(
                    keylen=32,
                    encrypt_metadata=encrypt_metadata,
                    recipients=recipient_objs,
                )
            else:
                raise misc.PdfError(
                    "Failed to impute a reasonable crypt filter config"
                )
        super().__init__(
            version,
            legacy_keylen,
            crypt_filter_config,
            encrypt_metadata=encrypt_metadata,
            compat_entries=compat_entries,
            kdf_salt=kdf_salt,
        )
        self.subfilter = pubkey_handler_subfilter
        self.encrypt_metadata = encrypt_metadata
        self._shared_key = None

    @classmethod
    def get_name(cls) -> str:
        return generic.NameObject('/Adobe.PubSec')

    @classmethod
    def support_generic_subfilters(cls) -> Set[str]:
        return {x.value for x in PubKeyAdbeSubFilter}

    @classmethod
    def read_cf_dictionary(
        cls, cfdict: generic.DictionaryObject, acts_as_default: bool
    ) -> CryptFilter:
        cf = build_crypt_filter(
            cls._known_crypt_filters, cfdict, acts_as_default
        )
        if cf is None:
            raise misc.PdfReadError(
                "An absent CFM or CFM of /None doesn't make sense in a "
                "PubSec CF dictionary"
            )
        return cf

    @classmethod
    def process_crypt_filters(
        cls, encrypt_dict: generic.DictionaryObject
    ) -> Optional['CryptFilterConfiguration']:
        cfc = super().process_crypt_filters(encrypt_dict)
        subfilter = cls._determine_subfilter(encrypt_dict)

        if cfc is not None and subfilter != PubKeyAdbeSubFilter.S5:
            raise misc.PdfReadError(
                "Crypt filters require /adbe.pkcs7.s5 as the declared "
                "handler."
            )
        elif cfc is None and subfilter == PubKeyAdbeSubFilter.S5:
            raise misc.PdfReadError(
                "/adbe.pkcs7.s5 handler requires crypt filters."
            )
        return cfc

    @classmethod
    def gather_pub_key_metadata(cls, encrypt_dict: generic.DictionaryObject):
        keylen_bits = encrypt_dict.get('/Length', 128)
        if (keylen_bits % 8) != 0:
            raise misc.PdfError("Key length must be a multiple of 8")
        keylen = keylen_bits // 8

        recipients = misc.get_and_apply(
            encrypt_dict,
            '/Recipients',
            lambda lst: [cms.ContentInfo.load(x.original_bytes) for x in lst],
        )

        # TODO get encrypt_metadata handling in line with ISO 32k
        #  (needs to happen at the crypt filter level instead)
        encrypt_metadata = encrypt_dict.get_and_apply(
            '/EncryptMetadata', bool, default=True
        )
        return dict(
            legacy_keylen=keylen,
            recipient_objs=recipients,
            encrypt_metadata=encrypt_metadata,
            kdf_salt=encrypt_dict.get_and_apply(
                '/KDFSalt',
                lambda x: (
                    x.original_bytes
                    if isinstance(
                        x, (generic.TextStringObject, generic.ByteStringObject)
                    )
                    else None
                ),
            ),
        )

    @classmethod
    def _determine_subfilter(cls, encrypt_dict: generic.DictionaryObject):
        try:
            return misc.get_and_apply(
                encrypt_dict,
                '/SubFilter',
                PubKeyAdbeSubFilter,
                default=(
                    PubKeyAdbeSubFilter.S5
                    if '/CF' in encrypt_dict
                    else PubKeyAdbeSubFilter.S4
                ),
            )
        except ValueError:
            raise misc.PdfReadError(
                "Invalid /SubFilter in public key encryption dictionary: "
                + encrypt_dict['/SubFilter']
            )

    @classmethod
    def instantiate_from_pdf_object(
        cls, encrypt_dict: generic.DictionaryObject
    ):
        v = SecurityHandlerVersion.from_number(encrypt_dict['/V'])

        return PubKeySecurityHandler(
            version=v,
            pubkey_handler_subfilter=cls._determine_subfilter(encrypt_dict),
            crypt_filter_config=cls.process_crypt_filters(encrypt_dict),
            **cls.gather_pub_key_metadata(encrypt_dict),
        )

    def as_pdf_object(self):
        result = generic.DictionaryObject()
        result['/Filter'] = generic.NameObject(self.get_name())
        result['/SubFilter'] = self.subfilter.value
        result['/V'] = self.version.as_pdf_object()
        if self._kdf_salt:
            result['/KDFSalt'] = generic.ByteStringObject(self._kdf_salt)
        if (
            self._compat_entries
            or self.version == SecurityHandlerVersion.RC4_LONGER_KEYS
        ):
            result['/Length'] = generic.NumberObject(self.keylen * 8)
        if self.version > SecurityHandlerVersion.RC4_LONGER_KEYS:
            result['/EncryptMetadata'] = generic.BooleanObject(
                self.encrypt_metadata
            )
        if self.subfilter == PubKeyAdbeSubFilter.S5:
            # include crypt filter config
            result.update(self.crypt_filter_config.as_pdf_object())
        else:
            # load recipients from default crypt filter into the encryption dict
            default_cf = self.get_stream_filter()
            if not isinstance(default_cf, PubKeyCryptFilter):
                raise TypeError  # pragma: nocover
            result['/Recipients'] = generic.ArrayObject(
                generic.ByteStringObject(recp.dump())
                for recp in default_cf.recipients
            )
        return result

    def add_recipients(
        self,
        certs: List[x509.Certificate],
        perms: PubKeyPermissions = PubKeyPermissions.allow_everything(),
        policy: RecipientEncryptionPolicy = RecipientEncryptionPolicy(),
    ):
        # add recipients to all *default* crypt filters
        # callers that want to do this more granularly are welcome to, but
        # then they have to do the legwork themselves.

        for cf in self.crypt_filter_config.standard_filters():
            if not isinstance(cf, PubKeyCryptFilter):
                continue
            cf.add_recipients(certs, perms=perms, policy=policy)

    def authenticate(
        self,
        credential: Union[EnvelopeKeyDecrypter, SerialisedCredential],
        id1=None,
    ) -> AuthResult:
        """
        Authenticate a user to this security handler.

        :param credential:
            The credential to use (an instance of :class:`.EnvelopeKeyDecrypter`
            in this case).
        :param id1:
            First part of the document ID.
            Public key encryption handlers ignore this key.
        :return:
            An :class:`AuthResult` object indicating the level of access
            obtained.
        """

        actual_credential: EnvelopeKeyDecrypter
        if isinstance(credential, SerialisedCredential):
            deser_credential = SerialisableCredential.deserialise(credential)
            if not isinstance(deser_credential, EnvelopeKeyDecrypter):
                raise misc.PdfReadError(
                    f"Pubkey authentication credential must be an instance of "
                    f"EnvelopeKeyDecrypter, not {type(deser_credential)}."
                )
            actual_credential = deser_credential
        else:
            actual_credential = credential

        perms = PubKeyPermissions.allow_everything()
        for cf in self.crypt_filter_config.standard_filters():
            if not isinstance(cf, PubKeyCryptFilter):
                continue
            recp: cms.ContentInfo
            result = cf.authenticate(actual_credential)
            if result.status == AuthStatus.FAILED:
                return result
            # these should really be the same for both filters, but hey,
            # you never know. ANDing them seems to be the most reasonable
            # course of action
            cf_flags = result.permission_flags
            if cf_flags is not None:
                assert isinstance(cf_flags, PubKeyPermissions)
                perms &= cf_flags
        if isinstance(actual_credential, SerialisableCredential):
            self._credential = actual_credential
        return AuthResult(AuthStatus.USER, perms)

    def get_file_encryption_key(self) -> bytes:
        # just grab the key from the default stream filter
        return self.crypt_filter_config.get_for_stream().shared_key
