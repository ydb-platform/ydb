""" constants.py
"""
import bz2
import hashlib
import os
import zlib
import warnings

from collections import namedtuple
from enum import Enum
from enum import IntEnum
from enum import IntFlag

from pyasn1.type.univ import ObjectIdentifier

from cryptography.hazmat.backends import openssl
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.ciphers import algorithms

from .types import FlagEnum
from .decorators import classproperty
from ._curves import BrainpoolP256R1, BrainpoolP384R1, BrainpoolP512R1, X25519, Ed25519

__all__ = [
    'Backend',
    'EllipticCurveOID',
    'ECPointFormat',
    'PacketTag',
    'SymmetricKeyAlgorithm',
    'PubKeyAlgorithm',
    'CompressionAlgorithm',
    'HashAlgorithm',
    'RevocationReason',
    'ImageEncoding',
    'SignatureType',
    'KeyServerPreferences',
    'S2KGNUExtension',
    'SecurityIssues',
    'String2KeyType',
    'TrustLevel',
    'KeyFlags',
    'Features',
    'RevocationKeyClass',
    'NotationDataFlags',
    'TrustFlags',
]


# this is 50 KiB
_hashtunedata = bytearray([10, 11, 12, 13, 14, 15, 16, 17] * 128 * 50)


class Backend(Enum):
    OpenSSL = openssl.backend


class EllipticCurveOID(Enum):
    """OIDs for supported elliptic curves."""
    # these are specified as:
    # id = (oid, curve)
    Invalid = ('', )
    #: DJB's fast elliptic curve
    Curve25519 = ('1.3.6.1.4.1.3029.1.5.1', X25519)
    #: Twisted Edwards variant of Curve25519
    Ed25519 = ('1.3.6.1.4.1.11591.15.1', Ed25519)
    #: NIST P-256, also known as SECG curve secp256r1
    NIST_P256 = ('1.2.840.10045.3.1.7', ec.SECP256R1)
    #: NIST P-384, also known as SECG curve secp384r1
    NIST_P384 = ('1.3.132.0.34', ec.SECP384R1)
    #: NIST P-521, also known as SECG curve secp521r1
    NIST_P521 = ('1.3.132.0.35', ec.SECP521R1)
    #: Brainpool Standard Curve, 256-bit
    #:
    #: .. note::
    #:     Requires OpenSSL >= 1.0.2
    Brainpool_P256 = ('1.3.36.3.3.2.8.1.1.7', BrainpoolP256R1)
    #: Brainpool Standard Curve, 384-bit
    #:
    #: .. note::
    #:     Requires OpenSSL >= 1.0.2
    Brainpool_P384 = ('1.3.36.3.3.2.8.1.1.11', BrainpoolP384R1)
    #: Brainpool Standard Curve, 512-bit
    #:
    #: .. note::
    #:     Requires OpenSSL >= 1.0.2
    Brainpool_P512 = ('1.3.36.3.3.2.8.1.1.13', BrainpoolP512R1)
    #: SECG curve secp256k1
    SECP256K1 = ('1.3.132.0.10', ec.SECP256K1)

    def __new__(cls, oid, curve=None):
        # preprocessing stage for enum members:
        #  - set enum_member.value to ObjectIdentifier(oid)
        #  - if curve is not None and curve.name is in ec._CURVE_TYPES, set enum_member.curve to curve
        #  - otherwise, set enum_member.curve to None
        obj = object.__new__(cls)
        obj._value_ = ObjectIdentifier(oid)
        obj.curve = None

        if curve is not None and curve.name in ec._CURVE_TYPES:
            obj.curve = curve

        return obj

    @property
    def can_gen(self):
        return self.curve is not None

    @property
    def key_size(self):
        if self.curve is not None:
            return self.curve.key_size

    @property
    def kdf_halg(self):
        # return the hash algorithm to specify in the KDF fields when generating a key
        algs = {256: HashAlgorithm.SHA256,
                384: HashAlgorithm.SHA384,
                512: HashAlgorithm.SHA512,
                521: HashAlgorithm.SHA512}

        return algs.get(self.key_size, None)

    @property
    def kek_alg(self):
        # return the AES algorithm to specify in the KDF fields when generating a key
        algs = {256: SymmetricKeyAlgorithm.AES128,
                384: SymmetricKeyAlgorithm.AES192,
                512: SymmetricKeyAlgorithm.AES256,
                521: SymmetricKeyAlgorithm.AES256}

        return algs.get(self.key_size, None)


class ECPointFormat(IntEnum):
    # https://tools.ietf.org/html/draft-ietf-openpgp-rfc4880bis-07#appendix-B
    Standard = 0x04
    Native = 0x40
    OnlyX = 0x41
    OnlyY = 0x42


class PacketTag(IntEnum):
    Invalid = 0
    PublicKeyEncryptedSessionKey = 1
    Signature = 2
    SymmetricKeyEncryptedSessionKey = 3
    OnePassSignature = 4
    SecretKey = 5
    PublicKey = 6
    SecretSubKey = 7
    CompressedData = 8
    SymmetricallyEncryptedData = 9
    Marker = 10
    LiteralData = 11
    Trust = 12
    UserID = 13
    PublicSubKey = 14
    UserAttribute = 17
    SymmetricallyEncryptedIntegrityProtectedData = 18
    ModificationDetectionCode = 19


class SymmetricKeyAlgorithm(IntEnum):
    """Supported symmetric key algorithms."""
    Plaintext = 0x00
    #: .. warning:: IDEA is insecure. PGPy only allows it to be used for decryption, not encryption!
    IDEA = 0x01
    #: Triple-DES with 168-bit key derived from 192
    TripleDES = 0x02
    #: CAST5 (or CAST-128) with 128-bit key
    CAST5 = 0x03
    #: Blowfish with 128-bit key and 16 rounds
    Blowfish = 0x04
    #: AES with 128-bit key
    AES128 = 0x07
    #: AES with 192-bit key
    AES192 = 0x08
    #: AES with 256-bit key
    AES256 = 0x09
    # Twofish with 256-bit key - not currently supported
    Twofish256 = 0x0A
    #: Camellia with 128-bit key
    Camellia128 = 0x0B
    #: Camellia with 192-bit key
    Camellia192 = 0x0C
    #: Camellia with 256-bit key
    Camellia256 = 0x0D

    @property
    def cipher(self):
        bs = {SymmetricKeyAlgorithm.IDEA: algorithms.IDEA,
              SymmetricKeyAlgorithm.TripleDES: algorithms.TripleDES,
              SymmetricKeyAlgorithm.CAST5: algorithms.CAST5,
              SymmetricKeyAlgorithm.Blowfish: algorithms.Blowfish,
              SymmetricKeyAlgorithm.AES128: algorithms.AES,
              SymmetricKeyAlgorithm.AES192: algorithms.AES,
              SymmetricKeyAlgorithm.AES256: algorithms.AES,
              SymmetricKeyAlgorithm.Twofish256: namedtuple('Twofish256', ['block_size'])(block_size=128),
              SymmetricKeyAlgorithm.Camellia128: algorithms.Camellia,
              SymmetricKeyAlgorithm.Camellia192: algorithms.Camellia,
              SymmetricKeyAlgorithm.Camellia256: algorithms.Camellia}

        if self in bs:
            return bs[self]

        raise NotImplementedError(repr(self))

    @property
    def is_supported(self):
        return callable(self.cipher)

    @property
    def is_insecure(self):
        insecure_ciphers = {SymmetricKeyAlgorithm.IDEA}
        return self in insecure_ciphers

    @property
    def block_size(self):
        return self.cipher.block_size

    @property
    def key_size(self):
        ks = {SymmetricKeyAlgorithm.IDEA: 128,
              SymmetricKeyAlgorithm.TripleDES: 192,
              SymmetricKeyAlgorithm.CAST5: 128,
              SymmetricKeyAlgorithm.Blowfish: 128,
              SymmetricKeyAlgorithm.AES128: 128,
              SymmetricKeyAlgorithm.AES192: 192,
              SymmetricKeyAlgorithm.AES256: 256,
              SymmetricKeyAlgorithm.Twofish256: 256,
              SymmetricKeyAlgorithm.Camellia128: 128,
              SymmetricKeyAlgorithm.Camellia192: 192,
              SymmetricKeyAlgorithm.Camellia256: 256}

        if self in ks:
            return ks[self]

        raise NotImplementedError(repr(self))

    def gen_iv(self):
        return os.urandom(self.block_size // 8)

    def gen_key(self):
        return os.urandom(self.key_size // 8)


class PubKeyAlgorithm(IntEnum):
    """Supported public key algorithms."""
    Invalid = 0x00
    #: Signifies that a key is an RSA key.
    RSAEncryptOrSign = 0x01
    RSAEncrypt = 0x02  # deprecated
    RSASign = 0x03     # deprecated
    #: Signifies that a key is an ElGamal key.
    ElGamal = 0x10
    #: Signifies that a key is a DSA key.
    DSA = 0x11
    #: Signifies that a key is an ECDH key.
    ECDH = 0x12
    #: Signifies that a key is an ECDSA key.
    ECDSA = 0x13
    FormerlyElGamalEncryptOrSign = 0x14  # deprecated - do not generate
    DiffieHellman = 0x15  # X9.42
    EdDSA = 0x16  # https://tools.ietf.org/html/draft-koch-eddsa-for-openpgp-04

    @property
    def can_gen(self):
        return self in {PubKeyAlgorithm.RSAEncryptOrSign,
                        PubKeyAlgorithm.DSA,
                        PubKeyAlgorithm.ECDSA,
                        PubKeyAlgorithm.ECDH,
                        PubKeyAlgorithm.EdDSA}

    @property
    def can_encrypt(self):  # pragma: no cover
        return self in {PubKeyAlgorithm.RSAEncryptOrSign, PubKeyAlgorithm.ElGamal, PubKeyAlgorithm.ECDH}

    @property
    def can_sign(self):
        return self in {PubKeyAlgorithm.RSAEncryptOrSign, PubKeyAlgorithm.DSA, PubKeyAlgorithm.ECDSA, PubKeyAlgorithm.EdDSA}

    @property
    def deprecated(self):
        return self in {PubKeyAlgorithm.RSAEncrypt,
                        PubKeyAlgorithm.RSASign,
                        PubKeyAlgorithm.FormerlyElGamalEncryptOrSign}

    def validate_params(self, size):
        min_size = MINIMUM_ASYMMETRIC_KEY_LENGTHS.get(self)
        if min_size is not None:
            if isinstance(min_size, set):
                # ECC
                curve = size
                safe_curves = min_size
                if curve in safe_curves:
                    return SecurityIssues.OK
                else:
                    return SecurityIssues.InsecureCurve
            else:
                # not ECC
                if size >= min_size:
                    return SecurityIssues.OK
                else:
                    return SecurityIssues.AsymmetricKeyLengthIsTooShort
        # min_size is None
        return SecurityIssues.BrokenAsymmetricFunc


class CompressionAlgorithm(IntEnum):
    """Supported compression algorithms."""
    #: No compression
    Uncompressed = 0x00
    #: ZIP DEFLATE
    ZIP = 0x01
    #: ZIP DEFLATE with zlib headers
    ZLIB = 0x02
    #: Bzip2
    BZ2 = 0x03

    def compress(self, data):
        if self is CompressionAlgorithm.Uncompressed:
            return data

        if self is CompressionAlgorithm.ZIP:
            return zlib.compress(data)[2:-4]

        if self is CompressionAlgorithm.ZLIB:
            return zlib.compress(data)

        if self is CompressionAlgorithm.BZ2:
            return bz2.compress(data)

        raise NotImplementedError(self)

    def decompress(self, data):
        if self is CompressionAlgorithm.Uncompressed:
            return data

        if self is CompressionAlgorithm.ZIP:
            return zlib.decompress(data, -15)

        if self is CompressionAlgorithm.ZLIB:
            return zlib.decompress(data)

        if self is CompressionAlgorithm.BZ2:
            return bz2.decompress(data)

        raise NotImplementedError(self)


class HashAlgorithm(IntEnum):
    """Supported hash algorithms."""
    Invalid = 0x00
    MD5 = 0x01
    SHA1 = 0x02
    RIPEMD160 = 0x03
    _reserved_1 = 0x04
    _reserved_2 = 0x05
    _reserved_3 = 0x06
    _reserved_4 = 0x07
    SHA256 = 0x08
    SHA384 = 0x09
    SHA512 = 0x0A
    SHA224 = 0x0B
    #SHA3_256 = 13
    #SHA3_384 = 14
    #SHA3_512 = 15

    def __init__(self, *args):
        super(self.__class__, self).__init__()
        self._tuned_count = 255

    @property
    def hasher(self):
        return hashlib.new(self.name)

    @property
    def digest_size(self):
        return self.hasher.digest_size

    @property
    def tuned_count(self):
        return self._tuned_count

    @property
    def is_supported(self):
        return True

    @property
    def is_second_preimage_resistant(self):
        return self in {HashAlgorithm.SHA1}

    @property
    def is_collision_resistant(self):
        return self in {HashAlgorithm.SHA256, HashAlgorithm.SHA384, HashAlgorithm.SHA512}

    @property
    def is_considered_secure(self):
        if self.is_collision_resistant:
            return SecurityIssues.OK

        warnings.warn('Hash function {hash} is not considered collision resistant'.format(hash=repr(self)))
        issues = SecurityIssues.HashFunctionNotCollisionResistant

        if not self.is_second_preimage_resistant:
            issues |= SecurityIssues.HashFunctionNotSecondPreimageResistant

        return issues


class RevocationReason(IntEnum):
    """Reasons explaining why a key or certificate was revoked."""
    #: No reason was specified. This is the default reason.
    NotSpecified = 0x00
    #: The key was superseded by a new key. Only meaningful when revoking a key.
    Superseded = 0x01
    #: Key material has been compromised. Only meaningful when revoking a key.
    Compromised = 0x02
    #: Key is retired and no longer used. Only meaningful when revoking a key.
    Retired = 0x03
    #: User ID information is no longer valid. Only meaningful when revoking a certification of a user id.
    UserID = 0x20


class ImageEncoding(IntEnum):
    Unknown = 0x00
    JPEG = 0x01

    @classmethod
    def encodingof(cls, imagebytes):
        if imagebytes[6:10] in (b'JFIF', b'Exif') or imagebytes[:4] == b'\xff\xd8\xff\xdb':
            return ImageEncoding.JPEG
        return ImageEncoding.Unknown  # pragma: no cover


class SignatureType(IntEnum):
    """Types of signatures that can be found in a Signature packet."""

    #: The signer either owns this document, created it, or certifies that it
    #: has not been modified.
    BinaryDocument = 0x00

    #: The signer either owns this document, created it, or certifies that it
    #: has not been modified.  The signature is calculated over the text
    #: data with its line endings converted to ``<CR><LF>``.
    CanonicalDocument = 0x01

    #: This signature is a signature of only its own subpacket contents.
    #: It is calculated identically to a signature over a zero-length
    #: ``BinaryDocument``.
    Standalone = 0x02

    #: The issuer of this certification does not make any particular
    #: claim as to how well the certifier has checked that the owner
    #: of the key is in fact the person described by the User ID.
    Generic_Cert = 0x10

    #: The issuer of this certification has not done any verification of
    #: the claim that the owner of this key is the User ID specified.
    Persona_Cert = 0x11

    #: The issuer of this certification has done some casual
    #: verification of the claim of identity.
    Casual_Cert = 0x12

    #: The issuer of this certification has done substantial
    #: verification of the claim of identity.
    Positive_Cert = 0x13

    #: This signature is issued by the primary key over itself and its user ID (or user attribute).
    #: See `draft-ietf-openpgp-rfc4880bis-08 <https://tools.ietf.org/html/draft-ietf-openpgp-rfc4880bis-08#section-5.2.1>`_
    Attestation = 0x16

    #: This signature is a statement by the top-level signing key that
    #: indicates that it owns the subkey.  This signature is calculated
    #: directly on the primary key and subkey, and not on any User ID or
    #: other packets.
    Subkey_Binding = 0x18

    #: This signature is a statement by a signing subkey, indicating
    #: that it is owned by the primary key and subkey. This signature
    #: is calculated the same way as a ``Subkey_Binding`` signature.
    PrimaryKey_Binding = 0x19

    #: A signature calculated directly on a key.  It binds the
    #: information in the Signature subpackets to the key, and is
    #: appropriate to be used for subpackets that provide information
    #: about the key, such as the Revocation Key subpacket.  It is also
    #: appropriate for statements that non-self certifiers want to make
    #: about the key itself, rather than the binding between a key and a
    #: name.
    DirectlyOnKey = 0x1F

    #: A signature calculated directly on the key being revoked.
    #: Only revocation signatures by the key being revoked, or by an
    #: authorized revocation key, should be considered valid revocation signatures.
    KeyRevocation = 0x20

    #: A signature calculated directly on the subkey being revoked.
    #: Only revocation signatures by the top-level signature key that is bound to this subkey,
    #: or by an authorized revocation key, should be considered valid revocation signatures.
    SubkeyRevocation = 0x28

    #: This signature revokes an earlier User ID certification signature or direct-key signature.
    #: It should be issued by the same key that issued the revoked signature or an authorized revocation key.
    #: The signature is computed over the same data as the certificate that it revokes.
    CertRevocation = 0x30

    #: This signature is only meaningful for the timestamp contained in it.
    Timestamp = 0x40

    #: This signature is a signature over some other OpenPGP Signature
    #: packet(s).  It is analogous to a notary seal on the signed data.
    ThirdParty_Confirmation = 0x50


class KeyServerPreferences(FlagEnum):
    NoModify = 0x80


class String2KeyType(IntEnum):
    Simple = 0
    Salted = 1
    Reserved = 2
    Iterated = 3
    GNUExtension = 101


class S2KGNUExtension(IntEnum):
    NoSecret = 1
    Smartcard = 2


class TrustLevel(IntEnum):
    Unknown = 0
    Expired = 1
    Undefined = 2
    Never = 3
    Marginal = 4
    Fully = 5
    Ultimate = 6


class KeyFlags(FlagEnum):
    """Flags that determine a key's capabilities."""
    #: Signifies that a key may be used to certify keys and user ids. Primary keys always have this, even if it is not specified.
    Certify = 0x01
    #: Signifies that a key may be used to sign messages and documents.
    Sign = 0x02
    #: Signifies that a key may be used to encrypt messages.
    EncryptCommunications = 0x04
    #: Signifies that a key may be used to encrypt storage. Currently equivalent to :py:obj:`~pgpy.constants.EncryptCommunications`.
    EncryptStorage = 0x08
    #: Signifies that the private component of a given key may have been split by a secret-sharing mechanism. Split
    #: keys are not currently supported by PGPy.
    Split = 0x10
    #: Signifies that a key may be used for authentication.
    Authentication = 0x20
    #: Signifies that the private component of a key may be in the possession of more than one person.
    MultiPerson = 0x80


class Features(FlagEnum):
    ModificationDetection = 0x01

    @classproperty
    def pgpy_features(cls):
        return Features.ModificationDetection


class RevocationKeyClass(FlagEnum):
    Sensitive = 0x40
    Normal = 0x80


class NotationDataFlags(FlagEnum):
    HumanReadable = 0x80


class TrustFlags(FlagEnum):
    Revoked = 0x20
    SubRevoked = 0x40
    Disabled = 0x80
    PendingCheck = 0x100


class SecurityIssues(IntFlag):
    OK = 0
    WrongSig = (1 << 0)
    Expired = (1 << 1)
    Disabled = (1 << 2)
    Revoked = (1 << 3)
    Invalid = (1 << 4)
    BrokenAsymmetricFunc = (1 << 5)
    HashFunctionNotCollisionResistant = (1 << 6)
    HashFunctionNotSecondPreimageResistant = (1 << 7)
    AsymmetricKeyLengthIsTooShort = (1 << 8)
    InsecureCurve = (1 << 9)
    NoSelfSignature = (1 << 10)

    @property
    def causes_signature_verify_to_fail(self):
        return self in {
            SecurityIssues.WrongSig,
            SecurityIssues.Expired,
            SecurityIssues.Disabled,
            SecurityIssues.Invalid,
            SecurityIssues.NoSelfSignature,
        }


# https://safecurves.cr.yp.to/
SAFE_CURVES = {
    EllipticCurveOID.Curve25519,
    EllipticCurveOID.Ed25519,
}

MINIMUM_ASYMMETRIC_KEY_LENGTHS = {
    PubKeyAlgorithm.RSAEncryptOrSign: 2048,
    PubKeyAlgorithm.RSASign: 2048,
    PubKeyAlgorithm.ElGamal: 2048,
    PubKeyAlgorithm.DSA: 2048,
    ##
    PubKeyAlgorithm.ECDSA: SAFE_CURVES,
    PubKeyAlgorithm.EdDSA: SAFE_CURVES,
    PubKeyAlgorithm.ECDH: SAFE_CURVES,
}
