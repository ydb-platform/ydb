""" signature.py

Signature SubPackets
"""
import binascii
import calendar
import warnings

from datetime import datetime
from datetime import timedelta
from datetime import timezone

from .types import EmbeddedSignatureHeader
from .types import Signature

from ...constants import CompressionAlgorithm
from ...constants import Features as _Features
from ...constants import HashAlgorithm
from ...constants import KeyFlags as _KeyFlags
from ...constants import KeyServerPreferences as _KeyServerPreferences
from ...constants import NotationDataFlags
from ...constants import PubKeyAlgorithm
from ...constants import RevocationKeyClass
from ...constants import RevocationReason
from ...constants import SymmetricKeyAlgorithm

from ...decorators import sdproperty

from ...types import Fingerprint


__all__ = ['URI',
           'FlagList',
           'ByteFlag',
           'Boolean',
           'CreationTime',
           'SignatureExpirationTime',
           'ExportableCertification',
           'TrustSignature',
           'RegularExpression',
           'Revocable',
           'KeyExpirationTime',
           'PreferredSymmetricAlgorithms',
           'RevocationKey',
           'Issuer',
           'NotationData',
           'PreferredHashAlgorithms',
           'PreferredCompressionAlgorithms',
           'KeyServerPreferences',
           'PreferredKeyServer',
           'PrimaryUserID',
           'Policy',
           'KeyFlags',
           'SignersUserID',
           'SubkeyBindingSignature',
           'ReasonForRevocation',
           'Features',
           'EmbeddedSignature',
           'IssuerFingerprint',
           'IntendedRecipient',
           'AttestedCertifications']


class URI(Signature):
    @sdproperty
    def uri(self):
        return self._uri

    @uri.register(str)
    @uri.register(str)
    def uri_str(self, val):
        self._uri = val

    @uri.register(bytearray)
    def uri_bytearray(self, val):
        self.uri = val.decode('latin-1')

    def __init__(self):
        super(URI, self).__init__()
        self.uri = ""

    def __bytearray__(self):
        _bytes = super(URI, self).__bytearray__()
        _bytes += self.uri.encode()
        return _bytes

    def parse(self, packet):
        super(URI, self).parse(packet)
        self.uri = packet[:(self.header.length - 1)]
        del packet[:(self.header.length - 1)]


class FlagList(Signature):
    __flags__ = None

    @sdproperty
    def flags(self):
        return self._flags

    @flags.register(list)
    @flags.register(tuple)
    def flags_list(self, val):
        self._flags = list(val)

    @flags.register(int)
    @flags.register(CompressionAlgorithm)
    @flags.register(HashAlgorithm)
    @flags.register(PubKeyAlgorithm)
    @flags.register(SymmetricKeyAlgorithm)
    def flags_int(self, val):
        if self.__flags__ is None:  # pragma: no cover
            raise AttributeError("Error: __flags__ not set!")

        self._flags.append(self.__flags__(val))

    @flags.register(bytearray)
    def flags_bytearray(self, val):
        self.flags = self.bytes_to_int(val)

    def __init__(self):
        super(FlagList, self).__init__()
        self.flags = []

    def __bytearray__(self):
        _bytes = super(FlagList, self).__bytearray__()
        _bytes += b''.join(self.int_to_bytes(b) for b in self.flags)
        return _bytes

    def parse(self, packet):
        super(FlagList, self).parse(packet)
        for i in range(0, self.header.length - 1):
            self.flags = packet[:1]
            del packet[:1]


class ByteFlag(Signature):
    __flags__ = None

    @sdproperty
    def flags(self):
        return self._flags

    @flags.register(set)
    @flags.register(list)
    def flags_seq(self, val):
        self._flags = set(val)

    @flags.register(int)
    @flags.register(_KeyFlags)
    @flags.register(_Features)
    def flags_int(self, val):
        if self.__flags__ is None:  # pragma: no cover
            raise AttributeError("Error: __flags__ not set!")

        self._flags |= (self.__flags__ & val)

    @flags.register(bytearray)
    def flags_bytearray(self, val):
        self.flags = self.bytes_to_int(val)

    def __init__(self):
        super(ByteFlag, self).__init__()
        self.flags = []

    def __bytearray__(self):
        _bytes = super(ByteFlag, self).__bytearray__()
        _bytes += self.int_to_bytes(sum(self.flags))
        # null-pad _bytes if they are not up to the end now
        if len(_bytes) < len(self):
            _bytes += b'\x00' * (len(self) - len(_bytes))
        return _bytes

    def parse(self, packet):
        super(ByteFlag, self).parse(packet)
        for i in range(0, self.header.length - 1):
            self.flags = packet[:1]
            del packet[:1]


class Boolean(Signature):
    @sdproperty
    def bflag(self):
        return self._bool

    @bflag.register(bool)
    def bflag_bool(self, val):
        self._bool = val

    @bflag.register(bytearray)
    def bflag_bytearray(self, val):
        self.bool = bool(self.bytes_to_int(val))

    def __init__(self):
        super(Boolean, self).__init__()
        self.bflag = False

    def __bytearray__(self):
        _bytes = super(Boolean, self).__bytearray__()
        _bytes += self.int_to_bytes(int(self.bflag))
        return _bytes

    def __bool__(self):
        return self.bflag

    def __nonzero__(self):
        return self.__bool__()

    def parse(self, packet):
        super(Boolean, self).parse(packet)
        self.bflag = packet[:1]
        del packet[:1]


class CreationTime(Signature):
    """
    5.2.3.4.  Signature Creation Time

    (4-octet time field)

    The time the signature was made.

    MUST be present in the hashed area.
   """
    __typeid__ = 0x02

    @sdproperty
    def created(self):
        return self._created

    @created.register(datetime)
    def created_datetime(self, val):
        if val.tzinfo is None:
            warnings.warn("Passing TZ-naive datetime object to CreationTime subpacket")
        self._created = val

    @created.register(int)
    def created_int(self, val):
        self.created = datetime.fromtimestamp(val, timezone.utc)

    @created.register(bytearray)
    def created_bytearray(self, val):
        self.created = self.bytes_to_int(val)

    def __init__(self):
        super(CreationTime, self).__init__()
        self.created = datetime.now(timezone.utc)

    def __bytearray__(self):
        _bytes = super(CreationTime, self).__bytearray__()
        _bytes += self.int_to_bytes(calendar.timegm(self.created.utctimetuple()), 4)
        return _bytes

    def parse(self, packet):
        super(CreationTime, self).parse(packet)
        self.created = packet[:4]
        del packet[:4]


class SignatureExpirationTime(Signature):
    """
    5.2.3.10.  Signature Expiration Time

    (4-octet time field)

    The validity period of the signature.  This is the number of seconds
    after the signature creation time that the signature expires.  If
    this is not present or has a value of zero, it never expires.
    """
    __typeid__ = 0x03

    @sdproperty
    def expires(self):
        return self._expires

    @expires.register(timedelta)
    def expires_timedelta(self, val):
        self._expires = val

    @expires.register(int)
    def expires_int(self, val):
        self.expires = timedelta(seconds=val)

    @expires.register(bytearray)
    def expires_bytearray(self, val):
        self.expires = self.bytes_to_int(val)

    def __init__(self):
        super(SignatureExpirationTime, self).__init__()
        self.expires = 0

    def __bytearray__(self):
        _bytes = super(SignatureExpirationTime, self).__bytearray__()
        _bytes += self.int_to_bytes(int(self.expires.total_seconds()), 4)
        return _bytes

    def parse(self, packet):
        super(SignatureExpirationTime, self).parse(packet)
        self.expires = packet[:4]
        del packet[:4]


class ExportableCertification(Boolean):
    """
    5.2.3.11.  Exportable Certification

    (1 octet of exportability, 0 for not, 1 for exportable)

    This subpacket denotes whether a certification signature is
    "exportable", to be used by other users than the signature's issuer.
    The packet body contains a Boolean flag indicating whether the
    signature is exportable.  If this packet is not present, the
    certification is exportable; it is equivalent to a flag containing a
    1.

    Non-exportable, or "local", certifications are signatures made by a
    user to mark a key as valid within that user's implementation only.
    Thus, when an implementation prepares a user's copy of a key for
    transport to another user (this is the process of "exporting" the
    key), any local certification signatures are deleted from the key.

    The receiver of a transported key "imports" it, and likewise trims
    any local certifications.  In normal operation, there won't be any,
    assuming the import is performed on an exported key.  However, there
    are instances where this can reasonably happen.  For example, if an
    implementation allows keys to be imported from a key database in
    addition to an exported key, then this situation can arise.

    Some implementations do not represent the interest of a single user
    (for example, a key server).  Such implementations always trim local
    certifications from any key they handle.
    """
    __typeid__ = 0x04


class TrustSignature(Signature):
    """
    5.2.3.13.  Trust Signature

    (1 octet "level" (depth), 1 octet of trust amount)

    Signer asserts that the key is not only valid but also trustworthy at
    the specified level.  Level 0 has the same meaning as an ordinary
    validity signature.  Level 1 means that the signed key is asserted to
    be a valid trusted introducer, with the 2nd octet of the body
    specifying the degree of trust.  Level 2 means that the signed key is
    asserted to be trusted to issue level 1 trust signatures, i.e., that
    it is a "meta introducer".  Generally, a level n trust signature
    asserts that a key is trusted to issue level n-1 trust signatures.
    The trust amount is in a range from 0-255, interpreted such that
    values less than 120 indicate partial trust and values of 120 or
    greater indicate complete trust.  Implementations SHOULD emit values
    of 60 for partial trust and 120 for complete trust.
    """
    __typeid__ = 0x05

    @sdproperty
    def level(self):
        return self._level

    @level.register(int)
    def level_int(self, val):
        self._level = val

    @level.register(bytearray)
    def level_bytearray(self, val):
        self.level = self.bytes_to_int(val)

    @sdproperty
    def amount(self):
        return self._amount

    @amount.register(int)
    def amount_int(self, val):
        # clamp 'val' to the range 0-255
        self._amount = max(0, min(val, 255))

    @amount.register(bytearray)
    def amount_bytearray(self, val):
        self.amount = self.bytes_to_int(val)

    def __init__(self):
        super(TrustSignature, self).__init__()
        self.level = 0
        self.amount = 0

    def __bytearray__(self):
        _bytes = super(TrustSignature, self).__bytearray__()
        _bytes += self.int_to_bytes(self.level)
        _bytes += self.int_to_bytes(self.amount)
        return _bytes

    def parse(self, packet):
        super(TrustSignature, self).parse(packet)
        self.level = packet[:1]
        del packet[:1]
        self.amount = packet[:1]
        del packet[:1]


class RegularExpression(Signature):
    """
    5.2.3.14.  Regular Expression

    (null-terminated regular expression)

    Used in conjunction with trust Signature packets (of level > 0) to
    limit the scope of trust that is extended.  Only signatures by the
    target key on User IDs that match the regular expression in the body
    of this packet have trust extended by the trust Signature subpacket.
    The regular expression uses the same syntax as the Henry Spencer's
    "almost public domain" regular expression [REGEX] package.  A
    description of the syntax is found in Section 8 below.
    """
    __typeid__ = 0x06

    @sdproperty
    def regex(self):
        return self._regex

    @regex.register(str)
    @regex.register(str)
    def regex_str(self, val):
        self._regex = val

    @regex.register(bytearray)
    def regex_bytearray(self, val):
        self.regex = val.decode('latin-1')

    def __init__(self):
        super(RegularExpression, self).__init__()
        self.regex = r''

    def __bytearray__(self):
        _bytes = super(RegularExpression, self).__bytearray__()
        _bytes += self.regex.encode()
        return _bytes

    def parse(self, packet):
        super(RegularExpression, self).parse(packet)
        self.regex = packet[:(self.header.length - 1)]
        del packet[:(self.header.length - 1)]


class Revocable(Boolean):
    """
    5.2.3.12.  Revocable

    (1 octet of revocability, 0 for not, 1 for revocable)

    Signature's revocability status.  The packet body contains a Boolean
    flag indicating whether the signature is revocable.  Signatures that
    are not revocable have any later revocation signatures ignored.  They
    represent a commitment by the signer that he cannot revoke his
    signature for the life of his key.  If this packet is not present,
    the signature is revocable.
    """
    __typeid__ = 0x07


class KeyExpirationTime(SignatureExpirationTime):
    """
    5.2.3.6.  Key Expiration Time

    (4-octet time field)

    The validity period of the key.  This is the number of seconds after
    the key creation time that the key expires.  If this is not present
    or has a value of zero, the key never expires.  This is found only on
    a self-signature.
    """
    __typeid__ = 0x09


class PreferredSymmetricAlgorithms(FlagList):
    """
    5.2.3.7.  Preferred Symmetric Algorithms

    (array of one-octet values)

    Symmetric algorithm numbers that indicate which algorithms the key
    holder prefers to use.  The subpacket body is an ordered list of
    octets with the most preferred listed first.  It is assumed that only
    algorithms listed are supported by the recipient's software.
    Algorithm numbers are in Section 9.  This is only found on a self-
    signature.
    """
    __typeid__ = 0x0B
    __flags__ = SymmetricKeyAlgorithm


class RevocationKey(Signature):
    """
    5.2.3.15.  Revocation Key

    (1 octet of class, 1 octet of public-key algorithm ID, 20 octets of
    fingerprint)

    Authorizes the specified key to issue revocation signatures for this
    key.  Class octet must have bit 0x80 set.  If the bit 0x40 is set,
    then this means that the revocation information is sensitive.  Other
    bits are for future expansion to other kinds of authorizations.  This
    is found on a self-signature.

    If the "sensitive" flag is set, the keyholder feels this subpacket
    contains private trust information that describes a real-world
    sensitive relationship.  If this flag is set, implementations SHOULD
    NOT export this signature to other users except in cases where the
    data needs to be available: when the signature is being sent to the
    designated revoker, or when it is accompanied by a revocation
    signature from that revoker.  Note that it may be appropriate to
    isolate this subpacket within a separate signature so that it is not
    combined with other subpackets that need to be exported.
    """
    __typeid__ = 0x0C

    @sdproperty
    def keyclass(self):
        return self._keyclass

    @keyclass.register(list)
    def keyclass_list(self, val):
        self._keyclass = val

    @keyclass.register(int)
    @keyclass.register(RevocationKeyClass)
    def keyclass_int(self, val):
        self._keyclass += RevocationKeyClass & val

    @keyclass.register(bytearray)
    def keyclass_bytearray(self, val):
        self.keyclass = self.bytes_to_int(val)

    @sdproperty
    def algorithm(self):
        return self._algorithm

    @algorithm.register(int)
    @algorithm.register(PubKeyAlgorithm)
    def algorithm_int(self, val):
        self._algorithm = PubKeyAlgorithm(val)

    @algorithm.register(bytearray)
    def algorithm_bytearray(self, val):
        self.algorithm = self.bytes_to_int(val)

    @sdproperty
    def fingerprint(self):
        return self._fingerprint

    @fingerprint.register(str)
    @fingerprint.register(str)
    @fingerprint.register(Fingerprint)
    def fingerprint_str(self, val):
        self._fingerprint = Fingerprint(val)

    @fingerprint.register(bytearray)
    def fingerprint_bytearray(self, val):
        self.fingerprint = ''.join('{:02x}'.format(c) for c in val).upper()

    def __init__(self):
        super(RevocationKey, self).__init__()
        self.keyclass = []
        self.algorithm = PubKeyAlgorithm.Invalid
        self._fingerprint = ""

    def __bytearray__(self):
        _bytes = super(RevocationKey, self).__bytearray__()
        _bytes += self.int_to_bytes(sum(self.keyclass))
        _bytes += self.int_to_bytes(self.algorithm.value)
        _bytes += self.fingerprint.__bytes__()
        return _bytes

    def parse(self, packet):
        super(RevocationKey, self).parse(packet)
        self.keyclass = packet[:1]
        del packet[:1]
        self.algorithm = packet[:1]
        del packet[:1]
        self.fingerprint = packet[:20]
        del packet[:20]


class Issuer(Signature):
    __typeid__ = 0x10

    @sdproperty
    def issuer(self):
        return self._issuer

    @issuer.register(bytearray)
    def issuer_bytearray(self, val):
        self._issuer = binascii.hexlify(val).upper().decode('latin-1')

    def __init__(self):
        super(Issuer, self).__init__()
        self.issuer = bytearray()

    def __bytearray__(self):
        _bytes = super(Issuer, self).__bytearray__()
        _bytes += binascii.unhexlify(self._issuer.encode())
        return _bytes

    def parse(self, packet):
        super(Issuer, self).parse(packet)
        self.issuer = packet[:8]
        del packet[:8]


class NotationData(Signature):
    __typeid__ = 0x14

    @sdproperty
    def flags(self):
        return self._flags

    @flags.register(list)
    def flags_list(self, val):
        self._flags = val

    @flags.register(int)
    @flags.register(NotationDataFlags)
    def flags_int(self, val):
        self.flags += NotationDataFlags & val

    @flags.register(bytearray)
    def flags_bytearray(self, val):
        self.flags = self.bytes_to_int(val)

    @sdproperty
    def name(self):
        return self._name

    @name.register(str)
    @name.register(str)
    def name_str(self, val):
        self._name = val

    @name.register(bytearray)
    def name_bytearray(self, val):
        self.name = val.decode('latin-1')

    @sdproperty
    def value(self):
        return self._value

    @value.register(str)
    @value.register(str)
    def value_str(self, val):
        self._value = val

    @value.register(bytearray)
    def value_bytearray(self, val):
        if NotationDataFlags.HumanReadable in self.flags:
            self.value = val.decode('latin-1')

        else:  # pragma: no cover
            self._value = val

    def __init__(self):
        super(NotationData, self).__init__()
        self.flags = [0, 0, 0, 0]
        self.name = ""
        self.value = ""

    def __bytearray__(self):
        _bytes = super(NotationData, self).__bytearray__()
        _bytes += self.int_to_bytes(sum(self.flags)) + b'\x00\x00\x00'
        _bytes += self.int_to_bytes(len(self.name), 2)
        _bytes += self.int_to_bytes(len(self.value), 2)
        _bytes += self.name.encode()
        _bytes += self.value if isinstance(self.value, bytearray) else self.value.encode()
        return bytes(_bytes)

    def parse(self, packet):
        super(NotationData, self).parse(packet)
        self.flags = packet[:1]
        del packet[:4]
        nlen = self.bytes_to_int(packet[:2])
        del packet[:2]
        vlen = self.bytes_to_int(packet[:2])
        del packet[:2]
        self.name = packet[:nlen]
        del packet[:nlen]
        self.value = packet[:vlen]
        del packet[:vlen]


class PreferredHashAlgorithms(FlagList):
    __typeid__ = 0x15
    __flags__ = HashAlgorithm


class PreferredCompressionAlgorithms(FlagList):
    __typeid__ = 0x16
    __flags__ = CompressionAlgorithm


class KeyServerPreferences(ByteFlag):
    __typeid__ = 0x17
    __flags__ = _KeyServerPreferences


class PreferredKeyServer(URI):
    __typeid__ = 0x18


class SubkeyBindingSignature(Signature):
    __typeid__ = 0x18


class PrimaryUserID(SubkeyBindingSignature):
    __typeid__ = 0x19

    @sdproperty
    def primary(self):
        return self._primary

    @primary.register(bool)
    def primary_bool(self, val):
        self._primary = val

    @primary.register(bytearray)
    def primary_byrearray(self, val):
        self.primary = bool(self.bytes_to_int(val))

    def __init__(self):
        super(PrimaryUserID, self).__init__()
        self.primary = True

    def __bytearray__(self):
        _bytes = super(PrimaryUserID, self).__bytearray__()
        _bytes += self.int_to_bytes(int(self.primary))
        return _bytes

    def __bool__(self):
        return self.primary

    def __nonzero__(self):
        return self.__bool__()

    def parse(self, packet):
        super(PrimaryUserID, self).parse(packet)
        self.primary = packet[:1]
        del packet[:1]


class Policy(URI):
    __typeid__ = 0x1a


class KeyFlags(ByteFlag):
    __typeid__ = 0x1B
    __flags__ = _KeyFlags


class SignersUserID(Signature):
    __typeid__ = 0x1C

    @sdproperty
    def userid(self):
        return self._userid

    @userid.register(str)
    @userid.register(str)
    def userid_str(self, val):
        self._userid = val

    @userid.register(bytearray)
    def userid_bytearray(self, val):
        self.userid = val.decode('latin-1')

    def __init__(self):
        super(SignersUserID, self).__init__()
        self.userid = ""

    def __bytearray__(self):
        _bytes = super(SignersUserID, self).__bytearray__()
        _bytes += self.userid.encode()
        return _bytes

    def parse(self, packet):
        super(SignersUserID, self).parse(packet)
        self.userid = packet[:(self.header.length - 1)]
        del packet[:(self.header.length - 1)]


class ReasonForRevocation(Signature):
    __typeid__ = 0x1D

    @sdproperty
    def code(self):
        return self._code

    @code.register(int)
    @code.register(RevocationReason)
    def code_int(self, val):
        self._code = RevocationReason(val)

    @code.register(bytearray)
    def code_bytearray(self, val):
        self.code = self.bytes_to_int(val)

    @sdproperty
    def string(self):
        return self._string

    @string.register(str)
    @string.register(str)
    def string_str(self, val):
        self._string = val

    @string.register(bytearray)
    def string_bytearray(self, val):
        self.string = val.decode('latin-1')

    def __init__(self):
        super(ReasonForRevocation, self).__init__()
        self.code = 0x00
        self.string = ""

    def __bytearray__(self):
        _bytes = super(ReasonForRevocation, self).__bytearray__()
        _bytes += self.int_to_bytes(self.code)
        _bytes += self.string.encode()
        return _bytes

    def parse(self, packet):
        super(ReasonForRevocation, self).parse(packet)
        self.code = packet[:1]
        del packet[:1]
        self.string = packet[:(self.header.length - 2)]
        del packet[:(self.header.length - 2)]


class Features(ByteFlag):
    __typeid__ = 0x1E
    __flags__ = _Features


##TODO: obtain subpacket type 0x1F - Signature Target


class EmbeddedSignature(Signature):
    __typeid__ = 0x20

    @sdproperty
    def _sig(self):
        return self._sigpkt

    @_sig.setter
    def _sig(self, val):
        esh = EmbeddedSignatureHeader()
        esh.version = val.header.version
        val.header = esh
        val.update_hlen()
        self._sigpkt = val

    @property
    def sigtype(self):
        return self._sig.sigtype

    @property
    def pubalg(self):
        return self._sig.pubalg

    @property
    def halg(self):
        return self._sig.halg

    @property
    def subpackets(self):
        return self._sig.subpackets

    @property
    def hash2(self):  # pragma: no cover
        return self._sig.hash2

    @property
    def signature(self):
        return self._sig.signature

    @property
    def signer(self):
        return self._sig.signer

    def __init__(self):
        super(EmbeddedSignature, self).__init__()
        from ..packets import SignatureV4
        self._sigpkt = SignatureV4()
        self._sigpkt.header = EmbeddedSignatureHeader()

    def __bytearray__(self):
        return super(EmbeddedSignature, self).__bytearray__() + self._sigpkt.__bytearray__()

    def parse(self, packet):
        super(EmbeddedSignature, self).parse(packet)
        self._sig.parse(packet)


class IssuerFingerprint(Signature):
    '''
    (from RFC4880bis-07)
    5.2.3.28.  Issuer Fingerprint

    (1 octet key version number, N octets of fingerprint)

    The OpenPGP Key fingerprint of the key issuing the signature.  This
    subpacket SHOULD be included in all signatures.  If the version of
    the issuing key is 4 and an Issuer subpacket is also included in the
    signature, the key ID of the Issuer subpacket MUST match the low 64
    bits of the fingerprint.

    Note that the length N of the fingerprint for a version 4 key is 20
    octets; for a version 5 key N is 32.
    '''
    __typeid__ = 0x21

    @sdproperty
    def version(self):
        return self._version

    @version.register(int)
    def version_int(self, val):
        self._version = val

    @version.register(bytearray)
    def version_bytearray(self, val):
        self.version = self.bytes_to_int(val)

    @sdproperty
    def issuer_fingerprint(self):
        return self._issuer_fpr

    @issuer_fingerprint.register(str)
    @issuer_fingerprint.register(str)
    @issuer_fingerprint.register(Fingerprint)
    def issuer_fingerprint_str(self, val):
        self._issuer_fpr = Fingerprint(val)

    @issuer_fingerprint.register(bytearray)
    def issuer_fingerprint_bytearray(self, val):
        self.issuer_fingerprint = ''.join('{:02x}'.format(c) for c in val).upper()

    def __init__(self):
        super(IssuerFingerprint, self).__init__()
        self.version = 4
        self._issuer_fpr = ""

    def __bytearray__(self):
        _bytes = super(IssuerFingerprint, self).__bytearray__()
        _bytes += self.int_to_bytes(self.version)
        _bytes += self.issuer_fingerprint.__bytes__()
        return _bytes

    def parse(self, packet):
        super(IssuerFingerprint, self).parse(packet)
        self.version = packet[:1]
        del packet[:1]

        if self.version == 4:
            fpr_len = 20
        elif self.version == 5:  # pragma: no cover
            fpr_len = 32
        else:  # pragma: no cover
            fpr_len = self.header.length - 1

        self.issuer_fingerprint = packet[:fpr_len]
        del packet[:fpr_len]


class IntendedRecipient(Signature):
    '''
    (from RFC4880bis-08)
    5.2.3.29. Intended Recipient

    (1 octet key version number, N octets of fingerprint)

    The OpenPGP Key fingerprint of the intended recipient primary key.
    If one or more subpackets of this type are included in a signature,
    it SHOULD be considered valid only in an encrypted context, where the
    key it was encrypted to is one of the indicated primary keys, or one
    of their subkeys.  This can be used to prevent forwarding a signature
    outside of its intended, encrypted context.

    Note that the length N of the fingerprint for a version 4 key is 20
    octets; for a version 5 key N is 32.
    '''
    __typeid__ = 0x23

    @sdproperty
    def version(self):
        return self._version

    @version.register(int)
    def version_int(self, val):
        self._version = val

    @version.register(bytearray)
    def version_bytearray(self, val):
        self.version = self.bytes_to_int(val)

    @sdproperty
    def intended_recipient(self):
        return self._intended_recipient

    @intended_recipient.register(str)
    @intended_recipient.register(str)
    @intended_recipient.register(Fingerprint)
    def intended_recipient_str(self, val):
        self._intended_recipient = Fingerprint(val)

    @intended_recipient.register(bytearray)
    def intended_recipient_bytearray(self, val):
        self.intended_recipient = ''.join('{:02x}'.format(c) for c in val).upper()

    def __init__(self):
        super(IntendedRecipient, self).__init__()
        self.version = 4
        self._intended_recipient = ""

    def __bytearray__(self):
        _bytes = super(IntendedRecipient, self).__bytearray__()
        _bytes += self.int_to_bytes(self.version)
        _bytes += self.intended_recipient.__bytes__()
        return _bytes

    def parse(self, packet):
        super(IntendedRecipient, self).parse(packet)
        self.version = packet[:1]
        del packet[:1]

        if self.version == 4:
            fpr_len = 20
        elif self.version == 5:  # pragma: no cover
            fpr_len = 32
        else:  # pragma: no cover
            fpr_len = self.header.length - 1

        self.intended_recipient = packet[:fpr_len]
        del packet[:fpr_len]


class AttestedCertifications(Signature):
    '''
    (from RFC4880bis-08)
    5.2.3.30. Attested Certifications

    (N octets of certification digests)

    This subpacket MUST only appear as a hashed subpacket of an
    Attestation Key Signature.  It has no meaning in any other signature
    type.  It is used by the primary key to attest to a set of third-
    party certifications over the associated User ID or User Attribute.
    This enables the holder of an OpenPGP primary key to mark specific
    third-party certifications as re-distributable with the rest of the
    Transferable Public Key (see the "No-modify" flag in "Key Server
    Preferences", above).  Implementations MUST include exactly one
    Attested Certification subpacket in any generated Attestation Key
    Signature.

    The contents of the subpacket consists of a series of digests using
    the same hash algorithm used by the signature itself.  Each digest is
    made over one third-party signature (any Certification, i.e.,
    signature type 0x10-0x13) that covers the same Primary Key and User
    ID (or User Attribute).  For example, an Attestation Key Signature
    made by key X over user ID U using hash algorithm SHA256 might
    contain an Attested Certifications subpacket of 192 octets (6*32
    octets) covering six third-party certification Signatures over <X,U>.
    They SHOULD be ordered by binary hash value from low to high (e.g., a
    hash with hexadecimal value 037a... precedes a hash with value
    0392..., etc).  The length of this subpacket MUST be an integer
    multiple of the length of the hash algorithm used for the enclosing
    Attestation Key Signature.

    The listed digests MUST be calculated over the third-party
    certification's Signature packet as described in the "Computing
    Signatures" section, but without a trailer: the hash data starts with
    the octet 0x88, followed by the four-octet length of the Signature,
    and then the body of the Signature packet.  (Note that this is an
    old-style packet header for a Signature packet with the length-of-
    length field set to zero.)  The unhashed subpacket data of the
    Signature packet being hashed is not included in the hash, and the
    unhashed subpacket data length value is set to zero.

    If an implementation encounters more than one such subpacket in an
    Attestation Key Signature, it MUST treat it as a single Attested
    Certifications subpacket containing the union of all hashes.

    The Attested Certifications subpacket in the most recent Attestation
    Key Signature over a given user ID supersedes all Attested
    Certifications subpackets from any previous Attestation Key
    Signature.  However, note that if more than one Attestation Key
    Signatures has the same (most recent) Signature Creation Time
    subpacket, implementations MUST consider the union of the
    attestations of all Attestation Key Signatures (this allows the
    keyholder to attest to more third-party certifications than could fit
    in a single Attestation Key Signature).

    If a keyholder Alice has already attested to third-party
    certifications from Bob and Carol and she wants to add an attestation
    to a certification from David, she should issue a new Attestation Key
    Signature (with a more recent Signature Creation timestamp) that
    contains an Attested Certifications subpacket covering all three
    third-party certifications.

    If she later decides that she does not want Carol's certification to
    be redistributed with her certificate, she can issue a new
    Attestation Key Signature (again, with a more recent Signature
    Creation timestamp) that contains an Attested Certifications
    subpacket covering only the certifications from Bob and David.

    Note that Certification Revocation Signatures are not relevant for
    Attestation Key Signatures.  To rescind all attestations, the primary
    key holder needs only to publish a more recent Attestation Key
    Signature with an empty Attested Certifications subpacket.
    '''
    __typeid__ = 0x25

    @sdproperty
    def attested_certifications(self):
        return self._attested_certifications

    @attested_certifications.register(bytearray)
    @attested_certifications.register(bytes)
    def attested_certifications_bytearray(self, val):
        self._attested_certifications = val

    def __init__(self):
        super(AttestedCertifications, self).__init__()
        self._attested_certifications = bytearray()

    def __bytearray__(self):
        _bytes = super(AttestedCertifications, self).__bytearray__()
        _bytes += self._attested_certifications
        return _bytes

    def parse(self, packet):
        super(AttestedCertifications, self).parse(packet)
        self.attested_certifications = packet[:(self.header.length - 1)]
        del packet[:(self.header.length - 1)]
