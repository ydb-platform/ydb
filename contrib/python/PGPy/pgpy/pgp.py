""" pgp.py

this is where the armorable PGP block objects live
"""
import binascii
import collections
try:
    import collections.abc as collections_abc
except ImportError:
    collections_abc = collections
import contextlib
import copy
import functools
import itertools
import operator
import os
import re
import warnings
import weakref

from datetime import datetime, timezone

from cryptography.hazmat.primitives import hashes

from .constants import CompressionAlgorithm
from .constants import Features
from .constants import HashAlgorithm
from .constants import ImageEncoding
from .constants import KeyFlags
from .constants import NotationDataFlags
from .constants import PacketTag
from .constants import PubKeyAlgorithm
from .constants import RevocationKeyClass
from .constants import RevocationReason
from .constants import SignatureType
from .constants import SymmetricKeyAlgorithm
from .constants import SecurityIssues

from .decorators import KeyAction

from .errors import PGPDecryptionError
from .errors import PGPError

from .packet import Key
from .packet import MDC
from .packet import Packet
from .packet import Primary
from .packet import Private
from .packet import PubKeyV4
from .packet import PrivKeyV4
from .packet import PrivSubKeyV4
from .packet import Public
from .packet import Sub
from .packet import UserID
from .packet import UserAttribute

from .packet.packets import CompressedData
from .packet.packets import IntegrityProtectedSKEData
from .packet.packets import IntegrityProtectedSKEDataV1
from .packet.packets import LiteralData
from .packet.packets import OnePassSignature
from .packet.packets import OnePassSignatureV3
from .packet.packets import PKESessionKey
from .packet.packets import PKESessionKeyV3
from .packet.packets import Signature
from .packet.packets import SignatureV4
from .packet.packets import SKEData
from .packet.packets import Marker
from .packet.packets import SKESessionKey
from .packet.packets import SKESessionKeyV4

from .packet.types import Opaque

from .types import Armorable
from .types import Fingerprint
from .types import ParentRef
from .types import PGPObject
from .types import SignatureVerification
from .types import SorteDeque

__all__ = ['PGPSignature',
           'PGPUID',
           'PGPMessage',
           'PGPKey',
           'PGPKeyring']


class PGPSignature(Armorable, ParentRef, PGPObject):
    _reason_for_revocation = collections.namedtuple('ReasonForRevocation', ['code', 'comment'])

    @property
    def __sig__(self):
        return self._signature.signature.__sig__()

    @property
    def cipherprefs(self):
        """
        A ``list`` of preferred symmetric algorithms specified in this signature, if any. Otherwise, an empty ``list``.
        """
        if 'PreferredSymmetricAlgorithms' in self._signature.subpackets:
            return next(iter(self._signature.subpackets['h_PreferredSymmetricAlgorithms'])).flags
        return []

    @property
    def compprefs(self):
        """
        A ``list`` of preferred compression algorithms specified in this signature, if any. Otherwise, an empty ``list``.
        """
        if 'PreferredCompressionAlgorithms' in self._signature.subpackets:
            return next(iter(self._signature.subpackets['h_PreferredCompressionAlgorithms'])).flags
        return []

    @property
    def created(self):
        """
        A :py:obj:`~datetime.datetime` of when this signature was created.
        """
        return self._signature.subpackets['h_CreationTime'][-1].created

    @property
    def embedded(self):
        return self.parent is not None

    @property
    def expires_at(self):
        """
        A :py:obj:`~datetime.datetime` of when this signature expires, if a signature expiration date is specified.
        Otherwise, ``None``
        """
        if 'SignatureExpirationTime' in self._signature.subpackets:
            expd = next(iter(self._signature.subpackets['SignatureExpirationTime'])).expires
            return self.created + expd
        return None

    @property
    def exportable(self):
        """
        ``False`` if this signature is marked as being not exportable. Otherwise, ``True``.
        """
        if 'ExportableCertification' in self._signature.subpackets:
            return bool(next(iter(self._signature.subpackets['ExportableCertification'])))

        return True

    @property
    def features(self):
        """
        A ``set`` of implementation features specified in this signature, if any. Otherwise, an empty ``set``.
        """
        if 'Features' in self._signature.subpackets:
            return next(iter(self._signature.subpackets['Features'])).flags
        return set()

    @property
    def hash2(self):
        return self._signature.hash2

    @property
    def hashprefs(self):
        """
        A ``list`` of preferred hash algorithms specified in this signature, if any. Otherwise, an empty ``list``.
        """
        if 'PreferredHashAlgorithms' in self._signature.subpackets:
            return next(iter(self._signature.subpackets['h_PreferredHashAlgorithms'])).flags
        return []

    @property
    def hash_algorithm(self):
        """
        The :py:obj:`~constants.HashAlgorithm` used when computing this signature.
        """
        return self._signature.halg

    def check_primitives(self):
        return self.hash_algorithm.is_considered_secure

    def check_soundness(self):
        return self.check_primitives()

    @property
    def is_expired(self):
        """
        ``True`` if the signature has an expiration date, and is expired. Otherwise, ``False``
        """
        expires_at = self.expires_at
        if expires_at is not None and expires_at != self.created:
            return expires_at < datetime.now(timezone.utc)

        return False

    @property
    def key_algorithm(self):
        """
        The :py:obj:`~constants.PubKeyAlgorithm` of the key that generated this signature.
        """
        return self._signature.pubalg

    @property
    def key_expiration(self):
        if 'KeyExpirationTime' in self._signature.subpackets:
            return next(iter(self._signature.subpackets['KeyExpirationTime'])).expires
        return None

    @property
    def key_flags(self):
        """
        A ``set`` of :py:obj:`~constants.KeyFlags` specified in this signature, if any. Otherwise, an empty ``set``.
        """
        if 'KeyFlags' in self._signature.subpackets:
            return next(iter(self._signature.subpackets['h_KeyFlags'])).flags
        return set()

    @property
    def keyserver(self):
        """
        The preferred key server specified in this signature, if any. Otherwise, an empty ``str``.
        """
        if 'PreferredKeyServer' in self._signature.subpackets:
            return next(iter(self._signature.subpackets['h_PreferredKeyServer'])).uri
        return ''

    @property
    def keyserverprefs(self):
        """
        A ``list`` of :py:obj:`~constants.KeyServerPreferences` in this signature, if any. Otherwise, an empty ``list``.
        """
        if 'KeyServerPreferences' in self._signature.subpackets:
            return next(iter(self._signature.subpackets['h_KeyServerPreferences'])).flags
        return []

    @property
    def magic(self):
        return "SIGNATURE"

    @property
    def notation(self):
        """
        A ``dict`` of notation data in this signature, if any. Otherwise, an empty ``dict``.
        """
        return dict((nd.name, nd.value) for nd in self._signature.subpackets['NotationData'])

    @property
    def policy_uri(self):
        """
        The policy URI specified in this signature, if any. Otherwise, an empty ``str``.
        """
        if 'Policy' in self._signature.subpackets:
            return next(iter(self._signature.subpackets['Policy'])).uri
        return ''

    @property
    def revocable(self):
        """
        ``False`` if this signature is marked as being not revocable. Otherwise, ``True``.
        """
        if 'Revocable' in self._signature.subpackets:
            return bool(next(iter(self._signature.subpackets['Revocable'])))
        return True

    @property
    def revocation_key(self):
        if 'RevocationKey' in self._signature.subpackets:
            raise NotImplementedError()
        return None

    @property
    def revocation_reason(self):
        if 'ReasonForRevocation' in self._signature.subpackets:
            subpacket = next(iter(self._signature.subpackets['ReasonForRevocation']))
            return self._reason_for_revocation(subpacket.code, subpacket.string)
        return None

    @property
    def attested_certifications(self):
        """
        Returns a set of all the hashes of attested certifications covered by this Attestation Key Signature.

        Unhashed subpackets are ignored.
        """
        if self._signature.sigtype != SignatureType.Attestation:
            return set()
        ret = set()
        hlen = self.hash_algorithm.digest_size
        for n in self._signature.subpackets['h_AttestedCertifications']:
            attestations = bytes(n.attested_certifications)
            for i in range(0, len(attestations), hlen):
                ret.add(attestations[i:i + hlen])
        return ret

    @property
    def signer(self):
        """
        The 16-character Key ID of the key that generated this signature.
        """
        return self._signature.signer

    @property
    def signer_fingerprint(self):
        """
        The fingerprint of the key that generated this signature, if it contained. Otherwise, an empty ``str``.
        """
        if 'IssuerFingerprint' in self._signature.subpackets:
            return next(iter(self._signature.subpackets['IssuerFingerprint'])).issuer_fingerprint
        return ''

    @property
    def intended_recipients(self):
        """
        Returns an iterator over all the primary key fingerprints marked as intended encrypted recipients for this signature.
        """
        return map(lambda x: x.intended_recipient, self._signature.subpackets['IntendedRecipient'])

    @property
    def target_signature(self):
        return NotImplemented

    @property
    def type(self):
        """
        The :py:obj:`~constants.SignatureType` of this signature.
        """
        return self._signature.sigtype

    @classmethod
    def new(cls, sigtype, pkalg, halg, signer, created=None):
        sig = PGPSignature()

        if created is None:
            created = datetime.now(timezone.utc)
        sigpkt = SignatureV4()
        sigpkt.header.tag = 2
        sigpkt.header.version = 4
        sigpkt.subpackets.addnew('CreationTime', hashed=True, created=created)
        sigpkt.subpackets.addnew('Issuer', _issuer=signer)

        sigpkt.sigtype = sigtype
        sigpkt.pubalg = pkalg

        if halg is not None:
            sigpkt.halg = halg

        sig._signature = sigpkt
        return sig

    def __init__(self):
        """
        PGPSignature objects represent OpenPGP compliant signatures.

        PGPSignature implements the ``__str__`` method, the output of which will be the signature object in
        OpenPGP-compliant ASCII-armored format.

        PGPSignature implements the ``__bytes__`` method, the output of which will be the signature object in
        OpenPGP-compliant binary format.
        """
        super(PGPSignature, self).__init__()
        self._signature = None

    def __bytearray__(self):
        return self._signature.__bytearray__()

    def __repr__(self):
        return "<PGPSignature [{:s}] object at 0x{:02x}>".format(self.type.name, id(self))

    def __lt__(self, other):
        return self.created < other.created

    def __or__(self, other):
        if isinstance(other, Signature):
            if self._signature is None:
                self._signature = other
                return self

        ##TODO: this is not a great way to do this
        if other.__class__.__name__ == 'EmbeddedSignature':
            self._signature = other
            return self

        raise TypeError

    def __copy__(self):
        # because the default shallow copy isn't actually all that useful,
        # and deepcopy does too much work
        sig = super(PGPSignature, self).__copy__()
        # sig = PGPSignature()
        # sig.ascii_headers = self.ascii_headers.copy()
        sig |= copy.copy(self._signature)
        return sig

    def attests_to(self, othersig):
        'returns True if this signature attests to othersig (acknolwedges it for redistribution)'
        if not isinstance(othersig, PGPSignature):
            raise TypeError
        h = self.hash_algorithm.hasher
        h.update(othersig._signature.canonical_bytes())
        return h.digest() in self.attested_certifications

    def hashdata(self, subject):
        _data = bytearray()

        if isinstance(subject, str):
            try:
                subject = subject.encode('utf-8')
            except UnicodeEncodeError:
                subject = subject.encode('charmap')

        """
        All signatures are formed by producing a hash over the signature
        data, and then using the resulting hash in the signature algorithm.
        """

        if self.type == SignatureType.BinaryDocument:
            """
            For binary document signatures (type 0x00), the document data is
            hashed directly.
            """
            if isinstance(subject, (SKEData, IntegrityProtectedSKEData)):
                _data += subject.__bytearray__()
            else:
                _data += bytearray(subject)

        if self.type == SignatureType.CanonicalDocument:
            """
            For text document signatures (type 0x01), the
            document is canonicalized by converting line endings to <CR><LF>,
            and the resulting data is hashed.
            """
            _data += re.subn(br'\r?\n', b'\r\n', subject)[0]

        if self.type in {SignatureType.Generic_Cert, SignatureType.Persona_Cert, SignatureType.Casual_Cert,
                         SignatureType.Positive_Cert, SignatureType.CertRevocation, SignatureType.Subkey_Binding,
                         SignatureType.PrimaryKey_Binding}:
            """
            When a signature is made over a key, the hash data starts with the
            octet 0x99, followed by a two-octet length of the key, and then body
            of the key packet.  (Note that this is an old-style packet header for
            a key packet with two-octet length.) ...
            Key revocation signatures (types 0x20 and 0x28)
            hash only the key being revoked.
            """
            _s = b''
            if isinstance(subject, PGPUID):
                _s = subject._parent.hashdata

            elif isinstance(subject, PGPKey) and not subject.is_primary:
                _s = subject._parent.hashdata

            elif isinstance(subject, PGPKey) and subject.is_primary:
                _s = subject.hashdata

            if len(_s) > 0:
                _data += b'\x99' + self.int_to_bytes(len(_s), 2) + _s

        if self.type in {SignatureType.Subkey_Binding, SignatureType.PrimaryKey_Binding}:
            """
            A subkey binding signature
            (type 0x18) or primary key binding signature (type 0x19) then hashes
            the subkey using the same format as the main key (also using 0x99 as
            the first octet).
            """
            if subject.is_primary:
                _s = subject.subkeys[self.signer].hashdata

            else:
                _s = subject.hashdata

            _data += b'\x99' + self.int_to_bytes(len(_s), 2) + _s

        if self.type in {SignatureType.KeyRevocation, SignatureType.SubkeyRevocation, SignatureType.DirectlyOnKey}:
            """
            The signature is calculated directly on the key being revoked.  A
            revoked key is not to be used.  Only revocation signatures by the
            key being revoked, or by an authorized revocation key, should be
            considered valid revocation signatures.

            Subkey revocation signature
            The signature is calculated directly on the subkey being revoked.
            A revoked subkey is not to be used.  Only revocation signatures
            by the top-level signature key that is bound to this subkey, or
            by an authorized revocation key, should be considered valid
            revocation signatures.

            - clarification from draft-ietf-openpgp-rfc4880bis-02:
            Primary key revocation signatures (type 0x20) hash
            only the key being revoked.  Subkey revocation signature (type 0x28)
            hash first the primary key and then the subkey being revoked

            Signature directly on a key
            This signature is calculated directly on a key.  It binds the
            information in the Signature subpackets to the key, and is
            appropriate to be used for subpackets that provide information
            about the key, such as the Revocation Key subpacket.  It is also
            appropriate for statements that non-self certifiers want to make
            about the key itself, rather than the binding between a key and a
            name.
            """
            if self.type == SignatureType.SubkeyRevocation:
                # hash the primary key first if this is a Subkey Revocation signature
                _s = subject.parent.hashdata
                _data += b'\x99' + self.int_to_bytes(len(_s), 2) + _s

            _s = subject.hashdata
            _data += b'\x99' + self.int_to_bytes(len(_s), 2) + _s

        if self.type in {SignatureType.Generic_Cert, SignatureType.Persona_Cert, SignatureType.Casual_Cert,
                         SignatureType.Positive_Cert, SignatureType.CertRevocation}:
            """
            A certification signature (type 0x10 through 0x13) hashes the User
            ID being bound to the key into the hash context after the above
            data.  ...  A V4 certification
            hashes the constant 0xB4 for User ID certifications or the constant
            0xD1 for User Attribute certifications, followed by a four-octet
            number giving the length of the User ID or User Attribute data, and
            then the User ID or User Attribute data.

            ...

            The [certificate revocation] signature
            is computed over the same data as the certificate that it
            revokes, and should have a later creation date than that
            certificate.
            """

            _s = subject.hashdata
            if subject.is_uid:
                _data += b'\xb4'

            else:
                _data += b'\xd1'

            _data += self.int_to_bytes(len(_s), 4) + _s

        # if this is a new signature, do update_hlen
        if 0 in list(self._signature.signature):
            self._signature.update_hlen()

        """
        Once the data body is hashed, then a trailer is hashed. (...)
        A V4 signature hashes the packet body
        starting from its first field, the version number, through the end
        of the hashed subpacket data.  Thus, the fields hashed are the
        signature version, the signature type, the public-key algorithm, the
        hash algorithm, the hashed subpacket length, and the hashed
        subpacket body.

        V4 signatures also hash in a final trailer of six octets: the
        version of the Signature packet, i.e., 0x04; 0xFF; and a four-octet,
        big-endian number that is the length of the hashed data from the
        Signature packet (note that this number does not include these final
        six octets).
        """

        hcontext = bytearray()
        hcontext.append(self._signature.header.version if not self.embedded else self._signature._sig.header.version)
        hcontext.append(self.type)
        hcontext.append(self.key_algorithm)
        hcontext.append(self.hash_algorithm)
        hcontext += self._signature.subpackets.__hashbytearray__()
        hlen = len(hcontext)
        _data += hcontext
        _data += b'\x04\xff'
        _data += self.int_to_bytes(hlen, 4)
        return bytes(_data)

    def make_onepass(self):
        onepass = OnePassSignatureV3()
        onepass.sigtype = self.type
        onepass.halg = self.hash_algorithm
        onepass.pubalg = self.key_algorithm
        onepass.signer = self.signer
        onepass.update_hlen()
        return onepass

    def parse(self, packet):
        unarmored = self.ascii_unarmor(packet)
        data = unarmored['body']

        if unarmored['magic'] is not None and unarmored['magic'] != 'SIGNATURE':
            raise ValueError('Expected: SIGNATURE. Got: {}'.format(str(unarmored['magic'])))

        if unarmored['headers'] is not None:
            self.ascii_headers = unarmored['headers']

        # load *one* packet from data
        pkt = Packet(data)
        if pkt.header.tag == PacketTag.Signature:
            if isinstance(pkt, Opaque):
                # this is an unrecognized version.
                pass
            else:
                self._signature = pkt
        else:
            raise ValueError('Expected: Signature. Got: {:s}'.format(pkt.__class__.__name__))


class PGPUID(ParentRef):
    @property
    def __sig__(self):
        return list(self._signatures)

    def _splitstring(self):
        '''returns name, comment email from User ID string'''
        if not isinstance(self._uid, UserID):
            return "", "", ""
        if self._uid.uid == "":
            return "", "", ""
        rfc2822 = re.match(r"""^
                           # name should always match something
                           (?P<name>.+?)
                           # comment *optionally* matches text in parens following name
                           # this should never come after email and must be followed immediately by
                           # either the email field, or the end of the packet.
                           (\ \((?P<comment>.+?)\)(?=(\ <|$)))?
                           # email *optionally* matches text in angle brackets following name or comment
                           # this should never come before a comment, if comment exists,
                           # but can immediately follow name if comment does not exist
                           (\ <(?P<email>.+)>)?
                           $
                           """, self._uid.uid, flags=re.VERBOSE).groupdict()

        return (rfc2822['name'], rfc2822['comment'] or "", rfc2822['email'] or "")

    @property
    def name(self):
        """If this is a User ID, the stored name. If this is not a User ID, this will be an empty string."""
        return self._splitstring()[0]

    @property
    def comment(self):
        """
        If this is a User ID, this will be the stored comment. If this is not a User ID, or there is no stored comment,
        this will be an empty string.,
        """
        return self._splitstring()[1]

    @property
    def email(self):
        """
        If this is a User ID, this will be the stored email address. If this is not a User ID, or there is no stored
        email address, this will be an empty string.
        """
        return self._splitstring()[2]

    @property
    def userid(self):
        """
        If this is a User ID, this will be the full UTF-8 string. If this is not a User ID, this will be ``None``.
        """
        return self._uid.uid if isinstance(self._uid, UserID) else None

    @property
    def image(self):
        """
        If this is a User Attribute, this will be the stored image. If this is not a User Attribute, this will be ``None``.
        """
        return self._uid.image.image if isinstance(self._uid, UserAttribute) else None

    @property
    def is_primary(self):
        """
        If the most recent, valid self-signature specifies this as being primary, this will be True. Otherwise, False.
        """
        if self.selfsig is not None:
            return bool(next(iter(self.selfsig._signature.subpackets['h_PrimaryUserID']), False))
        return False

    @property
    def is_uid(self):
        """
        ``True`` if this is a User ID, otherwise False.
        """
        return isinstance(self._uid, UserID)

    @property
    def is_ua(self):
        """
        ``True`` if this is a User Attribute, otherwise False.
        """
        return isinstance(self._uid, UserAttribute)

    @property
    def selfsig(self):
        """
        This will be the most recent, self-signature of this User ID or Attribute. If there isn't one, this will be ``None``.
        """
        if self.parent is not None:
            for sig in reversed(self._signatures):
                if sig.signer_fingerprint:
                    if self.parent.fingerprint == sig.signer_fingerprint:
                        return sig
                elif sig.signer:
                    if self.parent.fingerprint == sig.signer:
                        return sig

    @property
    def signers(self):
        """
        This will be a set of all of the key ids which have signed this User ID or Attribute.
        """
        return set(s.signer for s in self.__sig__)

    @property
    def hashdata(self):
        if self.is_uid:
            return self._uid.__bytearray__()[len(self._uid.header):]

        if self.is_ua:
            return self._uid.subpackets.__bytearray__()

    @property
    def third_party_certifications(self):
        '''
        A generator returning all third-party certifications
        '''
        if self.parent is None:
            return
        fpr = self.parent.fingerprint
        keyid = self.parent.fingerprint.keyid
        for sig in self._signatures:
            if (sig.signer_fingerprint != '' and fpr != sig.signer_fingerprint) or (sig.signer != keyid):
                yield sig

    def attested_to(self, certifications):
        '''filter certifications, only returning those that have been attested to by the first party'''
        # first find the set of the most recent valid Attestation Key Signatures:
        if self.parent is None:
            return
        mostrecent = None
        attestations = []
        now = datetime.now(timezone.utc)
        fpr = self.parent.fingerprint
        keyid = self.parent.fingerprint.keyid
        for sig in self._signatures:
            if sig._signature.sigtype == SignatureType.Attestation and \
               ((sig.signer_fingerprint == fpr) or (sig.signer == keyid)) and \
               self.parent.verify(self, sig) and \
               sig.created <= now:
                if mostrecent is None or sig.created > mostrecent:
                    attestations = [sig]
                    mostrecent = sig.created
                elif sig.created == mostrecent:
                    attestations.append(sig)
        # now filter the certifications:
        for certification in certifications:
            for a in attestations:
                if a.attests_to(certification):
                    yield certification

    @property
    def attested_third_party_certifications(self):
        '''
        A generator that provides a list of all third-party certifications attested to
        by the primary key.
        '''
        return self.attested_to(self.third_party_certifications)

    @classmethod
    def new(cls, pn, comment="", email=""):
        """
        Create a new User ID or photo.

        :param pn: User ID name, or photo. If this is a ``bytearray``, it will be loaded as a photo.
                   Otherwise, it will be used as the name field for a User ID.
        :type pn: ``bytearray``, ``str``, ``unicode``
        :param comment: The comment field for a User ID. Ignored if this is a photo.
        :type comment: ``str``, ``unicode``
        :param email: The email address field for a User ID. Ignored if this is a photo.
        :type email: ``str``, ``unicode``
        :returns: :py:obj:`PGPUID`
        """
        uid = PGPUID()
        if isinstance(pn, bytearray):
            uid._uid = UserAttribute()
            uid._uid.image.image = pn
            uid._uid.image.iencoding = ImageEncoding.encodingof(pn)
            uid._uid.update_hlen()

        else:
            uid._uid = UserID()
            uidstr = pn
            if comment:
                uidstr += ' (' + comment + ')'
            if email:
                uidstr += ' <' + email + '>'
            uid._uid.uid = uidstr
            uid._uid.update_hlen()

        return uid

    def __init__(self):
        """
        PGPUID objects represent User IDs and User Attributes for keys.

        PGPUID implements the ``__format__`` method for User IDs, returning a string in the format
        'name (comment) <email>', leaving out any comment or email fields that are not present.
        """
        super(PGPUID, self).__init__()
        self._uid = None
        self._signatures = SorteDeque()

    def __repr__(self):
        if self.selfsig is not None:
            return "<PGPUID [{:s}][{}] at 0x{:02X}>".format(self._uid.__class__.__name__, self.selfsig.created, id(self))
        return "<PGPUID [{:s}] at 0x{:02X}>".format(self._uid.__class__.__name__, id(self))

    def __lt__(self, other):  # pragma: no cover
        if self.is_uid == other.is_uid:
            if self.is_primary == other.is_primary:
                mysig = self.selfsig
                othersig = other.selfsig
                if mysig is None:
                    return not (othersig is None)
                if othersig is None:
                    return False
                return mysig > othersig

            if self.is_primary:
                return True

            return False

        if self.is_uid and other.is_ua:
            return True

        if self.is_ua and other.is_uid:
            return False

    def __or__(self, other):
        if isinstance(other, PGPSignature):
            self._signatures.insort(other)
            if self.parent is not None and self in self.parent._uids:
                self.parent._uids.resort(self)

            return self

        if isinstance(other, UserID) and self._uid is None:
            self._uid = other
            return self

        if isinstance(other, UserAttribute) and self._uid is None:
            self._uid = other
            return self

        raise TypeError("unsupported operand type(s) for |: '{:s}' and '{:s}'"
                        "".format(self.__class__.__name__, other.__class__.__name__))

    def __copy__(self):
        # because the default shallow copy isn't actually all that useful,
        # and deepcopy does too much work
        uid = PGPUID()
        uid |= copy.copy(self._uid)
        for sig in self._signatures:
            uid |= copy.copy(sig)
        return uid

    def __format__(self, format_spec):
        if self.is_uid:
            comment = "" if self.comment == "" else " ({:s})".format(self.comment)
            email = "" if self.email == "" else " <{:s}>".format(self.email)
            return "{:s}{:s}{:s}".format(self.name, comment, email)

        raise NotImplementedError


class PGPMessage(Armorable, PGPObject):
    @staticmethod
    def dash_unescape(text):
        return re.subn(r'^- ', '', text, flags=re.MULTILINE)[0]

    @staticmethod
    def dash_escape(text):
        return re.subn(r'^-', '- -', text, flags=re.MULTILINE)[0]

    @property
    def encrypters(self):
        """A ``set`` containing all key ids (if any) to which this message was encrypted."""
        return set(m.encrypter for m in self._sessionkeys if isinstance(m, PKESessionKey))

    @property
    def filename(self):
        """If applicable, returns the original filename of the message. Otherwise, returns an empty string."""
        if self.type == 'literal':
            return self._message.filename
        return ''

    @property
    def is_compressed(self):
        """``True`` if this message will be compressed when exported"""
        return self._compression != CompressionAlgorithm.Uncompressed

    @property
    def is_encrypted(self):
        """``True`` if this message is encrypted; otherwise, ``False``"""
        return isinstance(self._message, (SKEData, IntegrityProtectedSKEData))

    @property
    def is_sensitive(self):
        """``True`` if this message is marked sensitive; otherwise ``False``"""
        return self.type == 'literal' and self._message.filename == '_CONSOLE'

    @property
    def is_signed(self):
        """
        ``True`` if this message is signed; otherwise, ``False``.
        Should always be ``False`` if the message is encrypted.
        """
        return len(self._signatures) > 0

    @property
    def issuers(self):
        """A ``set`` containing all key ids (if any) which have signed or encrypted this message."""
        return self.encrypters | self.signers

    @property
    def magic(self):
        if self.type == 'cleartext':
            return "SIGNATURE"
        return "MESSAGE"

    @property
    def message(self):
        """The message contents"""
        if self.type == 'cleartext':
            return self.bytes_to_text(self._message)

        if self.type == 'literal':
            return self._message.contents

        if self.type == 'encrypted':
            return self._message

    @property
    def signatures(self):
        """A ``set`` containing all key ids (if any) which have signed this message."""
        return list(self._signatures)

    @property
    def signers(self):
        """A ``set`` containing all key ids (if any) which have signed this message."""
        return set(m.signer for m in self._signatures)

    @property
    def type(self):
        ##TODO: it might be better to use an Enum for the output of this
        if isinstance(self._message, (str, bytes, bytearray)):
            return 'cleartext'

        if isinstance(self._message, LiteralData):
            return 'literal'

        if isinstance(self._message, (SKEData, IntegrityProtectedSKEData)):
            return 'encrypted'

        raise NotImplementedError

    def __init__(self):
        """
        PGPMessage objects represent OpenPGP message compositions.

        PGPMessage implements the ``__str__`` method, the output of which will be the message composition in
        OpenPGP-compliant ASCII-armored format.

        PGPMessage implements the ``__bytes__`` method, the output of which will be the message composition in
        OpenPGP-compliant binary format.

        Any signatures within the PGPMessage that are marked as being non-exportable will not be included in the output
        of either of those methods.
        """
        super(PGPMessage, self).__init__()
        self._compression = CompressionAlgorithm.Uncompressed
        self._message = None
        self._mdc = None
        self._signatures = SorteDeque()
        self._sessionkeys = []

    def __bytearray__(self):
        if self.is_compressed:
            comp = CompressedData()
            comp.calg = self._compression
            comp.packets = [pkt for pkt in self]
            comp.update_hlen()
            return comp.__bytearray__()

        _bytes = bytearray()
        for pkt in self:
            _bytes += pkt.__bytearray__()
        return _bytes

    def __str__(self):
        if self.type == 'cleartext':
            tmpl = u"-----BEGIN PGP SIGNED MESSAGE-----\n" \
                   u"{hhdr:s}\n" \
                   u"{cleartext:s}\n" \
                   u"{signature:s}"

            # only add a Hash: header if we actually have at least one signature
            hashes = set(s.hash_algorithm.name for s in self.signatures)
            hhdr = 'Hash: {hashes:s}\n'.format(hashes=','.join(sorted(hashes))) if hashes else ''

            return tmpl.format(hhdr=hhdr,
                               cleartext=self.dash_escape(self.bytes_to_text(self._message)),
                               signature=super(PGPMessage, self).__str__())

        return super(PGPMessage, self).__str__()

    def __iter__(self):
        if self.type == 'cleartext':
            for sig in self._signatures:
                yield sig

        elif self.is_encrypted:
            for sig in self._signatures:
                yield sig
            for pkt in self._sessionkeys:
                yield pkt
            yield self.message

        else:
            ##TODO: is it worth coming up with a way of disabling one-pass signing?
            for sig in reversed(self._signatures):
                ops = sig.make_onepass()
                if sig is not self._signatures[-1]:
                    ops.nested = True
                yield ops

            yield self._message
            if self._mdc is not None:  # pragma: no cover
                yield self._mdc

            for sig in self._signatures:
                yield sig

    def __or__(self, other):
        if isinstance(other, Marker):
            return self

        if isinstance(other, CompressedData):
            self._compression = other.calg
            for pkt in other.packets:
                self |= pkt
            return self

        if isinstance(other, (str, bytes, bytearray)):
            if self._message is None:
                self._message = self.text_to_bytes(other)
                return self

        if isinstance(other, (LiteralData, SKEData, IntegrityProtectedSKEData)):
            if self._message is None:
                self._message = other
                return self

        if isinstance(other, MDC):
            if self._mdc is None:
                self._mdc = other
                return self

        if isinstance(other, OnePassSignature):
            # these are "generated" on the fly during composition
            return self

        if isinstance(other, Signature):
            other = PGPSignature() | other

        if isinstance(other, PGPSignature):
            self._signatures.insort(other)
            return self

        if isinstance(other, (PKESessionKey, SKESessionKey)):
            self._sessionkeys.append(other)
            return self

        if isinstance(other, PGPMessage):
            self._message = other._message
            self._mdc = other._mdc
            self._compression = other._compression
            self._sessionkeys += other._sessionkeys
            self._signatures += other._signatures
            return self

        raise NotImplementedError(str(type(other)))

    def __copy__(self):
        msg = super(PGPMessage, self).__copy__()
        msg._compression = self._compression
        msg._message = copy.copy(self._message)
        msg._mdc = copy.copy(self._mdc)

        for sig in self._signatures:
            msg |= copy.copy(sig)

        for sk in self._sessionkeys:
            msg |= copy.copy(sk)

        return msg

    @classmethod
    def new(cls, message, **kwargs):
        """
        Create a new PGPMessage object.

        :param message: The message to be stored.
        :type message: ``str``, ``unicode``, ``bytes``, ``bytearray``
        :returns: :py:obj:`PGPMessage`

        The following optional keyword arguments can be used with :py:meth:`PGPMessage.new`:

        :keyword file: if True, ``message`` should be a path to a file. The contents of that file will be read and used
                       as the contents of the message.
        :type file: ``bool``
        :keyword cleartext: if True, the message will be cleartext with inline signatures.
        :type cleartext: ``bool``
        :keyword sensitive: if True, the filename will be set to '_CONSOLE' to signal other OpenPGP clients to treat
                            this message as being 'for your eyes only'. Ignored if cleartext is True.
        :type sensitive: ``bool``
        :keyword format: Set the message format identifier. Ignored if cleartext is True.
        :type format: ``str``
        :keyword compression: Set the compression algorithm for the new message.
                              Defaults to :py:obj:`CompressionAlgorithm.ZIP`. Ignored if cleartext is True.
        :keyword encoding: Set the Charset header for the message.
        :type encoding: ``str`` representing a valid codec in codecs
        """
        # TODO: have 'codecs' above (in :type encoding:) link to python documentation page on codecs
        cleartext = kwargs.pop('cleartext', False)
        format = kwargs.pop('format', None)
        sensitive = kwargs.pop('sensitive', False)
        compression = kwargs.pop('compression', CompressionAlgorithm.ZIP)
        file = kwargs.pop('file', False)
        charset = kwargs.pop('encoding', None)

        filename = ''
        mtime = datetime.now(timezone.utc)

        msg = PGPMessage()

        if charset:
            msg.charset = charset

        # if format in 'tu' and isinstance(message, (bytes, bytearray)):
        #     # if message format is text or unicode and we got binary data, we'll need to transcode it to UTF-8
        #     message =

        if file and os.path.isfile(message):
            filename = message
            message = bytearray(os.path.getsize(filename))
            mtime = datetime.fromtimestamp(os.path.getmtime(filename), timezone.utc)

            with open(filename, 'rb') as mf:
                mf.readinto(message)

        # if format is None, we can try to detect it
        if format is None:
            if isinstance(message, str):
                # message is definitely UTF-8 already
                format = 'u'

            elif cls.is_ascii(message):
                # message is probably text
                format = 't'

            else:
                # message is probably binary
                format = 'b'

        # if message is a binary type and we're building a textual message, we need to transcode the bytes to UTF-8
        if isinstance(message, (bytes, bytearray)) and (cleartext or format in 'tu'):
            message = message.decode(charset or 'utf-8')

        if cleartext:
            msg |= message

        else:
            # load literal data
            lit = LiteralData()
            lit._contents = bytearray(msg.text_to_bytes(message))
            lit.filename = '_CONSOLE' if sensitive else os.path.basename(filename)
            lit.mtime = mtime
            lit.format = format

            # if cls.is_ascii(message):
            #     lit.format = 't'

            lit.update_hlen()

            msg |= lit
            msg._compression = compression

        return msg

    def encrypt(self, passphrase, sessionkey=None, **prefs):
        """
        encrypt(passphrase, [sessionkey=None,] **prefs)

        Encrypt the contents of this message using a passphrase.

        :param passphrase: The passphrase to use for encrypting this message.
        :type passphrase: ``str``, ``unicode``, ``bytes``

        :param sessionkey: Provide a session key to use when encrypting something. Default is ``None``.
                                    If ``None``, a session key of the appropriate length will be generated randomly.

                                    .. warning::

                                        Care should be taken when making use of this option! Session keys *absolutely need*
                                        to be unpredictable! Use the ``gen_key()`` method on the desired
                                        :py:obj:`~constants.SymmetricKeyAlgorithm` to generate the session key!

        :type sessionkey: ``bytes``, ``str``
        :raises: :py:exc:`~errors.PGPEncryptionError`
        :returns: A new :py:obj:`PGPMessage` containing the encrypted contents of this message.
        """
        cipher_algo = prefs.pop('cipher', SymmetricKeyAlgorithm.AES256)
        hash_algo = prefs.pop('hash', HashAlgorithm.SHA256)

        # set up a new SKESessionKeyV4
        skesk = SKESessionKeyV4()
        skesk.s2k.usage = 255
        skesk.s2k.specifier = 3
        skesk.s2k.halg = hash_algo
        skesk.s2k.encalg = cipher_algo
        skesk.s2k.count = skesk.s2k.halg.tuned_count

        if sessionkey is None:
            sessionkey = cipher_algo.gen_key()
        skesk.encrypt_sk(passphrase, sessionkey)
        del passphrase

        msg = PGPMessage() | skesk

        if not self.is_encrypted:
            skedata = IntegrityProtectedSKEDataV1()
            skedata.encrypt(sessionkey, cipher_algo, self.__bytes__())
            msg |= skedata

        else:
            msg |= self

        return msg

    def decrypt(self, passphrase):
        """
        Attempt to decrypt this message using a passphrase.

        :param passphrase: The passphrase to use to attempt to decrypt this message.
        :type passphrase: ``str``, ``unicode``, ``bytes``
        :raises: :py:exc:`~errors.PGPDecryptionError` if decryption failed for any reason.
        :returns: A new :py:obj:`PGPMessage` containing the decrypted contents of this message
        """
        if not self.is_encrypted:
            raise PGPError("This message is not encrypted!")

        for skesk in iter(sk for sk in self._sessionkeys if isinstance(sk, SKESessionKey)):
            try:
                symalg, key = skesk.decrypt_sk(passphrase)
                decmsg = PGPMessage()
                decmsg.parse(self.message.decrypt(key, symalg))

            except (TypeError, ValueError, NotImplementedError, PGPDecryptionError):
                continue

            else:
                del passphrase
                break

        else:
            raise PGPDecryptionError("Decryption failed")

        return decmsg

    def parse(self, packet):
        unarmored = self.ascii_unarmor(packet)
        data = unarmored['body']

        if unarmored['magic'] is not None and unarmored['magic'] not in ['MESSAGE', 'SIGNATURE']:
            raise ValueError('Expected: MESSAGE. Got: {}'.format(str(unarmored['magic'])))

        if unarmored['headers'] is not None:
            self.ascii_headers = unarmored['headers']

        # cleartext signature
        if unarmored['magic'] == 'SIGNATURE':
            # the composition for this will be the 'cleartext' as a str,
            # followed by one or more signatures (each one loaded into a PGPSignature)
            self |= self.dash_unescape(unarmored['cleartext'])
            while len(data) > 0:
                pkt = Packet(data)
                if not isinstance(pkt, Signature):  # pragma: no cover
                    warnings.warn("Discarded unexpected packet: {:s}".format(pkt.__class__.__name__), stacklevel=2)
                    continue
                self |= PGPSignature() | pkt

        else:
            while len(data) > 0:
                self |= Packet(data)


class PGPKey(Armorable, ParentRef, PGPObject):
    """
    11.1.  Transferable Public Keys

    OpenPGP users may transfer public keys.  The essential elements of a
    transferable public key are as follows:

     - One Public-Key packet

     - Zero or more revocation signatures
     - One or more User ID packets

     - After each User ID packet, zero or more Signature packets
       (certifications)

     - Zero or more User Attribute packets

     - After each User Attribute packet, zero or more Signature packets
       (certifications)

     - Zero or more Subkey packets

     - After each Subkey packet, one Signature packet, plus optionally a
       revocation

    The Public-Key packet occurs first.  Each of the following User ID
    packets provides the identity of the owner of this public key.  If
    there are multiple User ID packets, this corresponds to multiple
    means of identifying the same unique individual user; for example, a
    user may have more than one email address, and construct a User ID
    for each one.

    Immediately following each User ID packet, there are zero or more
    Signature packets.  Each Signature packet is calculated on the
    immediately preceding User ID packet and the initial Public-Key
    packet.  The signature serves to certify the corresponding public key
    and User ID.  In effect, the signer is testifying to his or her
    belief that this public key belongs to the user identified by this
    User ID.

    Within the same section as the User ID packets, there are zero or
    more User Attribute packets.  Like the User ID packets, a User
    Attribute packet is followed by zero or more Signature packets
    calculated on the immediately preceding User Attribute packet and the
    initial Public-Key packet.

    User Attribute packets and User ID packets may be freely intermixed
    in this section, so long as the signatures that follow them are
    maintained on the proper User Attribute or User ID packet.

    After the User ID packet or Attribute packet, there may be zero or
    more Subkey packets.  In general, subkeys are provided in cases where
    the top-level public key is a signature-only key.  However, any V4
    key may have subkeys, and the subkeys may be encryption-only keys,
    signature-only keys, or general-purpose keys.  V3 keys MUST NOT have
    subkeys.

    Each Subkey packet MUST be followed by one Signature packet, which
    should be a subkey binding signature issued by the top-level key.
    For subkeys that can issue signatures, the subkey binding signature
    MUST contain an Embedded Signature subpacket with a primary key
    binding signature (0x19) issued by the subkey on the top-level key.

    Subkey and Key packets may each be followed by a revocation Signature
    packet to indicate that the key is revoked.  Revocation signatures
    are only accepted if they are issued by the key itself, or by a key
    that is authorized to issue revocations via a Revocation Key
    subpacket in a self-signature by the top-level key.

    Transferable public-key packet sequences may be concatenated to allow
    transferring multiple public keys in one operation.

    11.2.  Transferable Secret Keys

    OpenPGP users may transfer secret keys.  The format of a transferable
    secret key is the same as a transferable public key except that
    secret-key and secret-subkey packets are used instead of the public
    key and public-subkey packets.  Implementations SHOULD include self-
    signatures on any user IDs and subkeys, as this allows for a complete
    public key to be automatically extracted from the transferable secret
    key.  Implementations MAY choose to omit the self-signatures,
    especially if a transferable public key accompanies the transferable
    secret key.
    """
    @property
    def __key__(self):
        return self._key.keymaterial

    @property
    def __sig__(self):
        return list(self._signatures)

    @property
    def created(self):
        """A :py:obj:`~datetime.datetime` object of the creation date and time of the key, in UTC."""
        return self._key.created

    @property
    def expires_at(self):
        """A :py:obj:`~datetime.datetime` object of when this key is to be considered expired, if any. Otherwise, ``None``"""
        expires = None
        for sig in iter(uid.selfsig for uid in self.userids if uid.selfsig):
            if sig.key_expiration is not None:
                expires = sig.key_expiration

        if expires is not None:
            return self.created + expires

        return None

    @property
    def fingerprint(self):
        """The fingerprint of this key, as a :py:obj:`~pgpy.types.Fingerprint` object."""
        if self._key:
            return self._key.fingerprint

    @property
    def hashdata(self):
        # when signing a key, only the public portion of the keys is hashed
        # if this is a private key, the private components of the key material need to be left out
        pub = self._key if self.is_public else self._key.pubkey()
        return pub.__bytearray__()[len(pub.header):]

    @property
    def is_expired(self):
        """``True`` if this key is expired, otherwise ``False``"""
        expires = self.expires_at
        if expires is not None:
            return expires <= datetime.now(timezone.utc)

        return False

    @property
    def is_primary(self):
        """``True`` if this is a primary key; ``False`` if this is a subkey"""
        return isinstance(self._key, Primary) and not isinstance(self._key, Sub)

    @property
    def is_protected(self):
        """``True`` if this is a private key that is protected with a passphrase, otherwise ``False``"""
        if self.is_public:
            return False

        return self._key.protected

    @property
    def is_public(self):
        """``True`` if this is a public key, otherwise ``False``"""
        return isinstance(self._key, Public) and not isinstance(self._key, Private)

    @property
    def is_unlocked(self):
        """``False`` if this is a private key that is protected with a passphrase and has not yet been unlocked, otherwise ``True``"""
        if self.is_public:
            return True

        if not self.is_protected:
            return True

        return self._key.unlocked

    @property
    def key_algorithm(self):
        """The :py:obj:`constants.PubKeyAlgorithm` pertaining to this key"""
        return self._key.pkalg

    @property
    def key_size(self):
        """
        The size pertaining to this key. ``int`` for non-EC key algorithms; :py:obj:`constants.EllipticCurveOID` for EC keys.

        .. versionadded:: 0.4.1
        """
        if self.key_algorithm in {PubKeyAlgorithm.ECDSA, PubKeyAlgorithm.ECDH, PubKeyAlgorithm.EdDSA}:
            return self._key.keymaterial.oid
        # check if keymaterial is not an Opaque class containing a bytearray
        param = next(iter(self._key.keymaterial))
        if isinstance(param, bytearray):
            return 0
        return param.bit_length()

    @property
    def magic(self):
        return '{:s} KEY BLOCK'.format('PUBLIC' if (isinstance(self._key, Public) and not isinstance(self._key, Private)) else
                                       'PRIVATE' if isinstance(self._key, Private) else '')

    @property
    def pubkey(self):
        """If the :py:obj:`PGPKey` object is a private key, this method returns a corresponding public key object with
        all the trimmings. If it is already a public key, just return it.
        """
        if self.is_public:
            return self
        if self._sibling is None or isinstance(self._sibling, weakref.ref):
            # create a new key shell
            pub = PGPKey()
            pub.ascii_headers = self.ascii_headers.copy()

            # get the public half of the primary key
            pub._key = self._key.pubkey()

            # get the public half of each subkey
            for skid, subkey in self.subkeys.items():
                pub |= subkey.pubkey

            # copy user ids and user attributes
            for uid in self._uids:
                pub |= copy.copy(uid)

            # copy signatures that weren't copied with uids
            for sig in self._signatures:
                if sig.parent is None:
                    pub |= copy.copy(sig)

            # keep connect the two halves using a weak reference
            self._sibling = weakref.ref(pub)
            pub._sibling = weakref.ref(self)

            # copy parent
            if self.parent:
                pub._parent = weakref.ref(self.parent)

        return self._sibling()

    @pubkey.setter
    def pubkey(self, pubkey):
        if self.is_public:
            raise TypeError("cannot add public sibling to pubkey")

        if not pubkey.is_public:
            raise TypeError("sibling must be public")

        if self._sibling is not None and self._sibling() is not None:
            raise ValueError("public key reference already set")

        if pubkey.fingerprint != self.fingerprint:
            raise ValueError("key fingerprint mismatch")

        # TODO: sync packets with sibling
        self._sibling = weakref.ref(pubkey)
        pubkey._sibling = weakref.ref(self)

    @property
    def self_signatures(self):
        keyid, keytype = (self.fingerprint.keyid, SignatureType.DirectlyOnKey) if self.is_primary \
            else (self.parent.fingerprint.keyid, SignatureType.Subkey_Binding)

        ##TODO: filter out revoked signatures as well
        for sig in iter(sig for sig in self._signatures
                        if all([sig.type == keytype, sig.signer == keyid, not sig.is_expired])):
            yield sig

    @property
    def signers(self):
        """A ``set`` of key ids of keys that were used to sign this key"""
        return {sig.signer for sig in self.__sig__}

    @property
    def revocation_signatures(self):
        keyid, keytype = (self.fingerprint.keyid, SignatureType.KeyRevocation) if self.is_primary \
            else (self.parent.fingerprint.keyid, SignatureType.SubkeyRevocation)

        for sig in iter(sig for sig in self._signatures
                        if all([sig.type == keytype, sig.signer == keyid, not sig.is_expired])):
            yield sig

    @property
    def subkeys(self):
        """An :py:obj:`~collections.OrderedDict` of subkeys bound to this primary key, if applicable,
        selected by 16-character keyid."""
        return self._children

    @property
    def userids(self):
        """A ``list`` of :py:obj:`PGPUID` objects containing User ID information about this key"""
        return [ u for u in self._uids if u.is_uid ]

    @property
    def userattributes(self):
        """A ``list`` of :py:obj:`PGPUID` objects containing one or more images associated with this key"""
        return [u for u in self._uids if u.is_ua]

    @property
    def revocation_keys(self):
        """A ``generator`` with the list of keys that can revoke this key.

        See also :py:func:`PGPSignature.revocation_key`"""
        for sig in self._signatures:
            if sig.revocation_key:
                yield sig.revocation_key

    @classmethod
    def new(cls, key_algorithm, key_size, created=None):
        """
        Generate a new PGP key

        :param key_algorithm: Key algorithm to use.
        :type key_algorithm: :py:obj:`~constants.PubKeyAlgorithm`
        :param key_size: Key size in bits, unless `key_algorithm` is :py:obj:`~constants.PubKeyAlgorithm.ECDSA` or
               :py:obj:`~constants.PubKeyAlgorithm.ECDH`, in which case it should be the Curve OID to use.
        :type key_size: ``int`` or :py:obj:`~constants.EllipticCurveOID`

        :param created: When was the key created? (``None`` or unset means now)
        :type created: :py:obj:`~datetime.datetime` or ``None``
        :return: A newly generated :py:obj:`PGPKey`
        """
        # new private key shell first
        key = PGPKey()

        if key_algorithm in {PubKeyAlgorithm.RSAEncrypt, PubKeyAlgorithm.RSASign}:  # pragma: no cover
            warnings.warn('{:s} is deprecated - generating key using RSAEncryptOrSign'.format(key_algorithm.name))
            key_algorithm = PubKeyAlgorithm.RSAEncryptOrSign

        # generate some key data to match key_algorithm and key_size
        key._key = PrivKeyV4.new(key_algorithm, key_size, created=created)

        return key

    def __init__(self):
        """
        PGPKey objects represent OpenPGP compliant keys along with all of their associated data.

        PGPKey implements the `__str__` method, the output of which will be the key composition in
        OpenPGP-compliant ASCII-armored format.

        PGPKey implements the `__bytes__` method, the output of which will be the key composition in
        OpenPGP-compliant binary format.

        Any signatures within the PGPKey that are marked as being non-exportable will not be included in the output
        of either of those methods.
        """
        super(PGPKey, self).__init__()
        self._key = None
        self._children = collections.OrderedDict()
        self._signatures = SorteDeque()
        self._uids = SorteDeque()
        self._sibling = None
        self._self_verified = None
        self._require_usage_flags = True

    def __bytearray__(self):
        _bytes = bytearray()
        # us
        _bytes += self._key.__bytearray__()
        # our signatures; ignore embedded signatures
        for sig in iter(s for s in self._signatures if not s.embedded and s.exportable):
            _bytes += sig.__bytearray__()
        # one or more User IDs, followed by their signatures
        for uid in self._uids:
            _bytes += uid._uid.__bytearray__()
            for s in [s for s in uid._signatures if s.exportable]:
                _bytes += s.__bytearray__()
        # subkeys
        for sk in self._children.values():
            _bytes += sk.__bytearray__()

        return _bytes

    def __repr__(self):
        if self._key is not None:
            return "<PGPKey [{:s}][0x{:s}] at 0x{:02X}>" \
                   "".format(self._key.__class__.__name__, self.fingerprint.keyid, id(self))

        return "<PGPKey [unknown] at 0x{:02X}>" \
               "".format(id(self))

    def __contains__(self, item):
        if isinstance(item, PGPKey):  # pragma: no cover
            return item.fingerprint.keyid in self.subkeys

        if isinstance(item, Fingerprint):  # pragma: no cover
            return item.keyid in self.subkeys

        if isinstance(item, PGPUID):
            return item in self._uids

        if isinstance(item, PGPSignature):
            return item in self._signatures

        raise TypeError

    def __or__(self, other, from_sib=False):
        if isinstance(other, Key) and self._key is None:
            self._key = other

        elif isinstance(other, PGPKey) and not other.is_primary and other.is_public == self.is_public:
            other._parent = self
            self._children[other.fingerprint.keyid] = other

        elif isinstance(other, PGPSignature):
            self._signatures.insort(other)

            # if this is a subkey binding signature that has embedded primary key binding signatures, add them to parent
            if other.type == SignatureType.Subkey_Binding:
                for es in iter(pkb for pkb in other._signature.subpackets['EmbeddedSignature']):
                    esig = PGPSignature() | es
                    esig._parent = other
                    self._signatures.insort(esig)

        elif isinstance(other, PGPUID):
            other._parent = weakref.ref(self)
            self._uids.insort(other)

        else:
            raise TypeError(
                "unsupported operand type(s) for |: '{:s}' and '{:s}'"
                "".format(self.__class__.__name__, other.__class__.__name__)
            )

        if isinstance(self._sibling, weakref.ref) and not from_sib:
            sib = self._sibling()
            if sib is None:
                self._sibling = None

            else:  # pragma: no cover
                sib.__or__(copy.copy(other), True)

        return self

    def __copy__(self):
        key = super(PGPKey, self).__copy__()
        key._key = copy.copy(self._key)

        for uid in self._uids:
            key |= copy.copy(uid)

        for id, subkey in self._children.items():
            key |= copy.copy(subkey)

        for sig in self._signatures:
            if sig.embedded:
                # embedded signatures don't need to be explicitly copied
                continue

            key |= copy.copy(sig)

        return key

    def protect(self, passphrase, enc_alg, hash_alg):
        """
        Add a passphrase to a private key. If the key is already passphrase protected, it should be unlocked before
        a new passphrase can be specified.

        Has no effect on public keys.

        :param passphrase: A passphrase to protect the key with
        :type passphrase: ``str``, ``unicode``
        :param enc_alg: Symmetric encryption algorithm to use to protect the key
        :type enc_alg: :py:obj:`~constants.SymmetricKeyAlgorithm`
        :param hash_alg: Hash algorithm to use in the String-to-Key specifier
        :type hash_alg: :py:obj:`~constants.HashAlgorithm`
        """
        ##TODO: specify strong defaults for enc_alg and hash_alg
        if self.is_public:
            # we can't protect public keys because only private key material is ever protected
            warnings.warn("Public keys cannot be passphrase-protected", stacklevel=2)
            return

        if self.is_protected and not self.is_unlocked:
            # we can't protect a key that is already protected unless it is unlocked first
            warnings.warn("This key is already protected with a passphrase - "
                          "please unlock it before attempting to specify a new passphrase", stacklevel=2)
            return

        for sk in itertools.chain([self], self.subkeys.values()):
            sk._key.protect(passphrase, enc_alg, hash_alg)

        del passphrase

    @contextlib.contextmanager
    def unlock(self, passphrase):
        """
        Context manager method for unlocking passphrase-protected private keys. Has no effect if the key is not both
        private and passphrase-protected.

        When the context managed block is exited, the unprotected private key material is removed.

        Example::

            privkey = PGPKey()
            privkey.parse(keytext)

            assert privkey.is_protected
            assert privkey.is_unlocked is False
            # privkey.sign("some text") <- this would raise an exception

            with privkey.unlock("TheCorrectPassphrase"):
                # privkey is now unlocked
                assert privkey.is_unlocked
                # so you can do things with it
                sig = privkey.sign("some text")

            # privkey is no longer unlocked
            assert privkey.is_unlocked is False

        Emits a :py:obj:`~warnings.UserWarning` if the key is public or not passphrase protected.

        :param passphrase: The passphrase to be used to unlock this key.
        :type passphrase: ``str``
        :raises: :py:exc:`~pgpy.errors.PGPDecryptionError` if the passphrase is incorrect
        """
        if self.is_public:
            # we can't unprotect public keys because only private key material is ever protected
            warnings.warn("Public keys cannot be passphrase-protected", stacklevel=3)
            yield self
            return

        if not self.is_protected:
            # we can't unprotect private keys that are not protected, because there is no ciphertext to decrypt
            warnings.warn("This key is not protected with a passphrase", stacklevel=3)
            yield self
            return

        try:
            for sk in itertools.chain([self], self.subkeys.values()):
                sk._key.unprotect(passphrase)
            del passphrase
            yield self

        finally:
            # clean up here by deleting the previously decrypted secret key material
            for sk in itertools.chain([self], self.subkeys.values()):
                sk._key.keymaterial.clear()

    def add_uid(self, uid, selfsign=True, **prefs):
        """
        Add a User ID to this key.

        :param uid: The user id to add
        :type uid: :py:obj:`~pgpy.PGPUID`
        :param selfsign: Whether or not to self-sign the user id before adding it
        :type selfsign: ``bool``

        Valid optional keyword arguments are identical to those of self-signatures for :py:meth:`PGPKey.certify`.
        Any such keyword arguments are ignored if selfsign is ``False``
        """
        uid._parent = self
        if selfsign:
            uid |= self.certify(uid, SignatureType.Positive_Cert, **prefs)

        self |= uid

    def get_uid(self, search):
        """
        Find and return a User ID that matches the search string given.

        :param search: A text string to match name, comment, or email address against
        :type search: ``str``, ``unicode``
        :return: The first matching :py:obj:`~pgpy.PGPUID`, or ``None`` if no matches were found.
        """
        if self.is_primary:
            return next((u for u in self._uids if search in filter(lambda a: a is not None, (u.name, u.comment, u.email))), None)
        return self.parent.get_uid(search)

    def del_uid(self, search):
        """
        Find and remove a user id that matches the search string given. This method does not modify the corresponding
        :py:obj:`~pgpy.PGPUID` object; it only removes it from the list of user ids on the key.

        :param search: A text string to match name, comment, or email address against
        :type search: ``str``, ``unicode``
        """
        u = self.get_uid(search)

        if u is None:
            raise KeyError("uid '{:s}' not found".format(search))

        u._parent = None
        self._uids.remove(u)

    def add_subkey(self, key, **prefs):
        """
        Add a key as a subkey to this key.

        :param key: A private :py:obj:`~pgpy.PGPKey` that does not have any subkeys of its own
        :keyword usage: A ``set`` of key usage flags, as :py:obj:`~constants.KeyFlags` for the subkey to be added.
        :type usage: ``set``

        Other valid optional keyword arguments are identical to those of self-signatures for :py:meth:`PGPKey.certify`
        """
        if self.is_public:
            raise PGPError("Cannot add a subkey to a public key. Add the subkey to the private component first!")

        if key.is_public:
            raise PGPError("Cannot add a public key as a subkey to this key")

        if key.is_primary:
            if len(key._children) > 0:
                raise PGPError("Cannot add a key that already has subkeys as a subkey!")

            # convert key into a subkey
            npk = PrivSubKeyV4()
            npk.pkalg = key._key.pkalg
            npk.created = key._key.created
            npk.keymaterial = key._key.keymaterial
            key._key = npk
            key._key.update_hlen()

        self._children[key.fingerprint.keyid] = key
        key._parent = self

        ##TODO: skip this step if the key already has a subkey binding signature
        bsig = self.bind(key, **prefs)
        key |= bsig

    def _get_key_flags(self, user=None):
        if self.is_primary:
            if user is not None:
                user = self.get_uid(user)

            elif len(self._uids) == 0:
                return {KeyFlags.Certify}

            else:
                user = next(iter(self.userids))

            # RFC 4880 says that primary keys *must* be capable of certification
            return {KeyFlags.Certify} | (user.selfsig.key_flags if user.selfsig else set())

        return next(self.self_signatures).key_flags

    def _sign(self, subject, sig, **prefs):
        """
        The actual signing magic happens here.
        :param subject: The subject to sign
        :param sig: The :py:obj:`PGPSignature` object the new signature is to be encapsulated within
        :returns: ``sig``, after the signature is added to it.
        """
        user = prefs.pop('user', None)
        uid = None
        if user is not None:
            uid = self.get_uid(user)

        else:
            uid = next(iter(self.userids), None)
            if uid is None and self.parent is not None:
                uid = next(iter(self.parent.userids), None)

        if sig.hash_algorithm is None:
            sig._signature.halg = next((h for h in uid.selfsig.hashprefs if h.is_supported), HashAlgorithm.SHA256)

        if uid is not None and sig.hash_algorithm not in uid.selfsig.hashprefs:
            warnings.warn("Selected hash algorithm not in key preferences", stacklevel=4)

        # signature options that can be applied at any level
        expires = prefs.pop('expires', None)
        notation = prefs.pop('notation', None)
        revocable = prefs.pop('revocable', True)
        policy_uri = prefs.pop('policy_uri', None)
        intended_recipients = prefs.pop('intended_recipients', [])

        for intended_recipient in intended_recipients:
            if isinstance(intended_recipient, PGPKey) and isinstance(intended_recipient._key, PubKeyV4):
                sig._signature.subpackets.addnew('IntendedRecipient', hashed=True, version=4,
                                                 intended_recipient=intended_recipient.fingerprint)
            elif isinstance(intended_recipient, Fingerprint):
                # FIXME: what if it's not a v4 fingerprint?
                sig._signature.subpackets.addnew('IntendedRecipient', hashed=True, version=4,
                                                 intended_recipient=intended_recipient)
            else:
                warnings.warn("Intended Recipient is not a PGPKey, ignoring")

        if expires is not None:
            # expires should be a timedelta, so if it's a datetime, turn it into a timedelta
            if isinstance(expires, datetime):
                expires = expires - self.created

            sig._signature.subpackets.addnew('SignatureExpirationTime', hashed=True, expires=expires)

        if revocable is False:
            sig._signature.subpackets.addnew('Revocable', hashed=True, bflag=revocable)

        if notation is not None:
            for name, value in notation.items():
                # mark all notations as human readable unless value is a bytearray
                flags = NotationDataFlags.HumanReadable
                if isinstance(value, bytearray):
                    flags = 0x00

                sig._signature.subpackets.addnew('NotationData', hashed=True, flags=flags, name=name, value=value)

        if policy_uri is not None:
            sig._signature.subpackets.addnew('Policy', hashed=True, uri=policy_uri)

        if user is not None and uid is not None:
            signers_uid = "{:s}".format(uid)
            sig._signature.subpackets.addnew('SignersUserID', hashed=True, userid=signers_uid)

        # handle an edge case for timestamp signatures vs standalone signatures
        if sig.type == SignatureType.Timestamp and len(sig._signature.subpackets._hashed_sp) > 1:
            sig._signature.sigtype = SignatureType.Standalone

        if prefs.pop('include_issuer_fingerprint', True):
            if isinstance(self._key, PrivKeyV4):
                sig._signature.subpackets.addnew('IssuerFingerprint', hashed=True, _version=4, _issuer_fpr=self.fingerprint)

        sigdata = sig.hashdata(subject)
        h2 = sig.hash_algorithm.hasher
        h2.update(sigdata)
        sig._signature.hash2 = bytearray(h2.digest()[:2])

        _sig = self._key.sign(sigdata, getattr(hashes, sig.hash_algorithm.name)())
        if _sig is NotImplemented:
            raise NotImplementedError(self.key_algorithm)

        sig._signature.signature.from_signer(_sig)
        sig._signature.update_hlen()

        return sig

    @KeyAction(KeyFlags.Sign, is_unlocked=True, is_public=False)
    def sign(self, subject, **prefs):
        """
        Sign text, a message, or a timestamp using this key.

        :param subject: The text to be signed
        :type subject: ``str``, :py:obj:`~pgpy.PGPMessage`, ``None``
        :raises: :py:exc:`~pgpy.errors.PGPError` if the key is passphrase-protected and has not been unlocked
        :raises: :py:exc:`~pgpy.errors.PGPError` if the key is public
        :returns: :py:obj:`PGPSignature`

        The following optional keyword arguments can be used with :py:meth:`PGPKey.sign`, as well as
        :py:meth:`PGPKey.certify`,  :py:meth:`PGPKey.revoke`, and :py:meth:`PGPKey.bind`:

        :keyword expires: Set an expiration date for this signature
        :type expires: :py:obj:`~datetime.datetime`, :py:obj:`~datetime.timedelta`
        :keyword notation: Add arbitrary notation data to this signature.
        :type notation: ``dict``
        :keyword policy_uri: Add a URI to the signature that should describe the policy under which the signature
                             was issued.
        :type policy_uri: ``str``
        :keyword revocable: If ``False``, this signature will be marked non-revocable
        :type revocable: ``bool``
        :keyword user: Specify which User ID to use when creating this signature. Also adds a "Signer's User ID"
                       to the signature.
        :type user: ``str``
        :keyword created: Specify the time that the signature should be made.  If unset or None,
                          it will use the present time.
        :type created: :py:obj:`~datetime.datetime`
        :keyword intended_recipients: Specify a list of :py:obj:`PGPKey` objects that will be encrypted to.
        :type intended_recipients: ``list``
        :keyword include_issuer_fingerprint: Whether to include a hashed subpacket indicating the issuer fingerprint.
                                             (only for v4 keys, defaults to True)
        :type include_issuer_fingerprint: ``bool``
        """
        sig_type = SignatureType.BinaryDocument
        hash_algo = prefs.pop('hash', None)

        if subject is None:
            sig_type = SignatureType.Timestamp

        if isinstance(subject, PGPMessage):
            if subject.type == 'cleartext':
                sig_type = SignatureType.CanonicalDocument

            subject = subject.message

        sig = PGPSignature.new(sig_type, self.key_algorithm, hash_algo, self.fingerprint.keyid, created=prefs.pop('created', None))

        return self._sign(subject, sig, **prefs)

    @KeyAction(KeyFlags.Certify, is_unlocked=True, is_public=False)
    def certify(self, subject, level=SignatureType.Generic_Cert, **prefs):
        """
        certify(subject, level=SignatureType.Generic_Cert, **prefs)

        Sign a key or a user id within a key.

        :param subject: The user id or key to be certified.
        :type subject: :py:obj:`PGPKey`, :py:obj:`PGPUID`
        :param level: :py:obj:`~constants.SignatureType.Generic_Cert`, :py:obj:`~constants.SignatureType.Persona_Cert`,
                      :py:obj:`~constants.SignatureType.Casual_Cert`, or :py:obj:`~constants.SignatureType.Positive_Cert`.
                      Only used if subject is a :py:obj:`PGPUID`; otherwise, it is ignored.
        :raises: :py:exc:`~pgpy.errors.PGPError` if the key is passphrase-protected and has not been unlocked
        :raises: :py:exc:`~pgpy.errors.PGPError` if the key is public
        :returns: :py:obj:`PGPSignature`

        In addition to the optional keyword arguments accepted by :py:meth:`PGPKey.sign`, the following optional
        keyword arguments can be used with :py:meth:`PGPKey.certify`.

        These optional keywords only make sense, and thus only have an effect, when self-signing a key or User ID:

        :keyword usage: A ``set`` of key usage flags, as :py:obj:`~constants.KeyFlags`.
                        This keyword is ignored for non-self-certifications.
        :type usage: ``set``
        :keyword ciphers: A list of preferred symmetric ciphers, as :py:obj:`~constants.SymmetricKeyAlgorithm`.
                          This keyword is ignored for non-self-certifications.
        :type ciphers: ``list``
        :keyword hashes: A list of preferred hash algorithms, as :py:obj:`~constants.HashAlgorithm`.
                         This keyword is ignored for non-self-certifications.
        :type hashes: ``list``
        :keyword compression: A list of preferred compression algorithms, as :py:obj:`~constants.CompressionAlgorithm`.
                              This keyword is ignored for non-self-certifications.
        :type compression: ``list``
        :keyword key_expiration: Specify a key expiration date for when this key should expire, or a
                              :py:obj:`~datetime.timedelta` of how long after the key was created it should expire.
                              This keyword is ignored for non-self-certifications.
        :type key_expiration: :py:obj:`datetime.datetime`, :py:obj:`datetime.timedelta`
        :keyword attested_certifications: A list of third-party certifications, as :py:obj:`PGPSignature`, that
                                          the certificate holder wants to attest to for redistribution with the certificate.
                                          Alternatively, any element in the list can be a ``bytes``  or ``bytearray`` object
                                          of the appropriate length (the length of this certification's digest).
                                          This keyword is only used for signatures of type Attestation.
        :type attested_certifications: ``list``
        :keyword keyserver: Specify the URI of the preferred key server of the user.
                            This keyword is ignored for non-self-certifications.
        :type keyserver: ``str``, ``unicode``, ``bytes``
        :keyword keyserver_flags: A set of Key Server Preferences, as :py:obj:`~constants.KeyServerPreferences`.
        :type keyserver_flags: ``set``
        :keyword primary: Whether or not to consider the certified User ID as the primary one.
                          This keyword is ignored for non-self-certifications, and any certifications directly on keys.
        :type primary: ``bool``

        These optional keywords only make sense, and thus only have an effect, when signing another key or User ID:

        :keyword trust: Specify the level and amount of trust to assert when certifying a public key. Should be a tuple
                        of two ``int`` s, specifying the trust level and trust amount. See
                        `RFC 4880 Section 5.2.3.13. Trust Signature <https://tools.ietf.org/html/rfc4880#section-5.2.3.13>`_
                        for more on what these values mean.
        :type trust: ``tuple`` of two ``int`` s
        :keyword regex: Specify a regular expression to constrain the specified trust signature in the resulting signature.
                        Symbolically signifies that the specified trust signature only applies to User IDs which match
                        this regular expression.
                        This is meaningless without also specifying trust level and amount.
        :type regex: ``str``
        :keyword exportable: Whether this certification is exportable or not.
        :type exportable: ``bool``
        """
        hash_algo = prefs.pop('hash', None)
        sig_type = level
        if isinstance(subject, PGPKey):
            sig_type = SignatureType.DirectlyOnKey

        sig = PGPSignature.new(sig_type, self.key_algorithm, hash_algo, self.fingerprint.keyid, created=prefs.pop('created', None))

        # signature options that only make sense in certifications
        usage = prefs.pop('usage', None)
        exportable = prefs.pop('exportable', None)

        if usage is not None:
            sig._signature.subpackets.addnew('KeyFlags', hashed=True, flags=usage)

        if exportable is not None:
            sig._signature.subpackets.addnew('ExportableCertification', hashed=True, bflag=exportable)

        keyfp = self.fingerprint
        if isinstance(subject, PGPKey):
            keyfp = subject.fingerprint
        if isinstance(subject, PGPUID) and subject._parent is not None:
            keyfp = subject._parent.fingerprint

        if keyfp == self.fingerprint:
            # signature options that only make sense in self-certifications
            cipher_prefs = prefs.pop('ciphers', None)
            hash_prefs = prefs.pop('hashes', None)
            compression_prefs = prefs.pop('compression', None)
            key_expires = prefs.pop('key_expiration', None)
            keyserver_flags = prefs.pop('keyserver_flags', None)
            keyserver = prefs.pop('keyserver', None)
            primary_uid = prefs.pop('primary', None)
            attested_certifications = prefs.pop('attested_certifications', [])

            if key_expires is not None:
                # key expires should be a timedelta, so if it's a datetime, turn it into a timedelta
                if isinstance(key_expires, datetime):
                    key_expires = key_expires - self.created

                sig._signature.subpackets.addnew('KeyExpirationTime', hashed=True, expires=key_expires)

            if cipher_prefs is not None:
                sig._signature.subpackets.addnew('PreferredSymmetricAlgorithms', hashed=True, flags=cipher_prefs)

            if hash_prefs:
                sig._signature.subpackets.addnew('PreferredHashAlgorithms', hashed=True, flags=hash_prefs)
                if sig.hash_algorithm is None:
                    sig._signature.halg = hash_prefs[0]
            if sig.hash_algorithm is None:
                sig._signature.halg = HashAlgorithm.SHA256

            if compression_prefs is not None:
                sig._signature.subpackets.addnew('PreferredCompressionAlgorithms', hashed=True, flags=compression_prefs)

            if keyserver_flags is not None:
                sig._signature.subpackets.addnew('KeyServerPreferences', hashed=True, flags=keyserver_flags)

            if keyserver is not None:
                sig._signature.subpackets.addnew('PreferredKeyServer', hashed=True, uri=keyserver)

            if primary_uid is not None:
                sig._signature.subpackets.addnew('PrimaryUserID', hashed=True, primary=primary_uid)

            cert_sigtypes = {SignatureType.Generic_Cert, SignatureType.Persona_Cert,
                             SignatureType.Casual_Cert, SignatureType.Positive_Cert,
                             SignatureType.CertRevocation}
            # Features is always set on certifications:
            if sig._signature.sigtype in cert_sigtypes:
                sig._signature.subpackets.addnew('Features', hashed=True, flags=Features.pgpy_features)

            # If this is an attestation, then we must include a Attested Certifications subpacket:
            if sig._signature.sigtype == SignatureType.Attestation:
                attestations = set()
                for attestation in attested_certifications:
                    if isinstance(attestation, PGPSignature) and attestation.type in cert_sigtypes:
                        h = sig.hash_algorithm.hasher
                        h.update(attestation._signature.canonical_bytes())
                        attestations.add(h.digest())
                    elif isinstance(attestation, (bytes, bytearray)) and len(attestation) == sig.hash_algorithm.digest_size:
                        attestations.add(attestation)
                    else:
                        warnings.warn(
                            'Attested Certification element is neither a PGPSignature certification nor '
                            'a bytes object of size {:d}; ignoring'.format(sig.hash_algorithm.digest_size)
                        )
                sig._signature.subpackets.addnew('AttestedCertifications', hashed=True, attested_certifications=b''.join(sorted(attestations)))

        else:
            # signature options that only make sense in non-self-certifications
            trust = prefs.pop('trust', None)
            regex = prefs.pop('regex', None)

            if trust is not None:
                sig._signature.subpackets.addnew('TrustSignature', hashed=True, level=trust[0], amount=trust[1])

                if regex is not None:
                    sig._signature.subpackets.addnew('RegularExpression', hashed=True, regex=regex)

        return self._sign(subject, sig, **prefs)

    @KeyAction(KeyFlags.Certify, is_unlocked=True, is_public=False)
    def revoke(self, target, **prefs):
        """
        Revoke a key, a subkey, or all current certification signatures of a User ID that were generated by this key so far.

        :param target: The key to revoke
        :type target: :py:obj:`PGPKey`, :py:obj:`PGPUID`
        :raises: :py:exc:`~pgpy.errors.PGPError` if the key is passphrase-protected and has not been unlocked
        :raises: :py:exc:`~pgpy.errors.PGPError` if the key is public
        :returns: :py:obj:`PGPSignature`

        In addition to the optional keyword arguments accepted by :py:meth:`PGPKey.sign`, the following optional
        keyword arguments can be used with :py:meth:`PGPKey.revoke`.

        :keyword reason: Defaults to :py:obj:`constants.RevocationReason.NotSpecified`
        :type reason: One of :py:obj:`constants.RevocationReason`.
        :keyword comment: Defaults to an empty string.
        :type comment: ``str``
        """
        hash_algo = prefs.pop('hash', None)
        if isinstance(target, PGPUID):
            sig_type = SignatureType.CertRevocation

        elif isinstance(target, PGPKey):
            ##TODO: check to make sure that the key that is being revoked:
            #        - is this key
            #        - is one of this key's subkeys
            #        - specifies this key as its revocation key
            if target.is_primary:
                sig_type = SignatureType.KeyRevocation

            else:
                sig_type = SignatureType.SubkeyRevocation

        else:  # pragma: no cover
            raise TypeError

        sig = PGPSignature.new(sig_type, self.key_algorithm, hash_algo, self.fingerprint.keyid, created=prefs.pop('created', None))

        # signature options that only make sense when revoking
        reason = prefs.pop('reason', RevocationReason.NotSpecified)
        comment = prefs.pop('comment', "")
        sig._signature.subpackets.addnew('ReasonForRevocation', hashed=True, code=reason, string=comment)

        return self._sign(target, sig, **prefs)

    @KeyAction(is_unlocked=True, is_public=False)
    def revoker(self, revoker, **prefs):
        """
        Generate a signature that specifies another key as being valid for revoking this key.

        :param revoker: The :py:obj:`PGPKey` to specify as a valid revocation key.
        :type revoker: :py:obj:`PGPKey`
        :raises: :py:exc:`~pgpy.errors.PGPError` if the key is passphrase-protected and has not been unlocked
        :raises: :py:exc:`~pgpy.errors.PGPError` if the key is public
        :returns: :py:obj:`PGPSignature`

        In addition to the optional keyword arguments accepted by :py:meth:`PGPKey.sign`, the following optional
        keyword arguments can be used with :py:meth:`PGPKey.revoker`.

        :keyword sensitive: If ``True``, this sets the sensitive flag on the RevocationKey subpacket. Currently,
                            this has no other effect.
        :type sensitive: ``bool``
        """
        hash_algo = prefs.pop('hash', None)

        sig = PGPSignature.new(SignatureType.DirectlyOnKey, self.key_algorithm, hash_algo, self.fingerprint.keyid, created=prefs.pop('created', None))

        # signature options that only make sense when adding a revocation key
        sensitive = prefs.pop('sensitive', False)
        keyclass = RevocationKeyClass.Normal | (RevocationKeyClass.Sensitive if sensitive else 0x00)

        sig._signature.subpackets.addnew('RevocationKey',
                                         hashed=True,
                                         algorithm=revoker.key_algorithm,
                                         fingerprint=revoker.fingerprint,
                                         keyclass=keyclass)

        # revocation keys should really not be revocable themselves
        prefs['revocable'] = False
        return self._sign(self, sig, **prefs)

    @KeyAction(is_unlocked=True, is_public=False)
    def bind(self, key, **prefs):
        """
        Bind a subkey to this key.

        In addition to the optional keyword arguments accepted for self-signatures by :py:meth:`PGPkey.certify`,
        the following optional keyword arguments can be used with :py:meth:`PGPKey.bind`.

        :keyword crosssign: If ``False``, do not attempt a cross-signature (defaults to ``True``). Subkeys
                            which are not capable of signing will not produce a cross-signature in any case.
                            Setting ``crosssign`` to ``False`` is likely to produce subkeys that will be rejected
                            by some other OpenPGP implementations.
        :type crosssign: ``bool``
        """
        hash_algo = prefs.pop('hash', None)

        if self.is_primary and not key.is_primary:
            sig_type = SignatureType.Subkey_Binding

        elif key.is_primary and not self.is_primary:
            sig_type = SignatureType.PrimaryKey_Binding

        else:  # pragma: no cover
            raise PGPError

        sig = PGPSignature.new(sig_type, self.key_algorithm, hash_algo, self.fingerprint.keyid, created=prefs.pop('created', None))

        if sig_type == SignatureType.Subkey_Binding:
            # signature options that only make sense in subkey binding signatures
            usage = prefs.pop('usage', None)

            if usage is not None:
                sig._signature.subpackets.addnew('KeyFlags', hashed=True, flags=usage)

            crosssig = None
            # if possible, have the subkey create a primary key binding signature
            if key.key_algorithm.can_sign and prefs.pop('crosssign', True):
                subkeyid = key.fingerprint.keyid

                if not key.is_public:
                    crosssig = key.bind(self)

                elif subkeyid in self.subkeys:  # pragma: no cover
                    crosssig = self.subkeys[subkeyid].bind(self)

            if crosssig is None:
                if usage is None:
                    raise PGPError('subkey with no key usage flags (may be used for any purpose, including signing) requires a cross-signature')
                if KeyFlags.Sign in usage:
                    raise PGPError('subkey marked for signing usage requires a cross-signature')
            else:
                sig._signature.subpackets.addnew('EmbeddedSignature', hashed=False, _sig=crosssig._signature)

        return self._sign(key, sig, **prefs)

    def is_considered_insecure(self, self_verifying=False):
        res = self.check_soundness(self_verifying=self_verifying)

        for sk in self.subkeys.values():
            res |= sk.check_soundness(self_verifying=self_verifying)
        return res

    def self_verify(self):
        self_sigs = list(self.self_signatures)
        res = SecurityIssues.OK
        if self_sigs:
            for s in self_sigs:
                if not self.verify(self, s):
                    res |= SecurityIssues.Invalid
                    break
        else:
            return SecurityIssues.NoSelfSignature
        return res

    def _do_self_signatures_verification(self):
        try:
            self._self_verified = self.self_verify()
        except Exception:
            self._self_verified = None
            raise

    @property
    def self_verified(self):
        warnings.warn("TODO: Self-sigs verification is not yet working because self-sigs are not parsed!!!")
        return SecurityIssues.OK

        if self._self_verified is None:
            self._do_self_signatures_verification()

        return self._self_verified

    def check_primitives(self):
        return self.key_algorithm.validate_params(self.key_size)

    def check_management(self, self_verifying=False):
        res = self.self_verified
        if self.is_expired:
            warnings.warn('Key {} has expired at {:s}'.format(repr(self), self.expires_at))
            res |= SecurityIssues.Expired

        warnings.warn("TODO: Revocation checks are not yet implemented!!!")
        warnings.warn("TODO: Flags (s.a. `disabled`) checks are not yet implemented!!!")
        res |= int(bool(list(self.revocation_signatures))) * SecurityIssues.Revoked
        return res

    def check_soundness(self, self_verifying=False):
        return self.check_management(self_verifying) | self.check_primitives()

    def verify(self, subject, signature=None):
        """
        Verify a subject with a signature using this key.

        :param subject: The subject to verify
        :type subject: ``str``, ``unicode``, ``None``, :py:obj:`PGPMessage`, :py:obj:`PGPKey`, :py:obj:`PGPUID`
        :param signature: If the signature is detached, it should be specified here.
        :type signature: :py:obj:`PGPSignature`
        :returns: :py:obj:`~pgpy.types.SignatureVerification`
        """
        sspairs = []

        # some type checking
        if not isinstance(subject, (type(None), PGPMessage, PGPKey, PGPUID, PGPSignature, str, bytes, bytearray)):
            raise TypeError("Unexpected subject value: {:s}".format(str(type(subject))))
        if not isinstance(signature, (type(None), PGPSignature)):
            raise TypeError("Unexpected signature value: {:s}".format(str(type(signature))))

        def _filter_sigs(sigs):
            _ids = {self.fingerprint.keyid} | set(self.subkeys)
            for sig in sigs:
                if sig.signer in _ids:
                    yield sig

        # collect signature(s)
        if signature is None:
            if isinstance(subject, PGPMessage):
                for sig in _filter_sigs(subject.signatures):
                    sspairs.append((sig, subject.message))

            if isinstance(subject, (PGPUID, PGPKey)):
                sspairs += [ (sig, subject) for sig in _filter_sigs(subject.__sig__) ]

            if isinstance(subject, PGPKey):
                # user ids
                for uid in subject.userids:
                    for sig in _filter_sigs(uid.__sig__):
                        sspairs.append((sig, uid))
                # user attributes
                for ua in subject.userattributes:
                    for sig in _filter_sigs(ua.__sig__):
                        sspairs.append((sig, ua))

                # subkey binding signatures
                for subkey in subject.subkeys.values():
                    for sig in _filter_sigs(subkey.__sig__):
                        sspairs.append((sig, subkey))

        elif signature.signer in {self.fingerprint.keyid} | set(self.subkeys):
            sspairs += [(signature, subject)]

        if len(sspairs) == 0:
            raise PGPError("No signatures to verify")

        # finally, start verifying signatures
        sigv = SignatureVerification()
        for sig, subj in sspairs:
            if self.fingerprint.keyid != sig.signer and sig.signer in self.subkeys:
                sigv &= self.subkeys[sig.signer].verify(subj, sig)

            else:
                if isinstance(subj, PGPKey):
                    self_verifying = sig.signer == subj.fingerprint
                else:
                    self_verifying = False

                subkey_issues = self.check_soundness(self_verifying)
                signature_issues = self.check_primitives()

                if self_verifying:
                    signature_issues &= ~SecurityIssues.HashFunctionNotCollisionResistant

                issues = signature_issues | subkey_issues
                if issues and issues.causes_signature_verify_to_fail:
                    sigv.add_sigsubj(sig, self, subj, issues)
                else:
                    verified = self._key.verify(sig.hashdata(subj), sig.__sig__, getattr(hashes, sig.hash_algorithm.name)())
                    if verified is NotImplemented:
                        raise NotImplementedError(sig.key_algorithm)

                    sigv.add_sigsubj(sig, self, subj, SecurityIssues.WrongSig if not verified else SecurityIssues.OK)

        return sigv

    @KeyAction(KeyFlags.EncryptCommunications, KeyFlags.EncryptStorage, is_public=True)
    def encrypt(self, message, sessionkey=None, **prefs):
        """encrypt(message[, sessionkey=None], **prefs)

        Encrypt a PGPMessage using this key.

        :param message: The message to encrypt.
        :type message: :py:obj:`PGPMessage`
        :param sessionkey: Provide a session key to use when encrypting something. Default is ``None``.
                                    If ``None``, a session key of the appropriate length will be generated randomly.

                                    .. warning::

                                        Care should be taken when making use of this option! Session keys *absolutely need*
                                        to be unpredictable! Use the ``gen_key()`` method on the desired
                                        :py:obj:`~constants.SymmetricKeyAlgorithm` to generate the session key!
        :type sessionkey: ``bytes``, ``str``

        :raises: :py:exc:`~errors.PGPEncryptionError` if encryption failed for any reason.
        :returns: A new :py:obj:`PGPMessage` with the encrypted contents of ``message``

        The following optional keyword arguments can be used with :py:meth:`PGPKey.encrypt`:

        :keyword cipher: Specifies the symmetric block cipher to use when encrypting the message.
        :type cipher: :py:obj:`~constants.SymmetricKeyAlgorithm`
        :keyword user: Specifies the User ID to use as the recipient for this encryption operation, for the purposes of
                       preference defaults and selection validation.
        :type user: ``str``, ``unicode``
        """
        user = prefs.pop('user', None)
        uid = None
        if user is not None:
            uid = self.get_uid(user)
        else:
            uid = next(iter(self.userids), None)
            if uid is None and self.parent is not None:
                uid = next(iter(self.parent.userids), None)
        pref_cipher = next((c for c in uid.selfsig.cipherprefs if c.is_supported), SymmetricKeyAlgorithm.TripleDES)
        cipher_algo = prefs.pop('cipher', pref_cipher)

        if cipher_algo not in uid.selfsig.cipherprefs:
            warnings.warn("Selected symmetric algorithm not in key preferences", stacklevel=3)

        if message.is_compressed and message._compression not in uid.selfsig.compprefs:
            warnings.warn("Selected compression algorithm not in key preferences", stacklevel=3)

        if sessionkey is None:
            sessionkey = cipher_algo.gen_key()

        # set up a new PKESessionKeyV3
        pkesk = PKESessionKeyV3()
        pkesk.encrypter = bytearray(binascii.unhexlify(self.fingerprint.keyid.encode('latin-1')))
        pkesk.pkalg = self.key_algorithm
        pkesk.encrypt_sk(self._key, cipher_algo, sessionkey)

        if message.is_encrypted:  # pragma: no cover
            _m = message

        else:
            _m = PGPMessage()
            skedata = IntegrityProtectedSKEDataV1()
            skedata.encrypt(sessionkey, cipher_algo, message.__bytes__())
            _m |= skedata

        _m |= pkesk

        return _m

    @KeyAction(is_unlocked=True, is_public=False)
    def decrypt(self, message):
        """
        Decrypt a PGPMessage using this key.

        :param message: An encrypted :py:obj:`PGPMessage`
        :raises: :py:exc:`~errors.PGPError` if the key is not private, or protected but not unlocked.
        :raises: :py:exc:`~errors.PGPDecryptionError` if decryption fails for any other reason.
        :returns: A new :py:obj:`PGPMessage` with the decrypted contents of ``message``.
        """
        if not message.is_encrypted:
            warnings.warn("This message is not encrypted", stacklevel=3)
            return message

        if self.fingerprint.keyid not in message.encrypters:
            sks = set(self.subkeys)
            mis = set(message.encrypters)
            if sks & mis:
                skid = list(sks & mis)[0]
                return self.subkeys[skid].decrypt(message)

            raise PGPError("Cannot decrypt the provided message with this key")

        pkesk = next(pk for pk in message._sessionkeys if pk.pkalg == self.key_algorithm and pk.encrypter == self.fingerprint.keyid)
        alg, key = pkesk.decrypt_sk(self._key)

        # now that we have the symmetric cipher used and the key, we can decrypt the actual message
        decmsg = PGPMessage()
        decmsg.parse(message.message.decrypt(key, alg))

        return decmsg

    def parse(self, data):
        unarmored = self.ascii_unarmor(data)
        data = unarmored['body']

        if unarmored['magic'] is not None and 'KEY' not in unarmored['magic']:
            raise ValueError('Expected: KEY. Got: {}'.format(str(unarmored['magic'])))

        if unarmored['headers'] is not None:
            self.ascii_headers = unarmored['headers']

        # parse packets
        # keys will hold other keys parsed here
        keys = collections.OrderedDict()
        # orphaned will hold all non-opaque orphaned packets
        orphaned = []
        # last holds the last non-signature thing processed

        ##TODO: see issue #141 and fix this better
        def _getpkt(d):
            return Packet(d) if d else None
        # some packets are filtered out
        getpkt = filter(lambda p: p.header.tag != PacketTag.Trust, iter(functools.partial(_getpkt, data), None))

        def pktgrouper():
            class PktGrouper(object):
                def __init__(self):
                    self.last = None

                def __call__(self, pkt):
                    if pkt.header.tag != PacketTag.Signature:
                        self.last = '{:02X}_{:s}'.format(id(pkt), pkt.__class__.__name__)
                    return self.last
            return PktGrouper()

        while True:
            for group in iter(group for _, group in itertools.groupby(getpkt, key=pktgrouper()) if not _.endswith('Opaque')):
                pkt = next(group)

                # deal with pkt first
                if isinstance(pkt, Key):
                    pgpobj = (self if self._key is None else PGPKey()) | pkt

                elif isinstance(pkt, (UserID, UserAttribute)):
                    pgpobj = PGPUID() | pkt

                else:  # pragma: no cover
                    break

                # add signatures to whatever we got
                [ operator.ior(pgpobj, PGPSignature() | sig) for sig in group if not isinstance(sig, Opaque) ]

                # and file away pgpobj
                if isinstance(pgpobj, PGPKey):
                    if pgpobj.is_primary:
                        keys[(pgpobj.fingerprint.keyid, pgpobj.is_public)] = pgpobj

                    else:
                        keys[next(reversed(keys))] |= pgpobj

                elif isinstance(pgpobj, PGPUID):
                    # parent is likely the most recently parsed primary key
                    keys[next(reversed(keys))] |= pgpobj

                else:  # pragma: no cover
                    break
            else:
                # finished normally
                break

            # this will only be reached called if the inner loop hit a break
            warnings.warn("Warning: Orphaned packet detected! {:s}".format(repr(pkt)), stacklevel=2)  # pragma: no cover
            orphaned.append(pkt)  # pragma: no cover
            for pkt in group:  # pragma: no cover
                orphaned.append(pkt)

        # remove the reference to self from keys
        [ keys.pop((getattr(self, 'fingerprint.keyid', '~'), None), t) for t in (True, False) ]
        # return {'keys': keys, 'orphaned': orphaned}
        return keys


class PGPKeyring(collections_abc.Container, collections_abc.Iterable, collections_abc.Sized):
    def __init__(self, *args):
        """
        PGPKeyring objects represent in-memory keyrings that can contain any combination of supported private and public
        keys. It can not currently be conveniently exported to a format that can be understood by GnuPG.
        """
        super(PGPKeyring, self).__init__()
        self._keys = {}
        self._pubkeys = collections.deque()
        self._privkeys = collections.deque()
        self._aliases = collections.deque([{}])
        self.load(*args)

    def __contains__(self, alias):
        aliases = set().union(*self._aliases)

        if isinstance(alias, str):
            return alias in aliases or alias.replace(' ', '') in aliases

        return alias in aliases  # pragma: no cover

    def __len__(self):
        return len(self._keys)

    def __iter__(self):  # pragma: no cover
        for pgpkey in itertools.chain(self._pubkeys, self._privkeys):
            yield pgpkey

    def _get_key(self, alias):
        for m in self._aliases:
            if alias in m:
                return self._keys[m[alias]]

            if alias.replace(' ', '') in m:
                return self._keys[m[alias.replace(' ', '')]]

        raise KeyError(alias)

    def _get_keys(self, alias):
        return [self._keys[m[alias]] for m in self._aliases if alias in m]

    def _sort_alias(self, alias):
        # remove alias from all levels of _aliases, and sort by created time and key half
        # so the order of _aliases from left to right:
        #  - newer keys come before older ones
        #  - private keys come before public ones
        #
        # this list is sorted in the opposite direction from that, because they will be placed into self._aliases
        # from right to left.
        pkids = sorted(list(set().union(m.pop(alias) for m in self._aliases if alias in m)),
                       key=lambda pkid: (self._keys[pkid].created, self._keys[pkid].is_public))

        # drop the now-sorted aliases into place
        for depth, pkid in enumerate(pkids):
            self._aliases[depth][alias] = pkid

        # finally, remove any empty dicts left over
        while {} in self._aliases:  # pragma: no cover
            self._aliases.remove({})

    def _add_alias(self, alias, pkid):
        # brand new alias never seen before!
        if alias not in self:
            self._aliases[-1][alias] = pkid

        # this is a duplicate alias->key link; ignore it
        elif alias in self and pkid in set(m[alias] for m in self._aliases if alias in m):
            pass  # pragma: no cover

        # this is an alias that already exists, but points to a key that is not already referenced by it
        else:
            adepth = len(self._aliases) - len([None for m in self._aliases if alias in m]) - 1
            # all alias maps have this alias, so increase total depth by 1
            if adepth == -1:
                self._aliases.appendleft({})
                adepth = 0

            self._aliases[adepth][alias] = pkid
            self._sort_alias(alias)

    def _add_key(self, pgpkey):
        pkid = id(pgpkey)
        if pkid not in self._keys:
            self._keys[pkid] = pgpkey

            # add to _{pub,priv}keys if this is either a primary key, or a subkey without one
            if pgpkey.parent is None:
                if pgpkey.is_public:
                    self._pubkeys.append(pkid)

                else:
                    self._privkeys.append(pkid)

            # aliases
            self._add_alias(pgpkey.fingerprint, pkid)
            self._add_alias(pgpkey.fingerprint.keyid, pkid)
            self._add_alias(pgpkey.fingerprint.shortid, pkid)
            for uid in pgpkey.userids:
                self._add_alias(uid.name, pkid)
                if uid.comment:
                    self._add_alias(uid.comment, pkid)

                if uid.email:
                    self._add_alias(uid.email, pkid)

            # subkeys
            for subkey in pgpkey.subkeys.values():
                self._add_key(subkey)

    def load(self, *args):
        r"""
        Load all keys provided into this keyring object.

        :param \*args: Each arg in ``args`` can be any of the formats supported by :py:meth:`PGPKey.from_file` and
                      :py:meth:`PGPKey.from_blob` or a :py:class:`PGPKey` instance, or a ``list`` or ``tuple`` of these.
        :type \*args: ``list``, ``tuple``, ``str``, ``unicode``, ``bytes``, ``bytearray``
        :returns: a ``set`` containing the unique fingerprints of all of the keys that were loaded during this operation.
        """
        def _preiter(first, iterable):
            yield first
            for item in iterable:
                yield item

        loaded = set()
        for key in iter(item for ilist in iter(ilist if isinstance(ilist, (tuple, list)) else [ilist] for ilist in args)
                        for item in ilist):
            keys = {}
            if isinstance(key, PGPKey):
                _key = key
            elif os.path.isfile(key):
                _key, keys = PGPKey.from_file(key)
            else:
                _key, keys = PGPKey.from_blob(key)

            for ik in _preiter(_key, keys.values()):
                self._add_key(ik)
                loaded |= {ik.fingerprint} | {isk.fingerprint for isk in ik.subkeys.values()}

        return list(loaded)

    @contextlib.contextmanager
    def key(self, identifier):
        """
        A context-manager method. Yields the first :py:obj:`PGPKey` object that matches the provided identifier.

        :param identifier: The identifier to use to select a loaded key.
        :type identifier: :py:exc:`PGPMessage`, :py:exc:`PGPSignature`, ``str``
        :raises: :py:exc:`KeyError` if there is no loaded key that satisfies the identifier.
        """
        if isinstance(identifier, PGPMessage):
            for issuer in identifier.issuers:
                if issuer in self:
                    identifier = issuer
                    break

        if isinstance(identifier, PGPSignature):
            identifier = identifier.signer

        yield self._get_key(identifier)

    def fingerprints(self, keyhalf='any', keytype='any'):
        """
        List loaded fingerprints with some optional filtering.

        :param keyhalf: Can be 'any', 'public', or 'private'. If 'public', or 'private', the fingerprints of keys of the
                            the other type will not be included in the results.
        :type keyhalf: ``str``
        :param keytype: Can be 'any', 'primary', or 'sub'. If 'primary' or 'sub', the fingerprints of keys of the
                            the other type will not be included in the results.
        :type keytype: ``str``
        :returns: a ``set`` of fingerprints of keys matching the filters specified.
        """
        return {pk.fingerprint for pk in self._keys.values()
                if pk.is_primary in [True if keytype in ['primary', 'any'] else None,
                                     False if keytype in ['sub', 'any'] else None]
                if pk.is_public in [True if keyhalf in ['public', 'any'] else None,
                                    False if keyhalf in ['private', 'any'] else None]}

    def unload(self, key):
        """
        Unload a loaded key and its subkeys.

        :param key: The key to unload.
        :type key: :py:obj:`PGPKey`

        The easiest way to do this is to select a key using :py:meth:`PGPKeyring.key` first::

            with keyring.key("DSA von TestKey") as key:
                keyring.unload(key)
        """
        assert isinstance(key, PGPKey)
        pkid = id(key)
        if pkid in self._keys:
            # remove references
            [ kd.remove(pkid) for kd in [self._pubkeys, self._privkeys] if pkid in kd ]
            # remove the key
            self._keys.pop(pkid)

            # remove aliases
            for m, a in [ (m, a) for m in self._aliases for a, p in m.items() if p == pkid ]:
                m.pop(a)
                # do a re-sort of this alias if it was not unique
                if a in self:
                    self._sort_alias(a)

            # if key is a primary key, unload its subkeys as well
            if key.is_primary:
                [ self.unload(sk) for sk in key.subkeys.values() ]
