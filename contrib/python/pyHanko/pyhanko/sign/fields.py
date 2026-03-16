"""
Utilities to deal with signature form fields and their properties in PDF files.
"""

import logging
from dataclasses import dataclass
from enum import Enum, Flag, unique
from typing import List, Optional, Set, Tuple, Union

from asn1crypto import x509
from asn1crypto.x509 import KeyUsage
from pyhanko_certvalidator.authority import AuthorityWithCert
from pyhanko_certvalidator.errors import InvalidCertificateError
from pyhanko_certvalidator.path import ValidationPath

from pyhanko.pdf_utils import generic
from pyhanko.pdf_utils.content import RawContent
from pyhanko.pdf_utils.generic import pdf_name, pdf_string
from pyhanko.pdf_utils.layout import BoxConstraints
from pyhanko.pdf_utils.misc import (
    OrderedEnum,
    PdfError,
    PdfReadError,
    PdfWriteError,
    get_and_apply,
    rd,
)
from pyhanko.pdf_utils.rw_common import PdfHandler
from pyhanko.pdf_utils.writer import BasePdfFileWriter
from pyhanko.sign.general import SigningError, UnacceptableSignerError

__all__ = [
    'SigFieldSpec',
    'SigSeedValFlags',
    'SigCertConstraints',
    'SigSeedValueSpec',
    'SigCertConstraintFlags',
    'SigSeedSubFilter',
    'SeedValueDictVersion',
    'SeedLockDocument',
    'SigCertKeyUsage',
    'MDPPerm',
    'FieldMDPAction',
    'FieldMDPSpec',
    'SignatureFormField',
    'InvisSigSettings',
    'VisibleSigSettings',
    'enumerate_sig_fields',
    'append_signature_field',
    'ensure_sig_flags',
    'prepare_sig_field',
    'apply_sig_field_spec_properties',
    'annot_width_height',
    'get_sig_field_annot',
]

logger = logging.getLogger(__name__)


# TODO support other seed value dict entries
# TODO add more customisability appearance-wise


class MDPPerm(OrderedEnum):
    """
    Indicates a ``/DocMDP`` level.

    Cf. Table 254  in ISO 32000-1.
    """

    NO_CHANGES = 1
    """
    No changes to the document are allowed.

    .. warning::
        This does not apply to DSS updates and the addition of document time
        stamps.
    """
    FILL_FORMS = 2
    """
    Form filling & signing is allowed.
    """

    ANNOTATE = 3
    """
    Form filling, signing and commenting are allowed.

    .. warning::
        Validating this ``/DocMDP`` level is not currently supported,
        but included in the list for completeness.
    """


class SeedSignatureType:
    """
    Signature type indicator to be embedded into the seed value dictionary
    attached to a signature field.

    :param mdp_perm:
        If not ``None``, indicates that the signature field is intended for
        a certification signature. The :class:`MDPPerm` value passed as the
        ``mdp_perm`` parameter indicates the modification policy that the
        certification signature should use.

        A value of ``None`` indicates that the signature field is intended for
        an approval signature (i.e. a non-certification signature).
    """

    def __init__(self, mdp_perm: Optional[MDPPerm] = None):
        self.mdp_perm = mdp_perm

    def __eq__(self, other):
        return (
            isinstance(other, SeedSignatureType)
            and other.mdp_perm == self.mdp_perm
        )

    def certification_signature(self) -> bool:
        return self.mdp_perm is not None


class SigSeedValFlags(Flag):
    """
    Flags for the ``/Ff`` entry in the seed value dictionary for a signature
    field. These mark which of the constraints are to be strictly enforced,
    as opposed to optional ones.

    .. warning::
        The flags :attr:`LEGAL_ATTESTATION` and :attr:`APPEARANCE_FILTER` are
        processed in accordance with the specification when creating a
        signature, but support is nevertheless limited.

        * PyHanko does not support legal attestations at all, so given that
          the :attr:`LEGAL_ATTESTATION` requirement flag only restricts the
          legal attestations that can be used by the signer, pyHanko can safely
          ignore it when signing.

          On the other hand, since the validator is not aware of
          legal attestations either, it cannot validate signatures that
          make :attr:`~.SigSeedValueSpec.legal_attestations` a mandatory
          constraint.
        * Since pyHanko does not define any named appearances, setting
          the :attr:`APPEARANCE_FILTER` flag and the
          :attr:`~.SigSeedValueSpec.appearance` entry in the seed value
          dictionary will make pyHanko refuse to sign the document.

          When validating, the situation is different: since pyHanko has no
          way of knowing whether the signer used the named appearance imposed
          by the seed value dictionary, it will simply emit a warning and
          continue validating the signature.
    """

    FILTER = 1
    """
    Makes the signature handler setting mandatory. PyHanko only supports
    ``/Adobe.PPKLite``.
    """

    SUBFILTER = 2
    """
    See :attr:`~.SigSeedValueSpec.subfilters`.
    """

    V = 4
    """
    See :attr:`~.SigSeedValueSpec.sv_dict_version`.
    """

    REASONS = 8
    """
    See :attr:`~.SigSeedValueSpec.reasons`.
    """

    LEGAL_ATTESTATION = 16
    """
    See :attr:`~.SigSeedValueSpec.legal_attestations`.
    """

    ADD_REV_INFO = 32
    """
    See :attr:`~.SigSeedValueSpec.add_rev_info`.
    """

    DIGEST_METHOD = 64
    """
    See :attr:`~.SigSeedValueSpec.digest_method`.
    """

    LOCK_DOCUMENT = 128
    """
    See :attr:`~.SigSeedValueSpec.lock_document`.
    """

    APPEARANCE_FILTER = 256
    """
    See :attr:`~.SigSeedValueSpec.appearance`.
    """


class SigCertConstraintFlags(Flag):
    """
    Flags for the ``/Ff`` entry in the certificate seed value dictionary for
    a dictionary field. These mark which of the constraints are to be
    strictly enforced, as opposed to optional ones.

    .. warning::
        While this enum records values for all flags, not all corresponding
        constraint types have been implemented yet.
    """

    SUBJECT = 1
    """
    See :attr:`SigCertConstraints.subjects`.
    """

    ISSUER = 2
    """
    See :attr:`SigCertConstraints.issuers`.
    """

    OID = 4
    """
    Currently not supported.
    """

    SUBJECT_DN = 8
    """ 
    See :attr:`SigCertConstraints.subject_dn`.
    """

    RESERVED = 16
    """
    Currently not supported (reserved).
    """

    KEY_USAGE = 32
    """
    See :attr:`SigCertConstraints.key_usage`.
    """

    URL = 64
    """
    See :attr:`SigCertConstraints.info_url`.
    
    .. note::
        As specified in the standard, this enforcement bit is supposed to be
        ignored by default. We include it for compatibility reasons.
    """

    UNSUPPORTED = RESERVED | OID
    """
    Flags for which the corresponding constraint is unsupported.
    """


class SigCertKeyUsage:
    """
    Encodes the key usage bits that must (resp. must not) be active on the
    signer's certificate.

    .. note::
        See § 4.2.1.3 in :rfc:`5280` and :class:`.KeyUsage` for more
        information on key usage extensions.

    .. note::
        The human-readable names of the key usage extensions are recorded
        in ``camelCase`` in :rfc:`5280`, but this class uses
        the naming convention of :class:`.KeyUsage` in ``asn1crypto``.
        The conversion is done by replacing ``camelCase`` with ``snake_case``.
        For example, ``nonRepudiation`` becomes ``non_repudiation``, and
        ``digitalSignature`` turns into ``digital_signature``.

    .. note::
        This class is intended to closely replicate the definition of the
        KeyUsage entry Table 235 in ISO 32000-1.
        In particular, it does *not* provide a mechanism to deal
        with extended key usage extensions (cf. § 4.2.1.12 in :rfc:`5280`).

    :param must_have:
        The :class:`.KeyUsage` object encoding the key usage extensions
        that must be present on the signer's certificate.
    :param forbidden:
        The :class:`.KeyUsage` object encoding the key usage extensions
        that must *not* be present on the signer's certificate.
    """

    def __init__(
        self,
        must_have: Optional[KeyUsage] = None,
        forbidden: Optional[KeyUsage] = None,
    ):
        self.must_have = must_have if must_have is not None else KeyUsage(set())
        self.forbidden = forbidden if forbidden is not None else KeyUsage(set())

    def encode_to_sv_string(self):
        """
        Encode the key usage requirements in the format specified in the PDF
        specification.

        :return:
            A string.
        """

        def fmt_bit(bit: int):
            if self.must_have[bit]:
                return '1'
            elif self.forbidden[bit]:
                return '0'
            else:
                return 'X'

        return ''.join(fmt_bit(bit) for bit in range(9))

    @classmethod
    def read_from_sv_string(cls, ku_str):
        """
        Parse a PDF KeyUsage string into an instance of
        :class:`.SigCertKeyUsage`. See Table 235 in ISO 32000-1.

        :param ku_str:
            A PDF KeyUsage string.
        :return:
            An instance of :class:`.SigCertKeyUsage`.
        """
        ku_str = ku_str[:9]

        def _as_tuple(with_val):
            return tuple(1 if val == with_val else 0 for val in ku_str)

        return SigCertKeyUsage(
            must_have=KeyUsage(_as_tuple('1')),
            forbidden=KeyUsage(_as_tuple('0')),
        )

    @classmethod
    def from_sets(
        cls,
        must_have: Optional[Set[str]] = None,
        forbidden: Optional[Set[str]] = None,
    ):
        """
        Initialise a :class:`.SigCertKeyUsage` object from two sets.

        :param must_have:
            The key usage extensions that must be present on the signer's
            certificate.
        :param forbidden:
            The key usage extensions that must *not* be present on the signer's
            certificate.
        :return:
            A :class:`.SigCertKeyUsage` object encoding these.
        """
        return SigCertKeyUsage(
            must_have=KeyUsage(set() if must_have is None else must_have),
            forbidden=KeyUsage(set() if forbidden is None else forbidden),
        )

    def must_have_set(self) -> Set[str]:
        """
        Return the set of key usage extensions that must be present
        on the signer's certificate.
        """
        return self.must_have.native

    def forbidden_set(self) -> Set[str]:
        """
        Return the set of key usage extensions that must not be present
        on the signer's certificate.
        """
        return self.forbidden.native

    def __eq__(self, other):
        return (
            isinstance(other, SigCertKeyUsage)
            and self.must_have_set() == other.must_have_set()
            and self.forbidden_set() == other.forbidden_set()
        )


name_type_abbrevs = {
    '2.5.4.3': 'CN',
    '2.5.4.5': 'SerialNumber',
    '2.5.4.6': 'C',
    '2.5.4.7': 'L',
    '2.5.4.8': 'ST',
    '2.5.4.10': 'O',
    '2.5.4.11': 'OU',
}

name_type_abbrevs_rev = {v: k for k, v in name_type_abbrevs.items()}


def x509_name_keyval_pairs(name: x509.Name, abbreviate_oids=False):
    rdns: x509.RDNSequence = name.chosen
    for rdn in rdns:
        for type_and_value in rdn:
            oid: x509.NameType = type_and_value['type']
            # these are all some kind of string, and the PDF
            # standard says that the value should be a text string object,
            # so we just have asn1crypto convert everything to strings
            value = type_and_value['value']
            key = oid.dotted
            if abbreviate_oids:
                key = name_type_abbrevs.get(key, key)

            yield key, value.native
            # these should be strings


@dataclass(frozen=True)
class SigCertConstraints:
    """
    This part of the seed value dictionary allows the document author
    to set constraints on the signer's certificate.

    See Table 235 in ISO 32000-1.
    """

    flags: SigCertConstraintFlags = SigCertConstraintFlags(0)
    """
    Enforcement flags. By default, all entries are optional.
    """

    subjects: Optional[List[x509.Certificate]] = None
    """
    Explicit list of certificates that can be used to sign a signature field.
    """

    subject_dn: Optional[x509.Name] = None
    """
    Certificate subject names that can be used to sign a signature field.
    Subject DN entries that are not mentioned are unconstrained.
    """

    issuers: Optional[List[x509.Certificate]] = None
    """
    List of issuer certificates that the signer certificate can be issued by.
    Note that these issuers do not need to be the *direct* issuer of the
    signer's certificate; any descendant relationship will do.
    """

    info_url: Optional[str] = None
    """
    Informational URL that should be opened when an appropriate certificate
    cannot be found (if :attr:`url_type` is ``/Browser``, that is).
    
    .. note::
        PyHanko ignores this value, but we include it for compatibility.
    """

    url_type: generic.NameObject = pdf_name('/Browser')
    """
    Handler that should be used to open :attr:`info_url`.
    ``/Browser`` is the only implementation-independent value.
    """

    key_usage: Optional[List[SigCertKeyUsage]] = None
    """
    Specify the key usage extensions that should (or should not) be present
    on the signer's certificate.
    """

    # TODO support OID constraints (certificate policies) and signature policy
    #  constraints.

    @classmethod
    def from_pdf_object(cls, pdf_dict):
        """
        Read a PDF dictionary into a :class:`.SigCertConstraints` object.

        :param pdf_dict:
            A :class:`~.generic.DictionaryObject`.
        :return:
            A :class:`.SigCertConstraints` object.
        """

        if isinstance(pdf_dict, generic.IndirectObject):
            pdf_dict = pdf_dict.get_object()
        try:
            if pdf_dict['/Type'] != '/SVCert':  # pragma: nocover
                raise PdfReadError('Object /Type entry is not /SVCert')
        except KeyError:  # pragma: nocover
            pass
        flags = SigCertConstraintFlags(pdf_dict.get('/Ff', 0))
        subjects = [
            x509.Certificate.load(cert.original_bytes)
            for cert in pdf_dict.get('/Subject', ())
        ]
        issuers = [
            x509.Certificate.load(cert.original_bytes)
            for cert in pdf_dict.get('/Issuer', ())
        ]

        def format_attr(attr):
            # strip initial /
            attr = attr[1:]
            # attempt to convert abbreviated attrs to OIDs, since build()
            # takes OIDs
            return name_type_abbrevs_rev.get(attr.upper(), attr)

        subject_dns = x509.Name.build(
            {
                format_attr(attr): value
                for dn_dir in pdf_dict.get('/SubjectDN', ())
                for attr, value in dn_dir.items()
            }
        )

        def parse_key_usage(val):
            return [SigCertKeyUsage.read_from_sv_string(ku) for ku in val]

        key_usage = get_and_apply(pdf_dict, '/KeyUsage', parse_key_usage)

        url = pdf_dict.get('/URL')
        url_type = pdf_dict.get('/URLType')
        kwargs = {
            'flags': flags,
            'subjects': subjects or None,
            'subject_dn': subject_dns or None,
            'issuers': issuers or None,
            'info_url': url,
            'key_usage': key_usage,
        }
        if url is not None and url_type is not None:
            kwargs['url_type'] = url_type
        return cls(**kwargs)

    def as_pdf_object(self):
        """
        Render this :class:`.SigCertConstraints` object to a PDF dictionary.

        :return:
            A :class:`~.generic.DictionaryObject`.
        """

        result = generic.DictionaryObject(
            {
                pdf_name('/Type'): pdf_name('/SVCert'),
                pdf_name('/Ff'): generic.NumberObject(self.flags.value),
            }
        )
        if self.subjects is not None:
            result[pdf_name('/Subject')] = generic.ArrayObject(
                generic.ByteStringObject(cert.dump()) for cert in self.subjects
            )
        if self.subject_dn:
            # FIXME Adobe Reader seems to ignore this for some reason.
            #  Should try to figure out what I'm doing wrong
            result[pdf_name('/SubjectDN')] = generic.ArrayObject(
                [
                    generic.DictionaryObject(
                        {
                            pdf_name('/' + key): pdf_string(value)
                            for key, value in x509_name_keyval_pairs(
                                self.subject_dn, abbreviate_oids=True
                            )
                        }
                    )
                ]
            )
        if self.issuers is not None:
            result[pdf_name('/Issuer')] = generic.ArrayObject(
                generic.ByteStringObject(cert.dump()) for cert in self.issuers
            )
        if self.info_url is not None:
            result[pdf_name('/URL')] = pdf_string(self.info_url)
            result[pdf_name('/URLType')] = self.url_type

        if self.key_usage is not None:
            result[pdf_name('/KeyUsage')] = generic.ArrayObject(
                pdf_string(ku.encode_to_sv_string()) for ku in self.key_usage
            )

        return result

    def satisfied_by(
        self,
        signer: x509.Certificate,
        validation_path: Optional[ValidationPath],
    ):
        """
        Evaluate whether a signing certificate satisfies the required
        constraints of this :class:`.SigCertConstraints` object.

        :param signer:
            The candidate signer's certificate.
        :param validation_path:
            Validation path of the signer's certificate.
        :raises UnacceptableSignerError:
            Raised if the conditions are not met.
        """
        # this function assumes that key usage & trust checks have
        #  passed already.
        flags = self.flags
        if flags & SigCertConstraintFlags.UNSUPPORTED:
            raise NotImplementedError(
                "Certificate constraint flags include mandatory constraints "
                "that are not supported."
            )
        if (
            flags & SigCertConstraintFlags.SUBJECT
        ) and self.subjects is not None:
            # Explicit whitelist of approved signer certificates
            # compare using issuer_serial
            acceptable = (s.issuer_serial for s in self.subjects)
            if signer.issuer_serial not in acceptable:
                raise UnacceptableSignerError(
                    "Signer certificate not on SVCert whitelist."
                )
        if (flags & SigCertConstraintFlags.ISSUER) and self.issuers is not None:
            if validation_path is None:
                raise UnacceptableSignerError("Validation path not provided.")
            # Here, we need to match any issuer in the chain of trust to
            #  any of the issuers on the approved list.

            # To do so, we collect all issuer_serial identifiers in the chain
            # for all certificates except the last one (i.e. the current signer)
            path_iss_serials = {
                authority.certificate.issuer_serial
                for authority in validation_path.iter_authorities()
                if isinstance(authority, AuthorityWithCert)
            }
            for issuer in self.issuers:
                if issuer.issuer_serial in path_iss_serials:
                    break
            else:
                # raise error if the loop runs to completion
                raise UnacceptableSignerError(
                    "Signer certificate cannot be traced back to approved "
                    "issuer."
                )
        if (flags & SigCertConstraintFlags.SUBJECT_DN) and self.subject_dn:
            # I'm not entirely sure whether my reading of the standard is
            #  is correct, but I believe that this is the intention:
            # A DistinguishedName object is a sequence of
            #  relative distinguished names (RDNs). The contents of the
            #  /SubjectDN specify a list of constraints that might apply to each
            #  of these RDNs. I believe the requirement is that each of the
            #  SubjectDN entries must match one of these RDNs.

            requirement_list = list(x509_name_keyval_pairs(self.subject_dn))
            subject_name = list(x509_name_keyval_pairs(signer.subject))
            if not all(attr in subject_name for attr in requirement_list):
                raise UnacceptableSignerError(
                    "Subject does not have some of the following required "
                    "attributes: " + self.subject_dn.human_friendly
                )
        if (
            flags & SigCertConstraintFlags.KEY_USAGE
        ) and self.key_usage is not None:
            from .validation.settings import KeyUsageConstraints

            for ku in self.key_usage:
                try:
                    KeyUsageConstraints(
                        key_usage=ku.must_have_set(),
                        key_usage_forbidden=ku.forbidden_set(),
                        # This is the way ISO 32k does things
                        match_all_key_usages=True,
                    ).validate(signer)
                    break
                except InvalidCertificateError:
                    continue
            else:
                raise UnacceptableSignerError(
                    "The signer satisfies none of the key usage "
                    "extension profiles specified in the seed value dictionary."
                )


@unique
class SigSeedSubFilter(Enum):
    """
    Enum declaring all supported ``/SubFilter`` values.
    """

    ADOBE_PKCS7_DETACHED = pdf_name("/adbe.pkcs7.detached")
    PADES = pdf_name("/ETSI.CAdES.detached")
    ETSI_RFC3161 = pdf_name("/ETSI.RFC3161")


@unique
class SigAuthType(Enum):
    """
    Enum declaring all supported ``/Prop_AuthType`` values.
    """

    PIN = pdf_string("PIN")
    PASSWORD = pdf_string("Password")
    FINGERPRINT = pdf_string("Fingerprint")


@unique
class SeedValueDictVersion(OrderedEnum):
    """
    Specify the minimal compliance level for a seed value dictionary processor.
    """

    PDF_1_5 = 1
    """
    Require the reader to understand all keys defined in PDF 1.5.
    """

    PDF_1_7 = 2
    """
    Require the reader to understand all keys defined in PDF 1.7.
    """

    PDF_2_0 = 3
    """
    Require the reader to understand all keys defined in PDF 2.0.
    """


@unique
class SeedLockDocument(Enum):
    """
    Provides a recommendation to the signer as to whether the document should
    be locked after signing.
    The corresponding flag in :attr:`.SigSeedValueSpec.flags` determines whether
    this constraint is a required constraint.
    """

    LOCK = pdf_name('/true')
    """
    Lock the document after signing.
    """

    DO_NOT_LOCK = pdf_name('/false')
    """
    Lock the document after signing.
    """

    SIGNER_DISCRETION = pdf_name('/auto')
    """
    Leave the decision up to the signer.
    
    .. note::
        This is functionally equivalent to not specifying any value.
    """


@dataclass(frozen=True)
class SigSeedValueSpec:
    """
    Python representation of a PDF seed value dictionary.
    """

    flags: SigSeedValFlags = SigSeedValFlags(0)
    """
    Enforcement flags. By default, all entries are optional.
    """

    reasons: Optional[List[str]] = None
    """
    Acceptable reasons for signing.
    """

    timestamp_server_url: Optional[str] = None
    """
    RFC 3161 timestamp server endpoint suggestion.
    """

    timestamp_required: bool = False
    """
    Flags whether a timestamp is required.
    This flag is only meaningful if :attr:`timestamp_server_url` is specified.
    """

    cert: Optional[SigCertConstraints] = None
    """
    Constraints on the signer's certificate.
    """

    subfilters: Optional[List[SigSeedSubFilter]] = None
    """
    Acceptable ``/SubFilter`` values.
    """

    digest_methods: Optional[List[str]] = None
    """
    Acceptable digest methods.
    """

    add_rev_info: Optional[bool] = None
    """
    Indicates whether revocation information should be embedded.
    
    .. warning::
        This flag exclusively refers to the Adobe-style revocation information
        embedded within the CMS object that is written to the signature field.
        PAdES-style revocation information that is saved to the document
        security store (DSS) does *not* satisfy the requirement.
        Additionally, the standard mandates that ``/SubFilter`` be equal to
        ``/adbe.pkcs7.detached`` if this flag is ``True``.
    """

    seed_signature_type: Optional[SeedSignatureType] = None
    """
    Specifies the type of signature that should occupy a signature field;
    this represents the ``/MDP`` entry in the seed value dictionary.
    See :class:`.SeedSignatureType` for details.
    
    .. caution::
        Since a certification-type signature is by definition the first
        signature applied to a document, compliance with this requirement
        cannot be cryptographically enforced.
    """

    sv_dict_version: Union[SeedValueDictVersion, int, None] = None
    """
    Specifies the compliance level required of a seed value dictionary
    processor. If ``None``, pyHanko will compute an appropriate value.
    
    .. note::
        You may also specify this value directly as an integer.
        This covers potential future versions of the standard that pyHanko
        does not support out of the box.
    """

    legal_attestations: Optional[List[str]] = None
    """
    Specifies the possible legal attestations that a certification signature
    occupying this signature field can supply.
    The corresponding flag in :attr:`flags` indicates whether this is a
    mandatory constraint.
    
    .. caution::
        Since :attr:`legal_attestations` is only relevant for certification
        signatures, compliance with this requirement cannot be reliably 
        enforced.
        Regardless, since pyHanko's validator is also unaware of legal
        attestation settings, it will refuse to validate signatures
        where this seed value constitutes a mandatory constraint.
        
        Additionally, since pyHanko does not support legal attestation
        specifications at all, it vacuously satisfies the requirements of this
        entry no matter what, and will therefore ignore it when signing.
    """

    lock_document: Optional[SeedLockDocument] = None
    """
    Tell the signer whether or not the document should be locked after signing
    this field; see :class:`.SeedLockDocument` for details.
    
    The corresponding flag in :attr:`flags` indicates whether this constraint
    is mandatory.
    """

    # TODO handle this value by reading named appearances from the user's
    #  settings

    appearance: Optional[str] = None
    """
    Specify a named appearance to use when generating the signature.
    The corresponding flag in :attr:`flags` indicates whether this constraint
    is mandatory.
    
    .. caution::
        There is no standard registry of named appearances, so these constraints
        are not portable, and cannot be validated.
        
        PyHanko currently does not define any named appearances.        
    """

    def as_pdf_object(self):
        """
        Render this :class:`.SigSeedValueSpec` object to a PDF dictionary.

        :return:
            A :class:`~.generic.DictionaryObject`.
        """
        min_version = SeedValueDictVersion.PDF_1_5
        result = generic.DictionaryObject(
            {
                pdf_name('/Type'): pdf_name('/SV'),
                pdf_name('/Ff'): generic.NumberObject(self.flags.value),
            }
        )

        if self.subfilters is not None:
            result[pdf_name('/SubFilter')] = generic.ArrayObject(
                sf.value for sf in self.subfilters
            )
        if self.add_rev_info is not None:
            min_version = SeedValueDictVersion.PDF_1_7
            result[pdf_name('/AddRevInfo')] = generic.BooleanObject(
                self.add_rev_info
            )
        if self.digest_methods is not None:
            min_version = SeedValueDictVersion.PDF_1_7
            result[pdf_name('/DigestMethod')] = generic.ArrayObject(
                map(pdf_string, self.digest_methods)
            )
        if self.reasons is not None:
            result[pdf_name('/Reasons')] = generic.ArrayObject(
                pdf_string(reason) for reason in self.reasons
            )
        if self.timestamp_server_url is not None:
            min_version = SeedValueDictVersion.PDF_1_7
            result[pdf_name('/TimeStamp')] = generic.DictionaryObject(
                {
                    pdf_name('/URL'): pdf_string(self.timestamp_server_url),
                    pdf_name('/Ff'): generic.NumberObject(
                        1 if self.timestamp_required else 0
                    ),
                }
            )
        if self.cert is not None:
            result[pdf_name('/Cert')] = self.cert.as_pdf_object()
        if self.seed_signature_type is not None:
            mdp_perm = self.seed_signature_type.mdp_perm
            result[pdf_name('/MDP')] = generic.DictionaryObject(
                {
                    pdf_name('/P'): generic.NumberObject(
                        mdp_perm.value if mdp_perm is not None else 0
                    )
                }
            )
        if self.legal_attestations is not None:
            result[pdf_name('/LegalAttestation')] = generic.ArrayObject(
                pdf_string(att) for att in self.legal_attestations
            )
        if self.lock_document is not None:
            min_version = SeedValueDictVersion.PDF_2_0
            result[pdf_name('/LockDocument')] = self.lock_document.value
        if self.appearance is not None:
            result[pdf_name('/AppearanceFilter')] = pdf_string(self.appearance)

        specified_version = self.sv_dict_version
        if specified_version is not None:
            result[pdf_name('/V')] = generic.NumberObject(
                specified_version.value
                if isinstance(specified_version, SeedValueDictVersion)
                else specified_version
            )
        else:
            result[pdf_name('/V')] = generic.NumberObject(min_version.value)
        return result

    @classmethod
    def from_pdf_object(cls, pdf_dict):
        """
        Read from a seed value dictionary.

        :param pdf_dict:
            A :class:`~.generic.DictionaryObject`.
        :return:
            A :class:`.SigSeedValueSpec` object.
        """
        if isinstance(pdf_dict, generic.IndirectObject):
            pdf_dict = pdf_dict.get_object()
        try:
            if pdf_dict['/Type'] != '/SV':  # pragma: nocover
                raise PdfReadError('Object /Type entry is not /SV')
        except KeyError:  # pragma: nocover
            pass

        flags = SigSeedValFlags(pdf_dict.get('/Ff', 0))
        try:
            sig_filter = pdf_dict['/Filter']
            if (flags & SigSeedValFlags.FILTER) and (
                sig_filter != '/Adobe.PPKLite'
            ):
                raise SigningError(
                    "Signature handler '%s' is not available, only the "
                    "default /Adobe.PPKLite is supported." % sig_filter
                )
        except KeyError:
            pass

        try:
            min_version = pdf_dict['/V']
            supported = SeedValueDictVersion.PDF_2_0.value
            if flags & SigSeedValFlags.V and min_version > supported:
                raise SigningError(
                    "Seed value dictionary version %s not supported."
                    % min_version
                )
            min_version = SeedValueDictVersion(min_version)
        except KeyError:
            min_version = None

        try:
            add_rev_info = bool(pdf_dict['/AddRevInfo'])
        except KeyError:
            add_rev_info = None

        subfilter_reqs = pdf_dict.get('/SubFilter', None)
        subfilters = None
        if subfilter_reqs is not None:

            def _subfilters():
                for s in subfilter_reqs:
                    try:
                        yield SigSeedSubFilter(s)
                    except ValueError:
                        pass

            subfilters = list(_subfilters())

        try:
            digest_methods = [s.lower() for s in pdf_dict['/DigestMethod']]
        except KeyError:
            digest_methods = None

        reasons = get_and_apply(pdf_dict, '/Reasons', list)
        legal_attestations = get_and_apply(pdf_dict, '/LegalAttestation', list)

        def read_mdp_dict(mdp):
            try:
                val = mdp['/P']
                return SeedSignatureType(None if val == 0 else MDPPerm(val))
            except (KeyError, TypeError, ValueError):
                raise SigningError(
                    f"/MDP entry {mdp} in seed value dictionary is not "
                    "correctly formatted."
                )

        signature_type = get_and_apply(pdf_dict, '/MDP', read_mdp_dict)

        def read_lock_document(val):
            try:
                return SeedLockDocument(val)
            except ValueError:
                raise SigningError(f"/LockDocument entry '{val}' is invalid.")

        lock_document = get_and_apply(
            pdf_dict, '/LockDocument', read_lock_document
        )
        appearance_filter = pdf_dict.get('/AppearanceFilter', None)
        timestamp_dict = pdf_dict.get('/TimeStamp', {})
        timestamp_server_url = timestamp_dict.get('/URL', None)
        timestamp_required = bool(timestamp_dict.get('/Ff', 0))
        cert_constraints = pdf_dict.get('/Cert', None)
        if cert_constraints is not None:
            cert_constraints = SigCertConstraints.from_pdf_object(
                cert_constraints
            )
        return cls(
            flags=flags,
            reasons=reasons,
            timestamp_server_url=timestamp_server_url,
            cert=cert_constraints,
            subfilters=subfilters,
            digest_methods=digest_methods,
            add_rev_info=add_rev_info,
            timestamp_required=timestamp_required,
            legal_attestations=legal_attestations,
            seed_signature_type=signature_type,
            sv_dict_version=min_version,
            lock_document=lock_document,
            appearance=appearance_filter,
        )

    def build_timestamper(self):
        """
        Return a timestamper object based on the :attr:`timestamp_server_url`
        attribute of this :class:`.SigSeedValueSpec` object.

        :return:
            A :class:`~.pyhanko.sign.timestamps.HTTPTimeStamper`.
        """
        from pyhanko.sign.timestamps import HTTPTimeStamper

        if self.timestamp_server_url:
            return HTTPTimeStamper(self.timestamp_server_url)


class FieldMDPAction(Enum):
    """
    Marker for the scope of a ``/FieldMDP`` policy.
    """

    ALL = pdf_name('/All')
    """
    The policy locks all form fields.
    """

    INCLUDE = pdf_name('/Include')
    """
    The policy locks all fields in the list (see :attr:`.FieldMDPSpec.fields`).
    """

    EXCLUDE = pdf_name('/Exclude')
    """
    The policy locks all fields except those specified in the list
    (see :attr:`.FieldMDPSpec.fields`).
    """


@dataclass(frozen=True)
class FieldMDPSpec:
    """``/FieldMDP`` policy description.

    This class models both field lock dictionaries and ``/FieldMDP``
    transformation parameters.
    """

    action: FieldMDPAction
    """
    Indicates the scope of the policy.
    """

    fields: Optional[List[str]] = None
    """
    Indicates the fields subject to the policy,
    unless :attr:`action` is :attr:`.FieldMDPAction.ALL`.
    """

    def as_pdf_object(self) -> generic.DictionaryObject:
        """
        Render this ``/FieldMDP`` policy description as a PDF dictionary.

        :return:
            A :class:`~.generic.DictionaryObject`.
        """
        result = generic.DictionaryObject(
            {
                pdf_name('/Action'): self.action.value,
            }
        )
        if self.action != FieldMDPAction.ALL:
            result['/Fields'] = generic.ArrayObject(
                map(pdf_string, self.fields or ())
            )
        return result

    def as_transform_params(self) -> generic.DictionaryObject:
        """
        Render this ``/FieldMDP`` policy description as a PDF dictionary,
        ready for inclusion into the ``/TransformParams`` entry of a
        ``/FieldMDP`` dictionary associated with a signature object.

        :return:
            A :class:`~.generic.DictionaryObject`.
        """

        result = self.as_pdf_object()
        result['/Type'] = pdf_name('/TransformParams')
        result['/V'] = pdf_name('/1.2')
        return result

    def as_sig_field_lock(self) -> generic.DictionaryObject:
        """
        Render this ``/FieldMDP`` policy description as a PDF dictionary,
        ready for inclusion into the ``/Lock`` dictionary of a signature field.

        :return:
            A :class:`~.generic.DictionaryObject`.
        """

        result = self.as_pdf_object()
        result['/Type'] = pdf_name('/SigFieldLock')
        return result

    @classmethod
    def from_pdf_object(cls, pdf_dict) -> 'FieldMDPSpec':
        """
        Read a PDF dictionary into a :class:`.FieldMDPSpec` object.

        :param pdf_dict:
            A :class:`~.generic.DictionaryObject`.
        :return:
            A :class:`.FieldMDPSpec` object.
        """
        try:
            action = FieldMDPAction(pdf_dict['/Action'])
        except KeyError:  # pragma: nocover
            raise PdfReadError("/Action is required.")

        if action != FieldMDPAction.ALL:
            try:
                fields = pdf_dict['/Fields']
            except KeyError:  # pragma: nocover
                raise PdfReadError(
                    "/Fields is required when /Action is not /All"
                )
        else:
            fields = None
        return cls(action=action, fields=fields)

    def is_locked(self, field_name: str) -> bool:
        """
        Adjudicate whether a field should be locked by the policy described by
        this :class:`.FieldMDPSpec` object.

        :param field_name:
            The name of a form field.
        :return:
            ``True`` if the field should be locked, ``False`` otherwise.
        """
        if self.action == FieldMDPAction.ALL:
            return True

        lock_result = self.action == FieldMDPAction.INCLUDE
        for scoped_field_name in self.fields or ():
            # treat non-terminal field in/exclusions as including the whole
            # tree beneath them
            if field_name.startswith(scoped_field_name):
                return lock_result
        return not lock_result


@dataclass(frozen=True)
class InvisSigSettings:
    """
    Invisible signature widget generation settings.

    These settings exist because there is no real way of including an untagged
    invisible signature in a document that complies with the requirements
    of both PDF/A-2 (or -3) and PDF/UA-1.

    Compatibility with PDF/A (the default) requires the print flag to be set.
    Compatibility with PDF/UA requires the hidden flag to be set (which is
    banned in PDF/A) or the box to be outside the crop box.
    """

    set_print_flag: bool = True
    """
    Set the print flag. Required in PDF/A.
    """

    set_hidden_flag: bool = False
    """
    Set the hidden flag. Required in PDF/UA.
    """

    box_out_of_bounds: bool = False
    """
    Put the box out of bounds (technically, this just makes the box
    zero-sized with large negative coordinates).

    This is a hack to get around the fact that PDF/UA requires the hidden
    flag to be set on all in-bounds untagged annotations, and some validators
    consider [0, 0, 0, 0] to be an in-bounds rectangle if (0, 0) is a point
    that falls within the crop box.
    """


@dataclass(frozen=True)
class VisibleSigSettings:
    """
    .. versionadded:: 0.14.0

    Additional flags used when setting up visible signature widgets.
    """

    rotate_with_page: bool = True
    """
    Allow the signature widget to rotate with the page if rotation is applied
    (e.g. by way of the page's ``/Rotate`` entry). Default is ``True``.

    .. note::
        If ``False``, this will cause the ``NoRotate`` flag to be set.
    """

    scale_with_page_zoom: bool = True
    """
    Allow the signature widget to scale with the page's zoom level.
    Default is ``True``.

    .. note::
        If ``False``, this will cause the ``NoZoom`` flag to be set.
    """

    print_signature: bool = True
    """
    Render the signature when the document is printed. Default ``True``.
    """


# TODO deal with fully qualified field names for the signature field


@dataclass(frozen=True)
class SigFieldSpec:
    """Description of a signature field to be created."""

    sig_field_name: str
    """
    Name of the signature field.
    """

    on_page: int = 0
    """
    Index of the page on which the signature field should be included (starting
    at `0`).
    A negative number counts pages from the back of the document,
    with index ``-1`` referring to the last page.
    
    .. note::
        This is essentially only relevant for visible signature fields, i.e.
        those that have a widget associated with them.
    """

    box: Optional[Tuple[int, int, int, int]] = None
    """
    Bounding box of the signature field, if applicable.

    Typically specified in ``ll_x``, ``ll_y``, ``ur_x``, ``ur_y`` format,
    where ``ll_*`` refers to the lower left and ``ur_*`` to the upper right
    corner.
    """

    seed_value_dict: Optional[SigSeedValueSpec] = None
    """
    Specification for the seed value dictionary, if applicable.
    """

    field_mdp_spec: Optional[FieldMDPSpec] = None
    """
    Specification for the field lock dictionary, if applicable.
    """

    doc_mdp_update_value: Optional[MDPPerm] = None
    """
    Value to use for the document modification policy associated with the
    signature in this field.
    
    This value will be embedded into the field lock dictionary if specified, and    
    is meaningless if :attr:`field_mdp_spec` is not specified.
    
    .. warning::
        DocMDP entries for approval signatures are a PDF 2.0 feature.
        Older PDF software will likely ignore this part of the field lock
        dictionary.
    """
    # TODO add a reference to the docs on certification once those are written.

    combine_annotation: bool = True
    """
    Flag controlling whether the field should be combined with its
    annotation dictionary; ``True`` by default.
    """

    empty_field_appearance: bool = False
    """
    Generate a neutral appearance stream for empty, visible signature fields.
    If ``False``, an empty appearance stream will be put in.
    
    .. note::
        We use an empty appearance stream to satisfy the appearance requirements
        for widget annotations in ISO 32000-2. However, even when a nontrivial
        appearance stream is present on an empty signature field, many viewers 
        will not use it to render the appearance of the empty field on-screen.

        Instead, these viewers typically substitute their own native widget.
    """

    invis_sig_settings: InvisSigSettings = InvisSigSettings()
    """
    Advanced settings to control invisible signature field generation.
    """

    readable_field_name: Optional[str] = None
    """
    Human-readable field name (``/TU`` entry).
    
    .. note::
        This value is commonly rendered as a tooltip in viewers, but also
        serves an accessibility purpose.
    """

    visible_sig_settings: VisibleSigSettings = VisibleSigSettings()
    """
    Advanced settings to control the generation of visible signature fields.
    """

    def format_lock_dictionary(self) -> Optional[generic.DictionaryObject]:
        if self.field_mdp_spec is None:
            return None
        result = self.field_mdp_spec.as_sig_field_lock()
        # this requires PDF 2.0 in principle, but meh, noncompliant
        # readers will ignore it anyway
        if self.doc_mdp_update_value is not None:
            result['/P'] = generic.NumberObject(self.doc_mdp_update_value.value)
        return result


def _insert_or_get_field_at(
    writer: BasePdfFileWriter,
    fields,
    path,
    parent_ref=None,
    modified=False,
    field_obj=None,
):
    current_partial, tail = path[0], path[1:]

    for field_ref in fields:
        assert isinstance(field_ref, generic.IndirectObject)
        field = field_ref.get_object()
        if field.get('/T', None) == current_partial:
            break
    else:
        # have to insert a new element into the fields array
        if field_obj is not None and not tail:
            field = field_obj
        else:
            # create a generic field
            field = generic.DictionaryObject()
        field['/T'] = pdf_string(current_partial)
        if parent_ref is not None:
            field['/Parent'] = parent_ref
        field_ref = writer.add_object(field)
        fields.append(field_ref)
        writer.update_container(fields)
        modified = True

    if not tail:
        return modified, field_ref
    # check for /Kids, and create it if necessary
    try:
        kids = field['/Kids']
    except KeyError:
        kids = field['/Kids'] = generic.ArrayObject()
        writer.update_container(field)
        modified = True

    # recurse in to /Kids array
    return _insert_or_get_field_at(
        writer,
        kids,
        tail,
        parent_ref=field_ref,
        modified=modified,
        field_obj=field_obj,
    )


def ensure_sig_flags(writer: BasePdfFileWriter, lock_sig_flags: bool = True):
    """
    Ensure the SigFlags setting is present in the AcroForm dictionary.

    :param writer:
        A PDF writer.
    :param lock_sig_flags:
        Whether to flag the document as append-only.
    """
    # make sure /SigFlags is present. If not, create it
    # 3 = use append-only mode

    form = writer.root['/AcroForm']

    if lock_sig_flags:
        orig_sig_flags = form.get('/SigFlags', None)
        form['/SigFlags'] = generic.NumberObject(3)
        if orig_sig_flags != 3:
            writer.update_container(form)
    else:
        form.setdefault(pdf_name('/SigFlags'), generic.NumberObject(1))


def prepare_sig_field(
    sig_field_name,
    root,
    update_writer: BasePdfFileWriter,
    existing_fields_only=False,
    **kwargs,
):
    """
    Returns a tuple of a boolean and a reference to a signature field.
    The boolean is ``True`` if the field was created, and ``False`` otherwise.

    .. danger::
        This function is internal API.
    """

    try:
        form = root['/AcroForm']

        try:
            fields = form['/Fields']
        except KeyError:
            fields = form['/Fields'] = generic.ArrayObject()

        candidates = enumerate_sig_fields_in(
            fields, with_name=sig_field_name, refs_seen=set()
        )
        sig_field_ref = None
        try:
            field_name, value, sig_field_ref = next(candidates)
            if value is not None:
                raise SigningError(
                    'Signature field with name %s appears to be filled already.'
                    % sig_field_name
                )
        except StopIteration:
            if existing_fields_only:
                raise SigningError(
                    'No empty signature field with name %s found.'
                    % sig_field_name
                )
        form_created = False
    except KeyError:
        # we have to create the form
        if existing_fields_only:
            raise SigningError('This file does not contain a form.')
        # no AcroForm present, so create one
        form = generic.DictionaryObject()
        root[pdf_name('/AcroForm')] = update_writer.add_object(form)
        fields = generic.ArrayObject()
        form[pdf_name('/Fields')] = fields
        # now we need to mark the root as updated
        update_writer.update_root()
        form_created = True
        sig_field_ref = None

    if sig_field_ref is not None:
        return False, sig_field_ref

    # no signature field exists, so create one
    # default: grab a reference to the first page
    page_ref = update_writer.find_page_for_modification(0)[0]
    sig_form_kwargs = {'include_on_page': page_ref}
    sig_form_kwargs.update(**kwargs)
    sig_field = SignatureFormField(sig_field_name, **sig_form_kwargs)
    created, sig_field_ref = _insert_or_get_field_at(
        update_writer,
        fields,
        path=sig_field_name.split('.'),
        field_obj=sig_field,
    )
    sig_field.register_widget_annotation(update_writer, sig_field_ref)

    # if a field was added to an existing form, register an extra update
    if not form_created:
        update_writer.update_container(fields)
    return True, sig_field_ref


def get_sig_field_annot(
    sig_field: generic.DictionaryObject,
) -> generic.DictionaryObject:
    """
    Internal function to get the annotation of a signature field.

    :param sig_field:
        A signature field dictionary.
    :return:
        The dictionary of the corresponding annotation.
    """
    try:
        (sig_annot,) = sig_field['/Kids']
        sig_annot = sig_annot.get_object()
    except (ValueError, TypeError):
        raise SigningError(
            "Failed to access signature field's annotation. "
            "Signature field must have exactly one child annotation, "
            "or it must be combined with its annotation."
        )
    except KeyError:
        sig_annot = sig_field
    return sig_annot


def annot_width_height(
    annot_dict: generic.DictionaryObject,
) -> Tuple[float, float]:
    """
    Internal function to compute the width and height of an annotation.

    :param annot_dict:
        Annotation dictionary.
    :return:
        a (width, height) tuple
    """
    try:
        x1, y1, x2, y2 = annot_dict['/Rect']
    except KeyError:
        return 0, 0
    w = abs(x1 - x2)
    h = abs(y1 - y2)
    return w, h


def enumerate_sig_fields(
    handler: PdfHandler,
    filled_status: Optional[bool] = None,
    with_name: Optional[str] = None,
):
    """
    Enumerate signature fields.

    :param handler:
        The :class:`~.rw_common.PdfHandler` to operate on.
    :param filled_status:
        Optional boolean. If ``True`` (resp. ``False``) then all filled
        (resp. empty) fields are returned. If left ``None`` (the default), then
        all fields are returned.
    :param with_name:
        If not ``None``, only look for fields with the specified name.
    :return:
        A generator producing signature fields.
    """

    try:
        fields = handler.root['/AcroForm']['/Fields']
    except KeyError:
        return

    yield from enumerate_sig_fields_in(
        fields,
        filled_status=filled_status,
        with_name=with_name,
        refs_seen=set(),
    )


def enumerate_sig_fields_in(
    field_list,
    filled_status=None,
    with_name=None,
    parent_name="",
    parents=None,
    *,
    refs_seen,
):
    if not isinstance(field_list, generic.ArrayObject):
        logger.warning(
            f"Values of type {type(field_list)} are not valid as field "
            f"lists, must be array objects -- skipping."
        )
        return

    parents = parents or ()
    for field_ref in field_list:
        if not isinstance(field_ref, generic.IndirectObject):
            logger.warning(
                "Entries in field list must be indirect references -- skipping."
            )
            continue
        if field_ref.reference in refs_seen:
            raise PdfReadError("Circular reference in form tree")

        field = field_ref.get_object()
        if not isinstance(field, generic.DictionaryObject):
            logger.warning(
                "Entries in field list must be dictionary objects, not "
                f"{type(field)} -- skipping."
            )
            continue
        # /T is the field name. If not specified, we're dealing with a bare
        # widget, so skip it. (these should never occur in /Fields, but hey)
        try:
            field_name = field['/T']
        except KeyError:
            continue
        fq_name = (
            field_name
            if not parent_name
            else ("%s.%s" % (parent_name, field_name))
        )
        explicitly_requested = with_name is not None and fq_name == with_name
        child_requested = explicitly_requested or (
            with_name is not None and with_name.startswith(fq_name)
        )
        # /FT is inheritable, so go up the chain
        current_path = (field,) + parents
        for parent_field in current_path:
            try:
                field_type = parent_field['/FT']
                break
            except KeyError:
                continue
        else:
            field_type = None

        if field_type == '/Sig':
            field_value = field.get('/V')
            # "cast" to a regular string object
            filled = field_value is not None
            status_check = filled_status is None or filled == filled_status
            name_check = with_name is None or explicitly_requested
            if status_check and name_check:
                yield fq_name, field_value, field_ref
        elif explicitly_requested:
            raise SigningError(
                'Field with name %s exists but is not a signature field'
                % fq_name
            )

        # if necessary, descend into the field hierarchy
        if with_name is None or (child_requested and not explicitly_requested):
            try:
                yield from enumerate_sig_fields_in(
                    field['/Kids'],
                    parent_name=fq_name,
                    parents=current_path,
                    with_name=with_name,
                    filled_status=filled_status,
                    refs_seen=refs_seen | {field_ref.reference},
                )
            except KeyError:
                continue


def append_signature_field(
    pdf_out: BasePdfFileWriter, sig_field_spec: SigFieldSpec
):
    """
    Append signature fields to a PDF file.

    :param pdf_out:
        Incremental writer to house the objects.
    :param sig_field_spec:
        A :class:`.SigFieldSpec` object describing the signature field
        to add.
    """
    root = pdf_out.root

    page_ref = pdf_out.find_page_for_modification(sig_field_spec.on_page)[0]
    field_created, sig_field_ref = prepare_sig_field(
        sig_field_spec.sig_field_name,
        root,
        update_writer=pdf_out,
        existing_fields_only=False,
        box=sig_field_spec.box,
        include_on_page=page_ref,
        combine_annotation=sig_field_spec.combine_annotation,
        invis_settings=sig_field_spec.invis_sig_settings,
        visible_settings=sig_field_spec.visible_sig_settings,
    )
    ensure_sig_flags(writer=pdf_out, lock_sig_flags=False)
    if not field_created:
        raise PdfWriteError(
            'Signature field with name %s already exists.'
            % sig_field_spec.sig_field_name
        )

    sig_field = sig_field_ref.get_object()
    apply_sig_field_spec_properties(
        pdf_out, sig_field=sig_field, sig_field_spec=sig_field_spec
    )

    if sig_field_spec.box is not None:
        llx, lly, urx, ury = sig_field_spec.box
        w = abs(urx - llx)
        h = abs(ury - lly)
        if w and h:
            sig_field[pdf_name('/AP')] = ap_dict = generic.DictionaryObject()
            if sig_field_spec.empty_field_appearance:
                # draw a simple rectangle
                appearance_cmds = [
                    b'q',
                    # background
                    b'q 0.95 0.95 0.95 rg 0 0 %g %g re f Q' % (w, h),
                    # border
                    b'0.5 w 0 0 %g %g re S' % (w, h),
                    b'Q',
                ]
                ap_stream = RawContent(
                    b' '.join(appearance_cmds),
                    box=BoxConstraints(width=w, height=h),
                ).as_form_xobject()
            else:
                ap_stream = RawContent(
                    b'', box=BoxConstraints(width=w, height=h)
                ).as_form_xobject()
            ap_dict[pdf_name('/N')] = pdf_out.add_object(ap_stream)


def apply_sig_field_spec_properties(
    pdf_out: BasePdfFileWriter,
    sig_field: generic.DictionaryObject,
    sig_field_spec: SigFieldSpec,
):
    """
    Internal function to apply field spec properties to a newly created field.
    """

    if sig_field_spec.readable_field_name is not None:
        sig_field[pdf_name('/TU')] = generic.TextStringObject(
            sig_field_spec.readable_field_name
        )
    if sig_field_spec.seed_value_dict is not None:
        # /SV must be an indirect reference as per the spec
        sv_ref = pdf_out.add_object(
            sig_field_spec.seed_value_dict.as_pdf_object()
        )
        sig_field[pdf_name('/SV')] = sv_ref

    lock = sig_field_spec.format_lock_dictionary()
    if lock is not None:
        sig_field[pdf_name('/Lock')] = pdf_out.add_object(lock)


class SignatureFormField(generic.DictionaryObject):
    def __init__(
        self,
        field_name,
        *,
        box=None,
        include_on_page=None,
        combine_annotation=True,
        invis_settings: InvisSigSettings = InvisSigSettings(),
        visible_settings: VisibleSigSettings = VisibleSigSettings(),
        annot_flags=None,
    ):
        if box is not None:
            rect = [generic.FloatObject(rd(x)) for x in box]
            invisible = not (abs(box[0] - box[2]) and abs(box[1] - box[3]))
        else:
            coord = -9999 if invis_settings.box_out_of_bounds else 0
            rect = [generic.FloatObject(coord)] * 4
            invisible = True

        super().__init__(
            {
                # Signature field properties
                pdf_name('/FT'): pdf_name('/Sig'),
                pdf_name('/T'): pdf_string(field_name),
            }
        )

        self.combine_annotation = combine_annotation
        annot_dict: generic.DictionaryObject
        if combine_annotation:
            annot_dict = self
        else:
            annot_dict = generic.DictionaryObject()

        # Annotation properties: bare minimum
        annot_dict['/Type'] = pdf_name('/Annot')
        annot_dict['/Subtype'] = pdf_name('/Widget')

        if annot_flags is None:
            # this sets the "lock" bit
            annot_flags = 0b10000000
            if invisible:
                if invis_settings.set_hidden_flag:
                    annot_flags |= 0b10
                if invis_settings.set_print_flag:
                    annot_flags |= 0b100
            else:
                if visible_settings.print_signature:
                    annot_flags |= 0b100
                if not visible_settings.scale_with_page_zoom:
                    annot_flags |= 0b1000
                if not visible_settings.rotate_with_page:
                    annot_flags |= 0b10000

        annot_dict['/F'] = generic.NumberObject(annot_flags)
        annot_dict['/Rect'] = generic.ArrayObject(rect)

        self.page_ref = include_on_page
        if include_on_page is not None:
            annot_dict['/P'] = include_on_page

        self.annot_dict = annot_dict

    def register_widget_annotation(
        self, writer: BasePdfFileWriter, sig_field_ref
    ):
        annot_dict = self.annot_dict
        if not self.combine_annotation:
            annot_ref = writer.add_object(annot_dict)
            self['/Kids'] = generic.ArrayObject([annot_ref])
        else:
            annot_ref = sig_field_ref
        writer.register_annotation(self.page_ref, annot_ref)
