import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import unique
from typing import (
    Any,
    ClassVar,
    Collection,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from asn1crypto import cms, core, crl, keys, x509
from pyhanko_certvalidator.errors import (
    PathBuildingError,
    PathValidationError,
    ValidationError,
)
from pyhanko_certvalidator.path import ValidationPath
from pyhanko_certvalidator.validate import ACValidationResult

from ...pdf_utils.misc import OrderedEnum
from ..ades.report import AdESFailure, AdESSubIndic
from ..diff_analysis import (
    DiffResult,
    ModificationLevel,
    SuspiciousModification,
)
from .errors import SignatureValidationError, SigSeedValueValidationError
from .settings import KeyUsageConstraints

__all__ = [
    'SignatureStatus',
    'TimestampSignatureStatus',
    'X509AttributeInfo',
    'CertifiedAttributeInfo',
    'ClaimedAttributes',
    'CertifiedAttributes',
    'CAdESSignerAttributeAssertions',
    'StandardCMSSignatureStatus',
    'SignatureCoverageLevel',
    'ModificationInfo',
    'PdfSignatureStatus',
    'DocumentTimestampStatus',
    'RevocationDetails',
    'SignerAttributeStatus',
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RevocationDetails:
    """
    Contains details about a certificate revocation related to a signature.
    """

    ca_revoked: bool
    """
    If ``False``, the revoked certificate is the signer's. If ``True``, there's
    a revoked CA certificate higher up the chain.
    """

    revocation_date: datetime
    """
    The date and time of revocation.
    """

    revocation_reason: crl.CRLReason
    """
    The reason why the certificate was revoked.
    """


@dataclass(frozen=True)
class SignatureStatus:
    """
    Class describing the validity of a (general) CMS signature.
    """

    intact: bool
    """
    Reports whether the signature is *intact*, i.e. whether the hash of the
    message content (which may or may not be embedded inside the CMS object
    itself) matches the hash value that was signed.

    If there are no signed attributes, this is equal to :attr:`valid`.
    """

    valid: bool
    """
    Reports whether the signature is *valid*, i.e. whether the signature
    in the CMS object itself (usually computed over a hash of the signed
    attributes) is cryptographically valid.
    """

    trust_problem_indic: Optional[AdESSubIndic]
    """
    If not ``None``, provides the AdES subindication indication what went
    wrong when validating the signer's certificate.
    """

    signing_cert: x509.Certificate
    """
    Contains the certificate of the signer, as embedded in the CMS object.
    """

    pkcs7_signature_mechanism: str
    """
    CMS signature mechanism used.
    """

    # TODO: also here some ambiguity analysis is in order
    md_algorithm: str
    """
    Message digest algorithm used.
    """

    validation_path: Optional[ValidationPath]
    """
    Validation path providing a valid chain of trust from the signer's
    certificate to a trusted root certificate.
    """

    revocation_details: Optional[RevocationDetails]
    """
    Details on why and when the signer's certificate (or another certificate
    in the chain) was revoked.
    """

    error_time_horizon: Optional[datetime]
    """
    Informational timestamp indicating a point in time where the validation
    behaviour potentially changed (e.g. expiration, revocation, etc.).

    The presence of this value by itself should not be taken as an assertion
    that validation would have succeeded if executed before that point in time.
    """

    # XXX frozenset makes more sense here, but asn1crypto doesn't allow that
    #  (probably legacy behaviour)
    key_usage: ClassVar[Set[str]] = {'non_repudiation'}
    """
    Class property indicating which key usages are accepted on the signer's
    certificate. The default is ``non_repudiation`` only.
    """

    extd_key_usage: ClassVar[Optional[Set[str]]] = None
    """
    Class property indicating which extended key usage key purposes are accepted
    to be present on the signer's certificate.

    See :attr:`.KeyUsageConstraints.extd_key_usage`.
    """

    validation_time: Optional[datetime]
    """
    Reference time for validation purposes.
    """

    def summary_fields(self):
        if self.trusted:
            cert_status = 'TRUSTED'
        elif self.revoked:
            cert_status = 'REVOKED'
        else:
            cert_status = 'UNTRUSTED'
        yield cert_status

    @property
    def revoked(self) -> bool:
        """
        Reports whether the signer's certificate has been revoked or not.
        If this field is ``True``, then obviously :attr:`trusted` will be
        ``False``.
        """
        return self.revocation_details is not None

    @property
    def trusted(self) -> bool:
        """
        Reports whether the signer's certificate is trusted w.r.t. the currently
        relevant validation context and key usage requirements.
        """
        return (
            self.valid
            and self.intact
            and self.trust_problem_indic is None
            and self.validation_path is not None
        )

    # TODO explain in more detail.
    def summary(self, delimiter=',') -> str:
        """
        Provide a textual but machine-parsable summary of the validity.
        """
        if self.intact and self.valid:
            return 'INTACT:' + delimiter.join(self.summary_fields())
        else:
            return 'INVALID'

    @classmethod
    def default_usage_constraints(
        cls, key_usage_settings: Optional[KeyUsageConstraints] = None
    ) -> KeyUsageConstraints:
        key_usage_settings = key_usage_settings or KeyUsageConstraints()
        key_usage_settings = KeyUsageConstraints(
            key_usage=(
                cls.key_usage
                if key_usage_settings.key_usage is None
                else key_usage_settings.key_usage
            ),
            extd_key_usage=(
                cls.extd_key_usage
                if key_usage_settings.extd_key_usage is None
                else key_usage_settings.extd_key_usage
            ),
        )
        return key_usage_settings

    @property
    def _trust_anchor(self) -> str:
        if self.validation_path is not None:
            trust_anchor: x509.Certificate = self.validation_path[0]
            return trust_anchor.subject.human_friendly
        else:
            return "No path to trust anchor found."


@dataclass(frozen=True)
class TimestampSignatureStatus(SignatureStatus):
    """
    Signature status class used when validating timestamp tokens.
    """

    key_usage: ClassVar[Set[str]] = set()
    """
    There are no (non-extended) key usage requirements for TSA certificates.
    """

    extd_key_usage = {'time_stamping'}
    """
    TSA certificates must have the ``time_stamping`` extended key usage
    extension (OID 1.3.6.1.5.5.7.3.8).
    """

    timestamp: datetime
    """
    Value of the timestamp token as a datetime object.
    """

    def describe_timestamp_trust(self):
        tsa = self.signing_cert

        return (
            "This timestamp is backed by a time stamping authority.\n"
            "The timestamp token is cryptographically "
            f"{'' if self.intact and self.valid else 'un'}sound.\n"
            f"TSA certificate subject: \"{tsa.subject.human_friendly}\"\n"
            f"TSA certificate SHA1 fingerprint: {tsa.sha1.hex()}\n"
            f"TSA certificate SHA256 fingerprint: {tsa.sha256.hex()}\n"
            f"TSA cert trust anchor: \"{self._trust_anchor}\"\n"
            "The TSA certificate is "
            f"{'' if self.trusted else 'un'}trusted."
        )


@dataclass(frozen=True)
class X509AttributeInfo:
    """
    Info on an X.509 attribute.
    """

    attr_type: cms.AttCertAttributeType
    """
    The certified attribute's type.
    """

    attr_values: Iterable[core.Asn1Value]
    """
    The certified attribute's values.
    """


@dataclass(frozen=True)
class CertifiedAttributeInfo(X509AttributeInfo):
    """
    Info on a certified attribute, including AC validation results.
    """

    validation_results: Iterable[ACValidationResult]
    """
    The validation details for the attribute in question
    (possibly several if values for the same attribute were sourced from
    several different ACs).
    """


def _handle_attr_err(
    attr_type: Optional[str], attr_kind: str, err: ValueError, fatal: bool
):
    attr_type_str = "unknown type" if not attr_type else f"type '{attr_type}'"
    msg = f"Failed to parse {attr_kind} of {attr_type_str}: {err.args[0]}"
    if fatal:
        raise SignatureValidationError(
            msg, ades_subindication=AdESFailure.FORMAT_FAILURE
        ) from err
    else:
        logger.warning(msg, exc_info=err)


class CertifiedAttributes:
    """
    Container class for extracted attribute certificate information.
    """

    @classmethod
    def from_results(
        cls, results: Iterable[ACValidationResult], parse_error_fatal=False
    ):
        # first, classify the attributes and results by type
        by_type: Dict[str, Tuple[List[Any], List[ACValidationResult]]] = (
            defaultdict(lambda: ([], []))
        )
        for result in results:
            for attr in result.approved_attributes.values():
                attr_type = attr['type'].native
                try:
                    values = list(attr['values'])  # force a surface-level parse
                except ValueError as e:
                    _handle_attr_err(
                        attr_type,
                        "certified attribute",
                        e,
                        fatal=parse_error_fatal,
                    )
                    continue
                type_values, type_results = by_type[attr_type]
                type_values.extend(values)
                type_results.append(result)
        # then, for each type, we package 'em up in a CertifiedAttributeInfo
        infos = CertifiedAttributes()
        for attr_type, (type_values, type_results) in by_type.items():
            infos._attrs[attr_type] = CertifiedAttributeInfo(
                attr_type=cms.AttCertAttributeType(attr_type),
                # (shallow) immutability
                attr_values=tuple(type_values),
                validation_results=tuple(type_results),
            )
        return infos

    def __init__(self: 'CertifiedAttributes'):
        self._attrs: Dict[str, CertifiedAttributeInfo] = {}

    def __getitem__(self, item: str) -> CertifiedAttributeInfo:
        return self._attrs[item]

    def __len__(self):
        return len(self._attrs)

    def __bool__(self):
        return bool(self._attrs)

    def __iter__(self):
        return iter(self._attrs.values())

    def __contains__(self, item: str) -> bool:
        return item in self._attrs


class ClaimedAttributes:
    """
    Container class for extracted information on attributes asserted
    by a signer without an attribute certificate.
    """

    @classmethod
    def from_iterable(
        cls, attrs: Iterable[cms.AttCertAttribute], parse_error_fatal=False
    ):
        infos = ClaimedAttributes()
        by_type = defaultdict(list)
        for attr in attrs:
            attr_type = None
            try:
                attr_type = attr['type'].native
                values = list(attr['values'])  # force a surface-level parse
            except ValueError as e:
                _handle_attr_err(
                    attr_type, "claimed attribute", e, fatal=parse_error_fatal
                )
                continue
            by_type[attr_type].extend(values)

        for attr_type, type_values in by_type.items():
            infos._attrs[attr_type] = X509AttributeInfo(
                attr_type=cms.AttCertAttributeType(attr_type),
                # (shallow) immutability
                attr_values=tuple(type_values),
            )
        return infos

    def __init__(self: 'ClaimedAttributes'):
        self._attrs: Dict[str, X509AttributeInfo] = {}

    def __getitem__(self, item: str) -> X509AttributeInfo:
        return self._attrs[item]

    def __len__(self):
        return len(self._attrs)

    def __bool__(self):
        return bool(self._attrs)

    def __iter__(self):
        return iter(self._attrs.values())

    def __contains__(self, item: str) -> bool:
        return item in self._attrs


@dataclass(frozen=True)
class CAdESSignerAttributeAssertions:
    """
    Value type describing information extracted (and, if relevant, validated)
    from a ``signer-attrs-v2`` signed attribute.
    """

    claimed_attrs: ClaimedAttributes
    """
    Attributes claimed by the signer without additional justification.
    May be empty.
    """

    certified_attrs: Optional[CertifiedAttributes] = None
    """
    Attributes claimed by the signer using an attribute certificate.

    This field will only be populated if an attribute certificate
    validation context is available, otherwise its value will be ``None``,
    even if there are no attribute certificates present.
    """

    ac_validation_errs: Optional[
        Collection[Union[ValidationError, PathBuildingError]]
    ] = None
    """
    Attribute certificate validation errors.

    This field will only be populated if an attribute certificate
    validation context is available, otherwise its value will be ``None``,
    even if there are no attribute certificates present.
    """

    unknown_attrs_present: bool = False
    """
    Records if the ``signer-attrs-v2`` attribute contained certificate types
    or signed assertions that could not be processed.

    This does not affect the validation process by default, but will trigger
    a warning.
    """

    @property
    def valid(self):
        return not self.ac_validation_errs


@dataclass(frozen=True)
class SignerAttributeStatus:
    ac_attrs: Optional[CertifiedAttributes] = None
    """
    Certified attributes sourced from valid attribute certificates embedded into
    the ``SignedData``'s ``certificates`` field and the CAdES-style
    ``signer-attrs-v2`` attribute (if present).

    Will be ``None`` if no validation context for attribute certificate
    validation was provided.

    .. note::
        There is a semantic difference between attribute certificates
        extracted from the ``certificates`` field and those extracted from
        the ``signer-attrs-v2`` attribute.
        In the former case, the ACs are not covered by the signature.
        However, a CAdES-style ``signer-attrs-v2`` attribute is signed, so
        the signer is expected to have explicitly _acknowledged_ all attributes,
        in the AC. See also :attr:`cades_signer_attrs`.
    """

    ac_validation_errs: Optional[
        Collection[Union[PathValidationError, PathBuildingError]]
    ] = None
    """
    Errors encountered while validating attribute certificates embedded into
    the ``SignedData``'s ``certificates`` field and the CAdES-style
    ``signer-attrs-v2`` attribute (if present).

    Will be ``None`` if no validation context for attribute certificate
    validation was provided.
    """

    cades_signer_attrs: Optional[CAdESSignerAttributeAssertions] = None
    """
    Information extracted and validated from the signed ``signer-attrs-v2``
    attribute defined in CAdES.
    """


@dataclass(frozen=True)
class StandardCMSSignatureStatus(SignerAttributeStatus, SignatureStatus):
    """
    Status of a standard "end-entity" CMS signature, potentially with
    timing information embedded inside.
    """

    signer_reported_dt: Optional[datetime] = None
    """
    Signer-reported signing time, if present in the signature.

    Generally speaking, this timestamp should not be taken as fact.
    """

    timestamp_validity: Optional[TimestampSignatureStatus] = None
    """
    Validation status of the signature timestamp token embedded in this
    signature, if present.
    """

    content_timestamp_validity: Optional[TimestampSignatureStatus] = None
    """
    Validation status of the content timestamp token embedded in this
    signature, if present.
    """

    @property
    def bottom_line(self) -> bool:
        """
        Formulates a general judgment on the validity of this signature.
        This takes into account the cryptographic validity of the signature,
        the signature's chain of trust and the validity of the timestamp token
        (if present).

        :return:
            ``True`` if all constraints are satisfied, ``False`` otherwise.
        """

        ts = self.timestamp_validity
        if ts is None:
            timestamp_ok = True
        else:
            timestamp_ok = ts.valid and ts.intact and ts.trusted

        content_ts = self.content_timestamp_validity
        if content_ts is None:
            content_timestamp_ok = True
        else:
            content_timestamp_ok = (
                content_ts.valid and content_ts.intact and content_ts.trusted
            )

        return (
            self.intact
            and self.valid
            and self.trusted
            and timestamp_ok
            and content_timestamp_ok
        )

    def summary_fields(self):
        yield from super().summary_fields()

        if self.timestamp_validity is not None:
            yield 'TIMESTAMP_TOKEN<%s>' % (
                self.timestamp_validity.summary(delimiter='|')
            )
        if self.content_timestamp_validity is not None:
            yield 'CONTENT_TIMESTAMP_TOKEN<%s>' % (
                self.content_timestamp_validity.summary(delimiter='|')
            )
        if (
            self.cades_signer_attrs is not None
            and not self.cades_signer_attrs.valid
        ):
            yield 'CERTIFIED_SIGNER_ATTRS_INVALID'

    def pretty_print_details(self):
        def fmt_section(hdr, body):
            return '\n'.join((hdr, '-' * len(hdr), body, '\n'))

        sections = self.pretty_print_sections()
        bottom_line = (
            f"The signature is judged {'' if self.bottom_line else 'IN'}VALID."
        )
        sections.append(("Bottom line", bottom_line))
        return '\n'.join(fmt_section(hdr, body) for hdr, body in sections)

    def pretty_print_sections(self) -> List[Tuple[str, str]]:
        cert: x509.Certificate = self.signing_cert

        # TODO add section about ACs
        if self.trusted:
            trust_status = "trusted"
        elif self.revoked:
            trust_status = "revoked"
        else:
            trust_status = "untrusted"
        about_signer = (
            f"Certificate subject: \"{cert.subject.human_friendly}\"\n"
            f"Certificate SHA1 fingerprint: {cert.sha1.hex()}\n"
            f"Certificate SHA256 fingerprint: {cert.sha256.hex()}\n"
            f"Trust anchor: \"{self._trust_anchor}\"\n"
            f"The signer's certificate is {trust_status}."
        )

        validity_info = (
            "The signature is cryptographically "
            f"{'' if self.intact and self.valid else 'un'}sound.\n\n"
            f"The digest algorithm used was '{self.md_algorithm}'.\n"
            f"The signature mechanism used was "
            f"'{self.pkcs7_signature_mechanism}'."
        )
        if 'ecdsa' in self.pkcs7_signature_mechanism:
            ec_params: keys.ECDomainParameters = cert.public_key['algorithm'][
                'parameters'
            ]
            if ec_params.name == 'named':
                curve_oid: core.ObjectIdentifier = ec_params.chosen
                validity_info += (
                    f"\nThe elliptic curve used for the signer's ECDSA "
                    f"public key was '{curve_oid.native}' "
                    f"(OID: {curve_oid.dotted})."
                )

        timing_infos = []
        reported_ts = self.signer_reported_dt
        if reported_ts is not None:
            timing_infos.append(
                f"Signing time as reported by signer: {reported_ts.isoformat()}"
            )

        tst_status = self.timestamp_validity
        if tst_status is not None:
            ts = tst_status.timestamp
            timing_infos.append(
                f"Signature timestamp token: {ts.isoformat()}\n"
                f"The token is guaranteed to be newer than the signature.\n"
                f"{tst_status.describe_timestamp_trust()}"
            )
        content_tst_status = self.content_timestamp_validity
        if content_tst_status is not None:
            ts = content_tst_status.timestamp
            timing_infos.append(
                f"Content timestamp token: {ts.isoformat()}\n"
                f"The token is guaranteed to be older than the signature.\n"
                f"{content_tst_status.describe_timestamp_trust()}"
            )
        timing_info = (
            "No available information about the signing time."
            if not timing_infos
            else '\n\n'.join(timing_infos)
        )
        return [
            ("Signer info", about_signer),
            ("Integrity", validity_info),
            ("Signing time", timing_info),
        ]


@unique
class SignatureCoverageLevel(OrderedEnum):
    """
    Indicate the extent to which a PDF signature (cryptographically) covers
    a document. Note that this does *not* pass judgment on whether uncovered
    updates are legitimate or not, but as a general rule, a legitimate signature
    will satisfy at least :attr:`ENTIRE_REVISION`.
    """

    UNCLEAR = 0
    """
    The signature's coverage is unclear and/or disconnected.
    In standard PDF signatures, this is usually a bad sign.
    """

    CONTIGUOUS_BLOCK_FROM_START = 1
    """
    The signature covers a contiguous block in the PDF file stretching from
    the first byte of the file to the last byte in the indicated ``/ByteRange``.
    In other words, the only interruption in the byte range is fully occupied
    by the signature data itself.
    """

    ENTIRE_REVISION = 2
    """
    The signature covers the entire revision in which it occurs, but incremental
    updates may have been added later. This is not necessarily evidence of
    tampering. In particular, it is expected when a file contains multiple
    signatures. Nonetheless, caution is required.
    """

    ENTIRE_FILE = 3
    """
    The entire file is covered by the signature.
    """


@dataclass(frozen=True)
class ModificationInfo:
    coverage: Optional[SignatureCoverageLevel] = None
    """
    Indicates how much of the document is covered by the signature.
    """

    diff_result: Optional[Union[DiffResult, SuspiciousModification]] = None
    """
    Result of the difference analysis run on the file:

    * If ``None``, no difference analysis was run.
    * If the difference analysis was successful, this attribute will contain
      a :class:`.DiffResult` object.
    * If the difference analysis failed due to unforeseen or suspicious
      modifications, the :class:`.SuspiciousModification` exception thrown
      by the difference policy will be stored in this attribute.
    """

    docmdp_ok: Optional[bool] = None
    """
    Indicates whether the signature's
    :attr:`~.ModificationInfo.modification_level` is in line with the document
    signature policy in force.

    If ``None``, compliance could not be determined.
    """

    @property
    def modification_level(self) -> Optional[ModificationLevel]:
        """
        Indicates the degree to which the document was modified after the
        signature was applied.

        Will be ``None`` if difference analysis results are not available;
        an instance of :class:`.ModificationLevel` otherwise.
        """

        coverage = self.coverage
        if self.diff_result is None:
            if coverage == SignatureCoverageLevel.ENTIRE_REVISION:
                # in this case, we can't know without the diff analysis result
                return None
            return (
                ModificationLevel.NONE
                if coverage == SignatureCoverageLevel.ENTIRE_FILE
                else ModificationLevel.OTHER
            )
        elif isinstance(self.diff_result, DiffResult):
            return self.diff_result.modification_level
        else:
            return ModificationLevel.OTHER


@dataclass(frozen=True)
class PdfSignatureStatus(ModificationInfo, StandardCMSSignatureStatus):
    """Class to indicate the validation status of a PDF signature."""

    has_seed_values: bool = False
    """
    Records whether the signature form field has seed values.
    """

    seed_value_constraint_error: Optional[SigSeedValueValidationError] = None
    """
    Records the reason for failure if the signature field's seed value
    constraints didn't validate.
    """

    @property
    def bottom_line(self) -> bool:
        """
        Formulates a general judgment on the validity of this signature.
        This takes into account the cryptographic validity of the signature,
        the signature's chain of trust, compliance with the document
        modification policy, seed value constraint compliance and the validity
        of the timestamp token (if present).

        :return:
            ``True`` if all constraints are satisfied, ``False`` otherwise.
        """
        generic_checks_ok = super().bottom_line

        return (
            generic_checks_ok
            and self.seed_value_ok
            and (self.docmdp_ok or self.modification_level is None)
        )

    @property
    def seed_value_ok(self) -> bool:
        """
        Indicates whether the signature satisfies all mandatory constraints in
        the seed value dictionary of the associated form field.

        .. warning::
            Currently, not all seed value entries are recognised by the signer
            and/or the validator, so this judgment may not be entirely accurate
            in some cases.

            See :class:`~.pyhanko.sign.fields.SigSeedValueSpec`.
        """

        return self.seed_value_constraint_error is None

    def summary_fields(self):
        yield from super().summary_fields()
        if self.coverage == SignatureCoverageLevel.ENTIRE_FILE:
            yield 'UNTOUCHED'
        elif self.coverage == SignatureCoverageLevel.ENTIRE_REVISION:
            if self.modification_level is not None:
                yield 'EXTENDED_WITH_' + self.modification_level.name
            else:
                yield 'EXTENDED'
        else:
            yield 'NONSTANDARD_COVERAGE'
        if self.docmdp_ok:
            if self.coverage != SignatureCoverageLevel.ENTIRE_FILE:
                yield 'ACCEPTABLE_MODIFICATIONS'
        else:
            yield 'ILLEGAL_MODIFICATIONS'

    def pretty_print_sections(self):
        sections = super().pretty_print_sections()
        if self.coverage == SignatureCoverageLevel.ENTIRE_FILE:
            modification_str = "The signature covers the entire file."
        else:
            if self.modification_level is not None:
                if self.modification_level == ModificationLevel.LTA_UPDATES:
                    modlvl_string = (
                        "All modifications relate to signature maintenance"
                    )
                elif self.modification_level == ModificationLevel.FORM_FILLING:
                    modlvl_string = (
                        "All modifications relate to signing and form filling "
                        "operations"
                    )
                else:
                    modlvl_string = "Some modifications may be illegitimate"
                modification_str = (
                    "The signature does not cover the entire file.\n"
                    f"{modlvl_string}, and they appear to be "
                    f"{'' if self.docmdp_ok else 'in'}compatible with the "
                    "current document modification policy."
                )
            else:
                modification_str = "Incremental update analysis was skipped"

        sections.append(("Modifications", modification_str))
        if self.has_seed_values:
            if self.seed_value_ok:
                sv_info = "There were no SV issues detected for this signature."
            else:
                sv_info = (
                    "The signature did not satisfy the SV constraints on "
                    "the signature field.\nError message: "
                    + self.seed_value_constraint_error.failure_message
                )
            sections.append(("Seed value constraints", sv_info))

        return sections


@dataclass(frozen=True)
class DocumentTimestampStatus(ModificationInfo, TimestampSignatureStatus):
    """Class to indicate the validation status of a PDF document timestamp."""
