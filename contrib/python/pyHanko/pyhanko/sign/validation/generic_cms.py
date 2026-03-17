import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import (
    IO,
    Any,
    Awaitable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from asn1crypto import algos, cms, core, tsp, x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from pyhanko_certvalidator import (
    CancelableAsyncIterator,
    ValidationContext,
    find_valid_path,
)
from pyhanko_certvalidator.errors import (
    DisallowedAlgorithmError,
    ExpiredError,
    InvalidCertificateError,
    PathBuildingError,
    PathValidationError,
    RevokedError,
    StaleRevinfoError,
    ValidationError,
)
from pyhanko_certvalidator.ltv.errors import TimeSlideFailure
from pyhanko_certvalidator.path import ValidationPath
from pyhanko_certvalidator.policy_decl import PKIXValidationParams
from pyhanko_certvalidator.validate import ACValidationResult, async_validate_ac

from pyhanko.sign.general import (
    CMSExtractionError,
    CMSStructuralError,
    MultivaluedAttributeError,
    NonexistentAttributeError,
    SignedDataCerts,
    check_ess_certid,
    extract_certificate_info,
    extract_signer_info,
    find_unique_cms_attribute,
    get_pyca_cryptography_hash,
)

from ...pdf_utils import misc
from ...pdf_utils.misc import lift_iterable_async
from ..ades.report import AdESFailure, AdESIndeterminate
from . import errors
from .settings import KeyUsageConstraints
from .status import (
    CAdESSignerAttributeAssertions,
    CertifiedAttributes,
    ClaimedAttributes,
    RevocationDetails,
    SignatureStatus,
    StandardCMSSignatureStatus,
    TimestampSignatureStatus,
)
from .utils import (
    DEFAULT_ALGORITHM_USAGE_POLICY,
    CMSAlgorithmUsagePolicy,
    extract_message_digest,
    validate_raw,
)

__all__ = [
    'validate_sig_integrity',
    'async_validate_cms_signature',
    'collect_timing_info',
    'validate_tst_signed_data',
    'async_validate_detached_cms',
    'cms_basic_validation',
    'compute_signature_tst_digest',
    'extract_tst_data',
    'extract_self_reported_ts',
    'extract_certs_for_validation',
    'collect_signer_attr_status',
    'validate_algorithm_protection',
    'get_signing_cert_attr',
]

logger = logging.getLogger(__name__)

StatusType = TypeVar('StatusType', bound=SignatureStatus)


def get_signing_cert_attr(
    signed_attrs: cms.CMSAttributes,
) -> Union[tsp.SigningCertificate, tsp.SigningCertificateV2, None]:
    """
    Retrieve the ``signingCertificate`` or ``signingCertificateV2`` attribute
    (giving preference to the latter) from a signature's signed attributes.

    :param signed_attrs:
        Signed attributes.
    :return:
        The value of the attribute, if present, else ``None``.
    """
    attr = _grab_signing_cert_attr(signed_attrs, v2=True)
    if attr is None:
        attr = _grab_signing_cert_attr(signed_attrs, v2=False)
    return attr


def _grab_signing_cert_attr(signed_attrs, v2: bool):
    # TODO check certificate policies, enforce restrictions on chain of trust
    # TODO document and/or mark as internal API explicitly
    attr_name = 'signing_certificate_v2' if v2 else 'signing_certificate'
    cls = tsp.SigningCertificateV2 if v2 else tsp.SigningCertificate
    try:
        value = find_unique_cms_attribute(signed_attrs, attr_name)
        # reencode the attribute to avoid accidentally tripping the
        # _is_mutated logic on the parent object (is important to preserve
        # the state of the signed attributes)
        return cls.load(value.dump())
    except NonexistentAttributeError:
        return None
    except MultivaluedAttributeError as e:
        # Banned by RFCs -> error
        err = AdESIndeterminate.NO_SIGNING_CERTIFICATE_FOUND
        raise errors.SignatureValidationError(
            "Wrong cardinality for signing certificate attribute",
            ades_subindication=err,
        ) from e


def _check_signing_certificate(
    cert: x509.Certificate, signed_attrs: cms.CMSAttributes
):
    # TODO check certificate policies, enforce restrictions on chain of trust
    # TODO document and/or mark as internal API explicitly

    attr = get_signing_cert_attr(signed_attrs)
    if attr is None:
        # if not present -> no constraints
        return

    # For the main signer cert, we only care about the first value, the others
    # limit the set of applicable CA certs
    certid = attr['certs'][0]

    if not check_ess_certid(cert, certid):
        err = AdESIndeterminate.NO_SIGNING_CERTIFICATE_FOUND
        raise errors.SignatureValidationError(
            f"Signing certificate attribute does not match selected "
            f"signer's certificate for subject"
            f"\"{cert.subject.human_friendly}\".",
            ades_subindication=err,
        )


def validate_algorithm_protection(
    attrs: cms.CMSAttributes,
    claimed_digest_algorithm_obj: cms.DigestAlgorithm,
    claimed_signature_algorithm_obj: Optional[algos.SignedDigestAlgorithm],
    claimed_mac_algorithm_obj: Optional[algos.HmacAlgorithm],
):
    """
    Internal API to validate the CMS algorithm protection attribute
    defined in :rfc:`6211`, if present.

    :param attrs:
        A CMS attribute list.
    :param claimed_digest_algorithm_obj:
        The claimed (i.e. unprotected) digest algorithm value.
    :param claimed_signature_algorithm_obj:
        The claimed (i.e. unprotected) signature algorithm value.
    :param claimed_mac_algorithm_obj:
        The claimed (i.e. unprotected) MAC algorithm value.
    :raises errors.CMSStructuralError:
        if multiple CMS protection attributes are present
    :raises errors.CMSAlgorithmProtectionError:
        if a mismatch is detected
    """

    try:
        cms_algid_protection = find_unique_cms_attribute(
            attrs, 'cms_algorithm_protection'
        )
    except NonexistentAttributeError:
        # TODO make this optional to enforce?
        cms_algid_protection = None
    except MultivaluedAttributeError:
        raise CMSStructuralError(
            "Multiple CMS algorithm protection attributes present",
        )
    if cms_algid_protection is not None:
        auth_digest_algorithm = cms_algid_protection['digest_algorithm'].native
        if auth_digest_algorithm != claimed_digest_algorithm_obj.native:
            raise errors.CMSAlgorithmProtectionError(
                "Digest algorithm does not match CMS algorithm protection "
                "attribute.",
            )
        if claimed_signature_algorithm_obj is not None:
            auth_sig_algorithm = cms_algid_protection[
                'signature_algorithm'
            ].native
            if auth_sig_algorithm is None:
                raise errors.CMSAlgorithmProtectionError(
                    "CMS algorithm protection attribute not valid for signed "
                    "data",
                )
            elif auth_sig_algorithm != claimed_signature_algorithm_obj.native:
                raise errors.CMSAlgorithmProtectionError(
                    "Signature mechanism does not match CMS algorithm "
                    "protection attribute.",
                )
        if claimed_mac_algorithm_obj is not None:
            auth_mac_algorithm = cms_algid_protection['mac_algorithm'].native
            if auth_mac_algorithm is None:
                raise errors.CMSAlgorithmProtectionError(
                    "CMS algorithm protection attribute not valid for "
                    "authenticated data",
                )
            elif auth_mac_algorithm != claimed_mac_algorithm_obj.native:
                raise errors.CMSAlgorithmProtectionError(
                    "MAC mechanism does not match CMS algorithm "
                    "protection attribute.",
                )


def validate_sig_integrity(
    signer_info: cms.SignerInfo,
    cert: x509.Certificate,
    expected_content_type: str,
    actual_digest: bytes,
    algorithm_usage_policy: Optional[CMSAlgorithmUsagePolicy] = None,
    time_indic: Optional[datetime] = None,
) -> Tuple[bool, bool]:
    """
    Validate the integrity of a signature for a particular signerInfo object
    inside a CMS signed data container.

    .. warning::
        This function does not do any trust checks, and is considered
        "dangerous" API because it is easy to misuse.

    :param signer_info:
        A :class:`cms.SignerInfo` object.
    :param cert:
        The signer's certificate.

        .. note::
            This function will not attempt to extract certificates from
            the signed data.
    :param expected_content_type:
        The expected value for the content type attribute (as a Python string,
        see :class:`cms.ContentType`).
    :param actual_digest:
        The actual digest to be matched to the message digest attribute.
    :param algorithm_usage_policy:
        Algorithm usage policy.
    :param time_indic:
        Time indication for the production of the signature.
    :return:
        A tuple of two booleans. The first indicates whether the provided
        digest matches the value in the signed attributes.
        The second indicates whether the signature of the digest is valid.
    """

    signature_algorithm: cms.SignedDigestAlgorithm = signer_info[
        'signature_algorithm'
    ]
    digest_algorithm_obj = signer_info['digest_algorithm']
    md_algorithm = digest_algorithm_obj['algorithm'].native
    if algorithm_usage_policy is not None:
        sig_algo_allowed = algorithm_usage_policy.signature_algorithm_allowed(
            signature_algorithm, moment=time_indic, public_key=cert.public_key
        )
        if not sig_algo_allowed:
            msg = (
                f"The algorithm {signature_algorithm['algorithm'].native} "
                f"is not allowed by the current usage policy."
            )
            if sig_algo_allowed.failure_reason is not None:
                msg += f" Reason: {sig_algo_allowed.failure_reason}."
            raise errors.DisallowedAlgorithmError(
                msg, permanent=sig_algo_allowed.not_allowed_after is None
            )
        digest_algo_allowed = algorithm_usage_policy.digest_algorithm_allowed(
            digest_algorithm_obj,
            moment=time_indic,
        )
        if not digest_algo_allowed:
            msg = (
                f"The algorithm {digest_algorithm_obj['algorithm'].native} "
                f"is not allowed by the current usage policy."
            )
            if digest_algo_allowed.failure_reason is not None:
                msg += f" Reason: {digest_algo_allowed.failure_reason}."
            raise errors.DisallowedAlgorithmError(
                msg, permanent=digest_algo_allowed.not_allowed_after is None
            )

    signature = signer_info['signature'].native

    signed_attrs_orig: cms.CMSAttributes = signer_info['signed_attrs']

    if signed_attrs_orig is core.VOID:
        embedded_digest = None
        prehashed = True
        signed_data = actual_digest
    else:
        # signed_attrs comes with context-specific tagging.
        # We need to re-tag it with a universal SET OF tag.
        signed_attrs = signer_info['signed_attrs'].untag()
        # do this ASAP to minimise the chances of accidentally disturbing
        # the state. We want to tolerate inconsequential deviations from DER,
        # even though CMS mandates strict adherence to DER (not all signers
        # follow that rule)
        # TODO offer a mode with ultra-strict adherence to DER where we call
        #  dump(force=True) here. That requires changes to asn1crypto, though,
        #  since it is too eager to mess with URI values in ways that go beyond
        #  DER.
        signed_data = signed_attrs.dump()
        prehashed = False
        # check the CMSAlgorithmProtection attr, if present
        try:
            validate_algorithm_protection(
                signed_attrs,
                claimed_digest_algorithm_obj=digest_algorithm_obj,
                claimed_signature_algorithm_obj=signature_algorithm,
                claimed_mac_algorithm_obj=None,
            )
        except CMSStructuralError as e:
            raise errors.SignatureValidationError(
                e.failure_message, ades_subindication=AdESFailure.FORMAT_FAILURE
            )
        except errors.CMSAlgorithmProtectionError as e:
            raise errors.SignatureValidationError(
                e.failure_message,
                # these are conceptually failures, but AdES doesn't have
                # them in its validation model, so 'GENERIC' it is.
                #  (same applies to other such cases)
                ades_subindication=AdESIndeterminate.GENERIC,
            )

        # check the signing-certificate or signing-certificate-v2 attr
        # Note: Through the usual "full validation" call path, this check is
        #   performed twice. AdES requires the check to be performed when
        #   selecting the signer's certificate (which happens elsewhere), but
        #   we keep this check for compatibility for those cases where
        #   validate_sig_integrity is used standalone.
        _check_signing_certificate(cert, signed_attrs)

        try:
            content_type = find_unique_cms_attribute(
                signed_attrs, 'content_type'
            )
        except (NonexistentAttributeError, MultivaluedAttributeError):
            raise errors.SignatureValidationError(
                'Content type not found in signature, or multiple content-type '
                'attributes present.',
                ades_subindication=AdESFailure.FORMAT_FAILURE,
            )
        content_type = content_type.native
        if content_type != expected_content_type:
            raise errors.SignatureValidationError(
                f'Content type {content_type} did not match expected value '
                f'{expected_content_type}',
                ades_subindication=AdESFailure.FORMAT_FAILURE,
            )

        embedded_digest = extract_message_digest(signer_info)

    try:
        validate_raw(
            signature,
            signed_data,
            cert,
            signature_algorithm,
            md_algorithm,
            prehashed=prehashed,
            algorithm_policy=algorithm_usage_policy,
            time_indic=time_indic,
        )
        valid = True
    except InvalidSignature:
        valid = False

    intact = (
        actual_digest == embedded_digest
        if embedded_digest is not None
        else valid
    )

    return intact, valid


def extract_certs_for_validation(
    signed_data: cms.SignedData,
) -> SignedDataCerts:
    """
    Extract certificates from a CMS signed data object for validation purposes,
    identifying the signer's certificate in accordance with ETSI EN 319 102-1,
    5.2.3.4.

    :param signed_data:
        The CMS payload.
    :return:
        The extracted certificates.
    """

    # TODO allow signer certificate to be obtained from elsewhere?

    try:
        cert_info = extract_certificate_info(signed_data)
        cert = cert_info.signer_cert
    except CMSExtractionError:
        raise errors.SignatureValidationError(
            'signer certificate not included in signature',
            ades_subindication=AdESIndeterminate.NO_SIGNING_CERTIFICATE_FOUND,
        )
    signer_info = extract_signer_info(signed_data)
    signed_attrs = signer_info['signed_attrs']
    # check the signing-certificate or signing-certificate-v2 attr
    _check_signing_certificate(cert, signed_attrs)
    return cert_info


async def cms_basic_validation(
    signed_data: cms.SignedData,
    raw_digest: Optional[bytes] = None,
    validation_context: Optional[ValidationContext] = None,
    status_kwargs: Optional[dict] = None,
    validation_path: Optional[ValidationPath] = None,
    pkix_validation_params: Optional[PKIXValidationParams] = None,
    algorithm_policy: Optional[CMSAlgorithmUsagePolicy] = None,
    *,
    key_usage_settings: KeyUsageConstraints,
) -> Dict[str, Any]:
    """
    Perform basic validation of CMS and PKCS#7 signatures in isolation
    (i.e. integrity and trust checks).

    Internal API.
    """
    signer_info = extract_signer_info(signed_data)
    cert_info = extract_certs_for_validation(signed_data)
    cert = cert_info.signer_cert
    other_certs = cert_info.other_certs

    time_indic = None
    if validation_context is not None:
        algorithm_policy = (
            algorithm_policy
            or CMSAlgorithmUsagePolicy.lift_policy(
                validation_context.algorithm_policy
            )
        )
        time_indic = validation_context.best_signature_time
    validation_context = validation_context or ValidationContext()
    if algorithm_policy is None:
        algorithm_policy = DEFAULT_ALGORITHM_USAGE_POLICY

    signature_algorithm: cms.SignedDigestAlgorithm = signer_info[
        'signature_algorithm'
    ]
    mechanism = signature_algorithm['algorithm'].native
    md_algorithm = signer_info['digest_algorithm']['algorithm'].native
    eci = signed_data['encap_content_info']
    expected_content_type = eci['content_type'].native
    if raw_digest is None:
        # this means that there should be encapsulated data
        raw = bytes(eci['content'])
        md_spec = get_pyca_cryptography_hash(md_algorithm)
        md = hashes.Hash(md_spec)
        md.update(raw)
        raw_digest = md.finalize()
    elif eci['content'] is not core.VOID:
        raise errors.SignatureValidationError(
            "CMS structural error: detached signatures should not have "
            "encapsulated data",
            ades_subindication=AdESFailure.FORMAT_FAILURE,
        )

    # first, do the cryptographic identity checks
    # TODO theoretically (e.g. DSA with param inheritance) this requires
    #  doing the X.509 validation step first. Since nobody cares about DSA
    #  (let alone DSA with inherited parameters), that's just a "nice to have".
    try:
        intact, valid = validate_sig_integrity(
            signer_info,
            cert,
            expected_content_type=expected_content_type,
            actual_digest=raw_digest,
            algorithm_usage_policy=algorithm_policy,
            time_indic=time_indic,
        )
    except CMSStructuralError as e:
        raise errors.SignatureValidationError(
            "CMS structural error: " + e.failure_message,
            ades_subindication=AdESFailure.FORMAT_FAILURE,
        ) from e

    # next, validate trust
    ades_status = path = revo_details = error_time_horizon = None
    if valid:
        try:
            validation_context.certificate_registry.register_multiple(
                other_certs
            )

            paths: CancelableAsyncIterator[ValidationPath]
            if validation_path is not None:
                paths = lift_iterable_async([validation_path])
            else:
                paths = validation_context.path_builder.async_build_paths_lazy(
                    cert
                )

            op_result = await validate_cert_usage(
                cert,
                validation_context,
                key_usage_settings=key_usage_settings,
                paths=paths,
                pkix_validation_params=pkix_validation_params,
            )
            ades_status = op_result.error_subindic
            revo_details = op_result.revo_details
            path = op_result.success_result or op_result.error_path
            error_time_horizon = op_result.error_time_horizon
        except ValueError as e:
            logger.error("Processing error in validation process", exc_info=e)
            ades_status = AdESIndeterminate.CERTIFICATE_CHAIN_GENERAL_FAILURE

    status_kwargs = status_kwargs or {}
    status_kwargs['validation_time'] = (
        None if validation_context is None else validation_context.moment
    )
    status_kwargs.update(
        intact=intact,
        valid=valid,
        signing_cert=cert,
        md_algorithm=md_algorithm,
        pkcs7_signature_mechanism=mechanism,
        trust_problem_indic=ades_status,
        validation_path=path,
        revocation_details=revo_details,
        error_time_horizon=error_time_horizon,
    )
    return status_kwargs


async def validate_cert_usage(
    cert: x509.Certificate,
    validation_context: ValidationContext,
    key_usage_settings: KeyUsageConstraints,
    paths: CancelableAsyncIterator[ValidationPath],
    pkix_validation_params: Optional[PKIXValidationParams] = None,
) -> 'CertvalidatorOperationResult[ValidationPath]':
    """
    Low-level certificate validation routine.
    Internal API.
    """

    async def _check() -> ValidationPath:
        # validate usage without going through pyhanko_certvalidator
        key_usage_settings.validate(cert)
        return await find_valid_path(
            cert,
            paths,
            validation_context=validation_context,
            pkix_validation_params=pkix_validation_params,
        )

    return await handle_certvalidator_errors(_check())


@overload
async def async_validate_cms_signature(
    signed_data: cms.SignedData,
    *,
    status_cls: Type[StatusType],
    raw_digest: Optional[bytes] = None,
    validation_context: Optional[ValidationContext] = None,
    status_kwargs: Optional[dict] = None,
    key_usage_settings: Optional[KeyUsageConstraints] = None,
) -> StatusType: ...


@overload
async def async_validate_cms_signature(
    signed_data: cms.SignedData,
    *,
    raw_digest: Optional[bytes] = None,
    validation_context: Optional[ValidationContext] = None,
    status_kwargs: Optional[dict] = None,
    key_usage_settings: Optional[KeyUsageConstraints] = None,
) -> SignatureStatus: ...


async def async_validate_cms_signature(
    signed_data: cms.SignedData,
    status_cls=SignatureStatus,
    raw_digest: Optional[bytes] = None,
    validation_context: Optional[ValidationContext] = None,
    status_kwargs: Optional[dict] = None,
    key_usage_settings: Optional[KeyUsageConstraints] = None,
    algorithm_policy: Optional[CMSAlgorithmUsagePolicy] = None,
) -> StatusType:
    """
    Validate a CMS signature (i.e. a ``SignedData`` object).

    :param signed_data:
        The :class:`.asn1crypto.cms.SignedData` object to validate.
    :param status_cls:
        Status class to use for the validation result.
    :param raw_digest:
        Raw digest, computed from context.
    :param validation_context:
        Validation context to validate the signer's certificate.
    :param status_kwargs:
        Other keyword arguments to pass to the ``status_class`` when reporting
        validation results.
    :param key_usage_settings:
        A :class:`.KeyUsageConstraints` object specifying which key usages
        must or must not be present in the signer's certificate.
    :param algorithm_policy:
        The algorithm usage policy for the signature validation.

        .. warning::
            This is distinct from the algorithm usage policy used for
            certificate validation, but the latter will be used as a fallback
            if this parameter is not specified.

            It is nonetheless recommended to align both policies unless
            there is a clear reason to do otherwise.
    :return:
        A :class:`.SignatureStatus` object (or an instance of a proper subclass)
    """
    eff_key_usage_settings = status_cls.default_usage_constraints(
        key_usage_settings
    )
    status_kwargs = await cms_basic_validation(
        signed_data,
        raw_digest,
        validation_context,
        status_kwargs,
        key_usage_settings=eff_key_usage_settings,
        algorithm_policy=algorithm_policy,
    )
    # noinspection PyArgumentList
    return status_cls(**status_kwargs)


def extract_self_reported_ts(signer_info: cms.SignerInfo) -> Optional[datetime]:
    """
    Extract self-reported timestamp (from the ``signingTime`` attribute)

    Internal API.

    :param signer_info:
        A ``SignerInfo`` value.
    :return:
        The value of the ``signingTime`` attribute as a ``datetime``, or
        ``None``.
    """
    try:
        sa = signer_info['signed_attrs']
        st = find_unique_cms_attribute(sa, 'signing_time')
        return st.native
    except (NonexistentAttributeError, MultivaluedAttributeError):
        return None


def extract_tst_data(
    signer_info: cms.SignerInfo, signed: bool = False
) -> Optional[cms.SignedData]:
    """
    Extract signed data associated with a timestamp token.

    Internal API.

    :param signer_info:
        A ``SignerInfo`` value.
    :param signed:
        If ``True``, look for a content timestamp (among the signed
        attributes), else look for a signature timestamp (among the unsigned
        attributes).
    :return:
        The ``SignedData`` value found, or ``None``.
    """
    try:
        if signed:
            sa = signer_info['signed_attrs']
            tst = find_unique_cms_attribute(sa, 'content_time_stamp')
        else:
            ua = signer_info['unsigned_attrs']
            tst = find_unique_cms_attribute(ua, 'signature_time_stamp_token')
        tst_signed_data = tst['content']
        return tst_signed_data
    except (NonexistentAttributeError, MultivaluedAttributeError):
        return None


def compute_signature_tst_digest(
    signer_info: cms.SignerInfo,
) -> Optional[bytes]:
    """
    Compute the digest of the signature according to the message imprint
    algorithm information in a signature timestamp token.

    Internal API.

    :param signer_info:
        A ``SignerInfo`` value.
    :return:
        The computed digest, or ``None`` if there is no signature timestamp.
    """

    tst_data = extract_tst_data(signer_info)
    if tst_data is None:
        return None

    eci = tst_data['encap_content_info']
    mi = eci['content'].parsed['message_imprint']
    tst_md_algorithm = mi['hash_algorithm']['algorithm'].native

    signature_bytes = signer_info['signature'].native
    tst_md_spec = get_pyca_cryptography_hash(tst_md_algorithm)
    md = hashes.Hash(tst_md_spec)
    md.update(signature_bytes)
    return md.finalize()


# TODO support signerInfo with multivalued timestamp attributes


async def collect_timing_info(
    signer_info: cms.SignerInfo,
    ts_validation_context: Optional[ValidationContext],
    raw_digest: bytes,
):
    """
    Collect and validate timing information in a ``SignerInfo`` value.
    This includes the ``signingTime`` attribute, content timestamp information
    and signature timestamp information.

    :param signer_info:
        A ``SignerInfo`` value.
    :param ts_validation_context:
        The timestamp validation context to validate against.
    :param raw_digest:
        The raw external message digest bytes (only relevant for the
        validation of the content timestamp token, if there is one)
    """

    status_kwargs: Dict[str, Any] = {}

    # timestamp-related validation
    signer_reported_dt = extract_self_reported_ts(signer_info)
    if signer_reported_dt is not None:
        status_kwargs['signer_reported_dt'] = signer_reported_dt

    tst_signed_data = extract_tst_data(signer_info, signed=False)
    if tst_signed_data is not None:
        tst_signature_digest = compute_signature_tst_digest(signer_info)
        assert tst_signature_digest is not None
        tst_validity_kwargs = await validate_tst_signed_data(
            tst_signed_data,
            ts_validation_context,
            tst_signature_digest,
        )
        tst_validity = TimestampSignatureStatus(**tst_validity_kwargs)
        status_kwargs['timestamp_validity'] = tst_validity

    content_tst_signed_data = extract_tst_data(signer_info, signed=True)
    if content_tst_signed_data is not None:
        content_tst_validity_kwargs = await validate_tst_signed_data(
            content_tst_signed_data,
            ts_validation_context,
            expected_tst_imprint=raw_digest,
        )
        content_tst_validity = TimestampSignatureStatus(
            **content_tst_validity_kwargs
        )
        status_kwargs['content_timestamp_validity'] = content_tst_validity

    return status_kwargs


async def validate_tst_signed_data(
    tst_signed_data: cms.SignedData,
    validation_context: Optional[ValidationContext],
    expected_tst_imprint: bytes,
    algorithm_policy: Optional[CMSAlgorithmUsagePolicy] = None,
):
    """
    Validate the ``SignedData`` of a time stamp token.

    :param tst_signed_data:
        The ``SignedData`` value to validate; must encapsulate a ``TSTInfo``
        value.
    :param validation_context:
        The validation context to validate against.
    :param expected_tst_imprint:
        The expected message imprint value that should be contained in
        the encapsulated ``TSTInfo``.
    :param algorithm_policy:
        The algorithm usage policy for the signature validation.

        .. warning::
            This is distinct from the algorithm usage policy used for
            certificate validation, but the latter will be used as a fallback
            if this parameter is not specified.

            It is nonetheless recommended to align both policies unless
            there is a clear reason to do otherwise.
    :return:
        Keyword arguments for a :class:`.TimeStampSignatureStatus`.
    """

    tst_info = None
    tst_info_bytes = tst_signed_data['encap_content_info']['content']
    if isinstance(tst_info_bytes, core.ParsableOctetString):
        tst_info = tst_info_bytes.parsed
    if not isinstance(tst_info, tsp.TSTInfo):
        raise errors.SignatureValidationError(
            "SignedData does not encapsulate TSTInfo",
            ades_subindication=AdESFailure.FORMAT_FAILURE,
        )
    timestamp = tst_info['gen_time'].native

    ku_settings = TimestampSignatureStatus.default_usage_constraints()
    status_kwargs = await cms_basic_validation(
        tst_signed_data,
        validation_context=validation_context,
        status_kwargs={'timestamp': timestamp},
        key_usage_settings=ku_settings,
        algorithm_policy=algorithm_policy,
    )
    # compare the expected TST digest against the message imprint
    # inside the signed data
    tst_imprint = tst_info['message_imprint']['hashed_message'].native
    if expected_tst_imprint != tst_imprint:
        logger.warning(
            f"Timestamp token imprint is {tst_imprint.hex()}, but expected "
            f"{expected_tst_imprint.hex()}."
        )
        status_kwargs['intact'] = False
    return status_kwargs


async def process_certified_attrs(
    acs: Iterable[cms.AttributeCertificateV2],
    signer_cert: x509.Certificate,
    validation_context: ValidationContext,
) -> Tuple[
    List[ACValidationResult],
    List[
        Union[PathValidationError, PathBuildingError, InvalidCertificateError]
    ],
]:
    jobs = [
        async_validate_ac(ac, validation_context, holder_cert=signer_cert)
        for ac in acs
    ]
    results = []
    errors = []
    for job in asyncio.as_completed(jobs):
        try:
            results.append(await job)
        except (
            PathBuildingError,
            PathValidationError,
            InvalidCertificateError,
        ) as e:
            errors.append(e)
    return results, errors


async def collect_signer_attr_status(
    sd_attr_certificates: Iterable[cms.AttributeCertificateV2],
    signer_cert: x509.Certificate,
    validation_context: Optional[ValidationContext],
    sd_signed_attrs: cms.CMSAttributes,
):
    # check if we need to process signer-attrs-v2 first
    try:
        signer_attrs = find_unique_cms_attribute(
            sd_signed_attrs, 'signer_attributes_v2'
        )
    except NonexistentAttributeError:
        signer_attrs = None
    except MultivaluedAttributeError as e:
        # TODO downgrade to a warning?
        raise errors.SignatureValidationError(
            str(e), ades_subindication=AdESFailure.FORMAT_FAILURE
        ) from e

    result: Dict[str, Any] = {}
    cades_ac_results = None
    cades_ac_errors = None
    if signer_attrs is not None:
        claimed_asn1 = signer_attrs['claimed_attributes']
        # process claimed attributes (no verification possible/required,
        # so this is independent of whether we have a validation context
        # available)
        # TODO offer a strict mode where all attributes must be recognised
        #  and/or at least parseable?
        claimed = ClaimedAttributes.from_iterable(
            claimed_asn1 if not isinstance(claimed_asn1, core.Void) else ()
        )
        # extract all X.509 attribute certs
        certified_asn1 = signer_attrs['certified_attributes_v2']
        unknown_cert_attrs = False
        if not isinstance(certified_asn1, core.Void):
            # if there are certified attributes but validation_context is None,
            # then cades_ac_results remains None
            cades_acs = [
                attr.chosen
                for attr in certified_asn1
                if attr.name == 'attr_cert'
            ]
            # record if there were other types of certified attributes
            unknown_cert_attrs = len(cades_acs) != len(certified_asn1)
            if validation_context is not None:
                # validate retrieved AC's
                val_job = process_certified_attrs(
                    cades_acs,
                    signer_cert,
                    validation_context,
                )
                cades_ac_results, cades_ac_errors = await val_job

        # If we were able to validate AC's from the signers-attrs-v2 attribute,
        # compile the validation results
        if cades_ac_results is not None:
            # TODO offer a strict mode where all attributes must be recognised
            #  and/or at least parseable?
            certified = CertifiedAttributes.from_results(cades_ac_results)
        else:
            certified = None

        # If there's a validation context (i.e. the caller cares about attribute
        #  validation semantics), then log a warning message in case there were
        # signed assertions or certified attributes that we didn't understand.
        unknown_attrs = unknown_cert_attrs or not isinstance(
            signer_attrs['signed_assertions'], core.Void
        )
        if validation_context is not None and unknown_attrs:
            logger.warning(
                "CAdES signer attributes with externally certified assertions "
                "for which no validation method is available. This may affect "
                "signature semantics in unexpected ways."
            )

        # store the result of the signer-attrs-v2 processing step
        result['cades_signer_attrs'] = CAdESSignerAttributeAssertions(
            claimed_attrs=claimed,
            certified_attrs=certified,
            ac_validation_errs=cades_ac_errors,
            unknown_attrs_present=unknown_attrs,
        )

    if validation_context is not None:
        # validate the ac's in the SD's 'certificates' entry, we have to do that
        # anyway
        ac_results, ac_errors = await process_certified_attrs(
            sd_attr_certificates, signer_cert, validation_context
        )
        # if there were validation results from the signer-attrs-v2 validation,
        # add them to the report here.
        if cades_ac_results:
            ac_results.extend(cades_ac_results)
        if cades_ac_errors:
            ac_errors.extend(cades_ac_errors)
        result['ac_attrs'] = CertifiedAttributes.from_results(ac_results)
        result['ac_validation_errs'] = ac_errors
    return result


async def async_validate_detached_cms(
    input_data: Union[bytes, IO, cms.ContentInfo, cms.EncapsulatedContentInfo],
    signed_data: cms.SignedData,
    signer_validation_context: Optional[ValidationContext] = None,
    ts_validation_context: Optional[ValidationContext] = None,
    ac_validation_context: Optional[ValidationContext] = None,
    key_usage_settings: Optional[KeyUsageConstraints] = None,
    algorithm_policy: Optional[CMSAlgorithmUsagePolicy] = None,
    chunk_size=misc.DEFAULT_CHUNK_SIZE,
    max_read=None,
) -> StandardCMSSignatureStatus:
    """
    .. versionadded: 0.9.0

    .. versionchanged: 0.11.0
        Added ``ac_validation_context`` param.

    Validate a detached CMS signature.

    :param input_data:
        The input data to sign. This can be either a :class:`bytes` object,
        a file-like object or a :class:`cms.ContentInfo` /
        :class:`cms.EncapsulatedContentInfo` object.

        If a CMS content info object is passed in, the `content` field
        will be extracted.
    :param signed_data:
        The :class:`cms.SignedData` object containing the signature to verify.
    :param signer_validation_context:
        Validation context to use to verify the signer certificate's trust.
    :param ts_validation_context:
        Validation context to use to verify the TSA certificate's trust, if
        a timestamp token is present.
        By default, the same validation context as that of the signer is used.
    :param ac_validation_context:
        Validation context to use to validate attribute certificates.
        If not supplied, no AC validation will be performed.

        .. note::
            :rfc:`5755` requires attribute authority trust roots to be specified
            explicitly; hence why there's no default.
    :param algorithm_policy:
        The algorithm usage policy for the signature validation.

        .. warning::
            This is distinct from the algorithm usage policy used for
            certificate validation, but the latter will be used as a fallback
            if this parameter is not specified.

            It is nonetheless recommended to align both policies unless
            there is a clear reason to do otherwise.
    :param key_usage_settings:
        Key usage parameters for the signer.
    :param chunk_size:
        Chunk size to use when consuming input data.
    :param max_read:
        Maximal number of bytes to read from the input stream.
    :return:
        A description of the signature's status.
    """

    if ts_validation_context is None:
        ts_validation_context = signer_validation_context
    signer_info = extract_signer_info(signed_data)
    digest_algorithm = signer_info['digest_algorithm']['algorithm'].native
    h = hashes.Hash(get_pyca_cryptography_hash(digest_algorithm))
    if isinstance(input_data, bytes):
        h.update(input_data)
    elif isinstance(input_data, (cms.ContentInfo, cms.EncapsulatedContentInfo)):
        h.update(bytes(input_data['content']))
    else:
        temp_buf = bytearray(chunk_size)
        misc.chunked_digest(temp_buf, input_data, h, max_read=max_read)
    digest_bytes = h.finalize()

    status_kwargs = await collect_timing_info(
        signer_info,
        ts_validation_context=ts_validation_context,
        raw_digest=digest_bytes,
    )
    key_usage_settings = StandardCMSSignatureStatus.default_usage_constraints(
        key_usage_settings
    )
    status_kwargs = await cms_basic_validation(
        signed_data,
        raw_digest=digest_bytes,
        validation_context=signer_validation_context,
        status_kwargs=status_kwargs,
        key_usage_settings=key_usage_settings,
        algorithm_policy=algorithm_policy,
    )
    cert_info = extract_certificate_info(signed_data)
    if ac_validation_context is not None:
        ac_validation_context.certificate_registry.register_multiple(
            cert_info.other_certs
        )
    status_kwargs.update(
        await collect_signer_attr_status(
            sd_attr_certificates=cert_info.attribute_certs,
            signer_cert=cert_info.signer_cert,
            validation_context=ac_validation_context,
            sd_signed_attrs=signer_info['signed_attrs'],
        )
    )
    return StandardCMSSignatureStatus(**status_kwargs)


ResultType = TypeVar('ResultType', covariant=True)


@dataclass(frozen=True)
class CertvalidatorOperationResult(Generic[ResultType]):
    """
    Internal class to inspect error data from certvalidator.
    """

    success_result: Optional[ResultType]
    revo_details: Optional[RevocationDetails] = None
    error_time_horizon: Optional[datetime] = None
    error_path: Optional[ValidationPath] = None
    error_subindic: Optional[AdESIndeterminate] = None


async def handle_certvalidator_errors(
    coro: Awaitable[ResultType],
) -> CertvalidatorOperationResult[ResultType]:
    """
    Internal error handling function that maps certvalidator errors
    to AdES status indications.

    :param coro:
    :return:
    """
    time_horizon: Optional[datetime] = None
    revo_details = path = None
    try:
        return CertvalidatorOperationResult(success_result=await coro)
    except InvalidCertificateError as e:
        logger.warning(e.failure_msg, exc_info=e)
        ades_status = AdESIndeterminate.CHAIN_CONSTRAINTS_FAILURE
    except TimeSlideFailure as e:
        logger.warning(e.failure_msg, exc_info=e)
        ades_status = AdESIndeterminate.NO_POE
    except StaleRevinfoError as e:
        logger.warning(e.failure_msg, exc_info=e)
        # note: the way pyhanko-certvalidator handles revinfo freshness
        # is not strictly compliant with AdES rules, but this mapping
        # should be roughly appropriate in most cases
        ades_status = AdESIndeterminate.TRY_LATER
        time_horizon = e.time_cutoff
    except DisallowedAlgorithmError as e:
        logger.warning(e.failure_msg, exc_info=e)
        # note: this is the one from the certvalidator hierarchy, which is
        # similar but not quite the same as the one for pyhanko itself
        # (conceptually identical, but the contextual data is different)
        path = e.original_path
        if e.banned_since is None:
            # permaban
            ades_status = AdESIndeterminate.CRYPTO_CONSTRAINTS_FAILURE
        else:
            # could get resolved with more POEs
            ades_status = AdESIndeterminate.CRYPTO_CONSTRAINTS_FAILURE_NO_POE
            time_horizon = e.banned_since
    except RevokedError as e:
        path = e.original_path
        logger.warning(e.failure_msg)
        time_horizon = e.revocation_dt
        if e.is_side_validation:
            # don't report this as a revocation event
            ades_status = AdESIndeterminate.CERTIFICATE_CHAIN_GENERAL_FAILURE
        elif e.is_ee_cert:
            ades_status = AdESIndeterminate.REVOKED_NO_POE
            revo_details = RevocationDetails(
                ca_revoked=False,
                revocation_date=e.revocation_dt,
                revocation_reason=e.reason,
            )
        else:
            ades_status = AdESIndeterminate.REVOKED_CA_NO_POE
            revo_details = RevocationDetails(
                ca_revoked=True,
                revocation_date=e.revocation_dt,
                revocation_reason=e.reason,
            )
    except PathBuildingError as e:
        logger.warning("Failed to build path", exc_info=e)
        ades_status = AdESIndeterminate.NO_CERTIFICATE_CHAIN_FOUND
    except ExpiredError as e:
        path = e.original_path
        logger.warning(e.failure_msg)
        time_horizon = e.expired_dt
        if not e.is_side_validation and e.is_ee_cert:
            # TODO modify certvalidator to perform revinfo checks on
            #  expired certs, possibly as an option. If this happens, we
            #  can potentially emit the more accurate status
            #  OUT_OF_BOUNDS_NOT_REVOKED here in cases where it applies.
            ades_status = AdESIndeterminate.OUT_OF_BOUNDS_NO_POE
        else:
            ades_status = AdESIndeterminate.CERTIFICATE_CHAIN_GENERAL_FAILURE
    except PathValidationError as e:
        path = e.original_path
        logger.warning(e.failure_msg, exc_info=e)
        ades_status = AdESIndeterminate.CERTIFICATE_CHAIN_GENERAL_FAILURE
    except ValidationError as e:
        logger.warning(e.failure_msg, exc_info=e)
        ades_status = AdESIndeterminate.CERTIFICATE_CHAIN_GENERAL_FAILURE

    return CertvalidatorOperationResult(
        success_result=None,
        revo_details=revo_details,
        error_time_horizon=time_horizon,
        error_path=path,
        error_subindic=ades_status,
    )
