import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Iterator, Optional, TypeVar

from asn1crypto import cms
from asn1crypto import pdf as asn1_pdf
from pyhanko_certvalidator import ValidationContext
from pyhanko_certvalidator.policy_decl import (
    CertRevTrustPolicy,
    RevocationCheckingPolicy,
    RevocationCheckingRule,
)

from pyhanko.pdf_utils.reader import PdfFileReader

from ..diff_analysis import DiffPolicy
from ..general import (
    MultivaluedAttributeError,
    NonexistentAttributeError,
    find_unique_cms_attribute,
)
from .dss import DocumentSecurityStore
from .errors import (
    NoDSSFoundError,
    SignatureValidationError,
    ValidationInfoReadingError,
)
from .generic_cms import (
    async_validate_cms_signature,
    cms_basic_validation,
    collect_signer_attr_status,
    validate_tst_signed_data,
)
from .pdf_embedded import EmbeddedPdfSignature, report_seed_value_validation
from .settings import KeyUsageConstraints
from .status import (
    PdfSignatureStatus,
    SignatureStatus,
    TimestampSignatureStatus,
)

__all__ = [
    'RevocationInfoValidationType',
    'apply_adobe_revocation_info',
    'retrieve_adobe_revocation_info',
    'get_timestamp_chain',
    'async_validate_pdf_ltv_signature',
    'establish_timestamp_trust',
]


logger = logging.getLogger(__name__)

StatusType = TypeVar('StatusType', bound=SignatureStatus)


class RevocationInfoValidationType(Enum):
    """
    Indicates a validation profile to use when validating revocation info.
    """

    ADOBE_STYLE = 'adobe'
    """
    Retrieve validation information from the CMS object, using Adobe's
    revocation info archival attribute.
    """

    PADES_LT = 'pades'
    """
    Retrieve validation information from the DSS, and require the signature's
    embedded timestamp to still be valid.
    """

    PADES_LTA = 'pades-lta'
    """
    Retrieve validation information from the DSS, but read & validate the chain
    of document timestamps leading up to the signature to establish the
    integrity of the validation information at the time of signing.
    """

    @classmethod
    def as_tuple(cls):
        return tuple(m.value for m in cls)


DEFAULT_LTV_INTERNAL_REVO_CHECK_POLICY = RevocationCheckingPolicy(
    ee_certificate_rule=RevocationCheckingRule.CHECK_IF_DECLARED,
    intermediate_ca_cert_rule=RevocationCheckingRule.CHECK_IF_DECLARED,
)


def _default_ltv_internal_revo_policy(**kwargs):
    return CertRevTrustPolicy(
        revocation_checking_policy=DEFAULT_LTV_INTERNAL_REVO_CHECK_POLICY,
        **kwargs,
    )


STRICT_LTV_INTERNAL_REVO_CHECK_POLICY = RevocationCheckingPolicy(
    ee_certificate_rule=RevocationCheckingRule.CRL_OR_OCSP_REQUIRED,
    intermediate_ca_cert_rule=RevocationCheckingRule.CRL_OR_OCSP_REQUIRED,
)


def _strict_ltv_internal_revo_policy(**kwargs):
    return CertRevTrustPolicy(
        revocation_checking_policy=STRICT_LTV_INTERNAL_REVO_CHECK_POLICY,
        **kwargs,
    )


def _strict_vc_context_kwargs(
    timestamp: datetime, validation_context_kwargs: dict
):
    # create a new validation context using the timestamp value as the time
    # of evaluation, turn off fetching and load OCSP responses / CRL data
    # from the DSS / revocation info object
    validation_context_kwargs['allow_fetching'] = False
    validation_context_kwargs['moment'] = timestamp

    # Certs with OCSP/CRL endpoints should have the relevant revocation data
    # embedded, if no stricter revocation_mode policy is in place already

    revinfo_policy: CertRevTrustPolicy = validation_context_kwargs.get(
        'revinfo_policy', None
    )
    retroactive = validation_context_kwargs.get('retroactive_revinfo', False)
    if revinfo_policy is None:
        # handle legacy revocation mode
        legacy_rm = validation_context_kwargs.pop('revocation_mode', None)
        if legacy_rm and legacy_rm != 'soft-fail':
            revinfo_policy = CertRevTrustPolicy(
                RevocationCheckingPolicy.from_legacy(legacy_rm),
            )
        elif legacy_rm == 'soft-fail':
            # fall back to the default
            revinfo_policy = _default_ltv_internal_revo_policy(
                retroactive_revinfo=retroactive
            )
    elif not revinfo_policy.revocation_checking_policy.essential:
        # also in this case, we sub in the default
        revinfo_policy = _strict_ltv_internal_revo_policy(
            retroactive_revinfo=retroactive
        )

    validation_context_kwargs['revinfo_policy'] = revinfo_policy


async def establish_timestamp_trust(
    tst_signed_data: cms.SignedData,
    validation_context: ValidationContext,
    expected_tst_imprint: bytes,
):
    """
    Wrapper around :func:`validate_tst_signed_data` for use when analysing
    timestamps for the purpose of establishing a timestamp chain.
    Its main purpose is throwing/logging an error if validation fails, since
    that amounts to lack of trust in the purported validation time.

    This is internal API.

    :param tst_signed_data:
        The ``SignedData`` value to validate; must encapsulate a ``TSTInfo``
        value.
    :param validation_context:
        The validation context to apply to the timestamp.
    :param expected_tst_imprint:
        The expected message imprint for the ``TSTInfo`` value.
    :return:
        A :class:`.TimestampSignatureStatus` if validation is successful.
    :raises:
        :class:`SignatureValidationError` if validation fails.
    """
    timestamp_status_kwargs = await validate_tst_signed_data(
        tst_signed_data, validation_context, expected_tst_imprint
    )
    timestamp_status = TimestampSignatureStatus(**timestamp_status_kwargs)

    if not timestamp_status.valid or not timestamp_status.trusted:
        logger.warning(
            "Could not validate embedded timestamp token: %s.",
            timestamp_status.summary(),
        )
        raise SignatureValidationError(
            "Could not establish time of signing, timestamp token did not "
            "validate with current settings."
        )
    return timestamp_status


def get_timestamp_chain(
    reader: PdfFileReader,
) -> Iterator[EmbeddedPdfSignature]:
    """
    Get the document timestamp chain of the associated reader, ordered
    from new to old.

    :param reader:
        A :class:`.PdfFileReader`.
    :return:
        An iterable of
        :class:`~pyhanko.sign.validation.pdf_embedded.EmbeddedPdfSignature`
        objects representing document timestamps.
    """
    return filter(
        lambda sig: sig.sig_object.get('/Type', None) == '/DocTimeStamp',
        reversed(reader.embedded_signatures),
    )


@dataclass
class _TimestampTrustData:
    latest_dts: EmbeddedPdfSignature
    earliest_ts_status: Optional[TimestampSignatureStatus]
    ts_chain_length: int
    current_signature_vc_kwargs: dict


def _instantiate_ltv_vc(
    emb_timestamp: EmbeddedPdfSignature, validation_context_kwargs
):
    try:
        hist_resolver = emb_timestamp.reader.get_historical_resolver(
            emb_timestamp.signed_revision
        )
        dss = DocumentSecurityStore.read_dss(hist_resolver)
        return dss.as_validation_context(validation_context_kwargs)
    except NoDSSFoundError:
        validation_context_kwargs.setdefault('crls', ())
        validation_context_kwargs.setdefault('ocsps', ())
        return ValidationContext(**validation_context_kwargs)


async def _establish_timestamp_trust_lta(
    reader,
    bootstrap_validation_context,
    validation_context_kwargs,
    until_revision,
) -> _TimestampTrustData:
    timestamps = get_timestamp_chain(reader)
    validation_context_kwargs = dict(validation_context_kwargs)
    current_vc = bootstrap_validation_context
    ts_status = None
    ts_count = -1
    emb_timestamp = None
    for ts_count, emb_timestamp in enumerate(timestamps):
        if emb_timestamp.signed_revision < until_revision:
            break

        external_digest = emb_timestamp.compute_digest()
        ts_status = await establish_timestamp_trust(
            emb_timestamp.signed_data, current_vc, external_digest
        )
        # set up the validation kwargs for the next iteration
        _strict_vc_context_kwargs(
            ts_status.timestamp, validation_context_kwargs
        )
        # read the DSS at the current revision into a new
        # validation context object
        current_vc = _instantiate_ltv_vc(
            emb_timestamp, validation_context_kwargs
        )

    return _TimestampTrustData(
        latest_dts=emb_timestamp,
        earliest_ts_status=ts_status,
        ts_chain_length=ts_count + 1,
        current_signature_vc_kwargs=validation_context_kwargs,
    )


# TODO verify formal PAdES requirements for timestamps
# TODO verify other formal PAdES requirements (coverage, etc.)
# TODO signature/verification policy-based validation! (PAdES-EPES-* etc)
#  (this is a different beast, though)
# TODO "tolerant" timestamp validation, where we tolerate problems in the
#  timestamp chain provided that newer timestamps are "strong" enough to
#  cover the gap.
async def async_validate_pdf_ltv_signature(
    embedded_sig: EmbeddedPdfSignature,
    validation_type: RevocationInfoValidationType,
    validation_context_kwargs: Optional[dict] = None,
    bootstrap_validation_context: Optional[ValidationContext] = None,
    ac_validation_context_kwargs=None,
    force_revinfo=False,
    diff_policy: Optional[DiffPolicy] = None,
    key_usage_settings: Optional[KeyUsageConstraints] = None,
    skip_diff: bool = False,
) -> PdfSignatureStatus:
    """
    .. versionadded:: 0.9.0

    Validate a PDF LTV signature according to a particular profile.

    :param embedded_sig:
        Embedded signature to evaluate.
    :param validation_type:
        Validation profile to use.
    :param validation_context_kwargs:
        Keyword args to instantiate
        :class:`.pyhanko_certvalidator.ValidationContext` objects needed over
        the course of the validation.
    :param ac_validation_context_kwargs:
        Keyword arguments for the validation context to use to
        validate attribute certificates.
        If not supplied, no AC validation will be performed.

        .. note::
            :rfc:`5755` requires attribute authority trust roots to be specified
            explicitly; hence why there's no default.
    :param bootstrap_validation_context:
        Validation context used to validate the current timestamp.
    :param force_revinfo:
        Require all certificates encountered to have some form of live
        revocation checking provisions.
    :param diff_policy:
        Policy to evaluate potential incremental updates that were appended
        to the signed revision of the document.
        Defaults to
        :const:`~pyhanko.sign.diff_analysis.DEFAULT_DIFF_POLICY`.
    :param key_usage_settings:
        A :class:`.KeyUsageConstraints` object specifying which key usages
        must or must not be present in the signer's certificate.
    :param skip_diff:
        If ``True``, skip the difference analysis step entirely.
    :return:
        The status of the signature.
    """

    # create a fresh copy of the validation_kwargs
    vc_kwargs: dict = dict(validation_context_kwargs or {})

    # To validate the first timestamp, allow fetching by default
    # we'll turn it off later
    vc_kwargs.setdefault('allow_fetching', True)
    vc_kwargs.setdefault('retroactive_revinfo', False)
    # same for revocation_mode: if force_revinfo is false, we simply turn on
    # hard-fail by default for now. Once the timestamp is validated,
    # we switch to hard-fail forcibly.
    retroactive = vc_kwargs['retroactive_revinfo']
    if force_revinfo:
        vc_kwargs['revinfo_policy'] = _default_ltv_internal_revo_policy(
            retroactive_revinfo=retroactive
        )
        if ac_validation_context_kwargs is not None:
            ac_validation_context_kwargs['revinfo_policy'] = (
                _strict_ltv_internal_revo_policy(
                    retroactive_revinfo=retroactive
                )
            )
    elif 'revocation_mode' not in vc_kwargs:
        vc_kwargs.setdefault(
            'revinfo_policy',
            _default_ltv_internal_revo_policy(retroactive_revinfo=retroactive),
        )
        if ac_validation_context_kwargs is not None:
            ac_validation_context_kwargs.setdefault(
                'revinfo_policy',
                _default_ltv_internal_revo_policy(
                    retroactive_revinfo=retroactive
                ),
            )

    current_vc: ValidationContext
    reader = embedded_sig.reader
    if validation_type == RevocationInfoValidationType.ADOBE_STYLE:
        dss = None
        current_vc = bootstrap_validation_context or ValidationContext(
            **vc_kwargs
        )
    else:
        # If there's a DSS, there's no harm in reading additional certs from it
        dss = DocumentSecurityStore.read_dss(reader)
        if bootstrap_validation_context is None:
            current_vc = dss.as_validation_context(
                vc_kwargs, include_revinfo=False
            )
        else:
            current_vc = bootstrap_validation_context
            # add the certs from the DSS
            current_vc.certificate_registry.register_multiple(dss.load_certs())

    embedded_sig.compute_digest()
    embedded_sig.compute_tst_digest()

    # If the validation profile is PAdES-type, then we validate the timestamp
    #  chain now.
    #  This is bootstrapped using the current validation context.
    #  If successful, we obtain a new validation context set to a new
    #  "known good" verification time. We then repeat the process using this
    #  new validation context instead of the current one.
    # also record the embedded sig object assoc. with the oldest applicable
    # DTS in the timestamp chain
    earliest_good_timestamp_st: Optional[TimestampSignatureStatus] = None
    latest_dts: Optional[EmbeddedPdfSignature] = None
    ts_chain_length = 0
    if validation_type != RevocationInfoValidationType.ADOBE_STYLE:
        ts_trust_data = await _establish_timestamp_trust_lta(
            reader,
            current_vc,
            validation_context_kwargs=vc_kwargs,
            until_revision=embedded_sig.signed_revision,
        )
        ts_chain_length = ts_trust_data.ts_chain_length
        vc_kwargs = ts_trust_data.current_signature_vc_kwargs
        # if we found a timestamp, use it to pass to the next VC iteration
        if ts_trust_data.latest_dts:
            current_vc = _instantiate_ltv_vc(
                ts_trust_data.latest_dts, vc_kwargs
            )
        # In PAdES-LTA, we should only rely on DSS information that is covered
        # by an appropriate document timestamp.
        # If the validation profile is PAdES-LTA, then we must have seen
        # at least one document timestamp pass by, i.e. earliest_known_timestamp
        # must be non-None by now.
        if (
            ts_trust_data.earliest_ts_status is None
            and validation_type == RevocationInfoValidationType.PADES_LTA
        ):
            raise SignatureValidationError(
                "Purported PAdES-LTA signature does not have a timestamp chain."
            )
        # if this assertion fails, there's a bug in the validation code
        assert (
            validation_type == RevocationInfoValidationType.PADES_LT
            or ts_trust_data.ts_chain_length >= 1
        )
        earliest_good_timestamp_st = ts_trust_data.earliest_ts_status
        latest_dts = ts_trust_data.latest_dts

    # now that we have arrived at the revision with the signature,
    # we can check for a timestamp token attribute there
    # (This is allowed, regardless of whether we use Adobe-style LTV or
    # a PAdES validation profile)
    tst_signed_data = embedded_sig.attached_timestamp_data
    if tst_signed_data is not None:
        tst_signature_digest = embedded_sig.tst_signature_digest
        assert tst_signature_digest is not None
        earliest_good_timestamp_st = await establish_timestamp_trust(
            tst_signed_data, current_vc, tst_signature_digest
        )
        assert earliest_good_timestamp_st is not None
        # update validation context moments to the new signature POE
        signature_poe = earliest_good_timestamp_st.timestamp
        vc_kwargs['moment'] = signature_poe
        if ac_validation_context_kwargs is not None:
            ac_validation_context_kwargs['moment'] = signature_poe
    elif (
        validation_type == RevocationInfoValidationType.PADES_LTA
        and ts_chain_length == 1
    ):
        # TODO Pretty sure that this is the spirit of the LTA profile,
        #  but are we being too harsh here? I don't think so, but it's worth
        #  revisiting later
        # For later review: I believe that this check is appropriate, because
        # the timestamp that protects the signature should be verifiable
        # using only information from the next DSS, which should in turn
        # also be protected using a DTS. This requires at least two timestamps.
        raise SignatureValidationError(
            "PAdES-LTA signature requires separate timestamps protecting "
            "the signature & the rest of the revocation info."
        )

    # if, by now, we still don't have a trusted timestamp, there's a problem
    # regardless of the validation profile in use.
    if earliest_good_timestamp_st is None:
        raise SignatureValidationError(
            'LTV signatures require a trusted timestamp.'
        )

    _strict_vc_context_kwargs(earliest_good_timestamp_st.timestamp, vc_kwargs)

    stored_ac_vc = None
    if validation_type == RevocationInfoValidationType.ADOBE_STYLE:
        ocsps, crls = retrieve_adobe_revocation_info(embedded_sig.signer_info)
        vc_kwargs['ocsps'] = ocsps
        vc_kwargs['crls'] = crls
        stored_vc = ValidationContext(**vc_kwargs)
        if ac_validation_context_kwargs is not None:
            ac_validation_context_kwargs['ocsps'] = ocsps
            ac_validation_context_kwargs['crls'] = crls
            stored_ac_vc = ValidationContext(**ac_validation_context_kwargs)
    elif validation_type == RevocationInfoValidationType.PADES_LT:
        # in this case, we don't care about whether the information
        # in the DSS is protected by any timestamps, so just ingest everything
        assert dss is not None
        stored_vc = dss.as_validation_context(vc_kwargs)
        if ac_validation_context_kwargs is not None:
            stored_ac_vc = dss.as_validation_context(
                ac_validation_context_kwargs
            )
    else:
        # in the LTA profile, we should use only DSS information covered
        # by the last relevant timestamp, so the correct VC is encoded
        # by the current validation context kwargs retrieved by the lta helper
        # logic (note that we updated the POE timestamps further up)
        assert latest_dts is not None
        stored_vc = _instantiate_ltv_vc(latest_dts, vc_kwargs)
        if ac_validation_context_kwargs is not None:
            stored_ac_vc = _instantiate_ltv_vc(
                latest_dts, ac_validation_context_kwargs
            )

    # Now, we evaluate the validity of the timestamp guaranteeing the signature
    #  *within* the LTV context.
    #   (i.e. we check whether there's enough revinfo to keep tabs on the
    #   timestamp's validity)
    # If the last timestamp comes from a timestamp token attached to the
    # signature, it should be possible to validate it using only data from the
    # DSS / revocation info store, so validate the timestamp *again*
    # using those settings.

    if (
        tst_signed_data is not None
        or validation_type == RevocationInfoValidationType.PADES_LT
    ):
        if tst_signed_data is not None:
            ts_to_validate = tst_signed_data
        else:
            # we're in the PAdES-LT case with a detached TST now.
            # this should be conceptually equivalent to the above
            # so we run the same check here
            assert latest_dts is not None
            ts_to_validate = latest_dts.signed_data
        ts_status_coro = async_validate_cms_signature(
            ts_to_validate,
            status_cls=TimestampSignatureStatus,
            validation_context=stored_vc,
            status_kwargs={'timestamp': earliest_good_timestamp_st.timestamp},
        )
        timestamp_status: TimestampSignatureStatus = await ts_status_coro
    else:
        # In the LTA case, we don't have to do any further checks, since the
        # _establish_timestamp_trust_lta handled that for us.
        # We can therefore just take earliest_good_timestamp_st at face value.
        timestamp_status = earliest_good_timestamp_st

    embedded_sig.compute_integrity_info(
        diff_policy=diff_policy, skip_diff=skip_diff
    )
    status_kwargs = embedded_sig.summarise_integrity_info()
    status_kwargs.update(
        {
            'signer_reported_dt': earliest_good_timestamp_st.timestamp,
            'timestamp_validity': timestamp_status,
        }
    )
    key_usage_settings = PdfSignatureStatus.default_usage_constraints(
        key_usage_settings
    )
    status_kwargs = await cms_basic_validation(
        embedded_sig.signed_data,
        raw_digest=embedded_sig.external_digest,
        validation_context=stored_vc,
        status_kwargs=status_kwargs,
        key_usage_settings=key_usage_settings,
    )

    report_seed_value_validation(
        embedded_sig, status_kwargs['validation_path'], timestamp_found=True
    )
    if stored_ac_vc is not None:
        stored_ac_vc.certificate_registry.register_multiple(
            embedded_sig.other_embedded_certs
        )
    status_kwargs.update(
        await collect_signer_attr_status(
            sd_attr_certificates=embedded_sig.embedded_attr_certs,
            signer_cert=embedded_sig.signer_cert,
            validation_context=stored_ac_vc,
            sd_signed_attrs=embedded_sig.signer_info['signed_attrs'],
        )
    )

    return PdfSignatureStatus(**status_kwargs)


def retrieve_adobe_revocation_info(signer_info: cms.SignerInfo):
    """
    Retrieve Adobe-style revocation information from a ``SignerInfo`` value,
    if present.

    Internal API.

    :param signer_info:
        A ``SignerInfo`` value.
    :return:
        A tuple of two (potentially empty) lists, containing OCSP
        responses and CRLs, respectively.
    """
    try:
        revinfo: asn1_pdf.RevocationInfoArchival = find_unique_cms_attribute(
            signer_info['signed_attrs'], "adobe_revocation_info_archival"
        )
    except (NonexistentAttributeError, MultivaluedAttributeError) as e:
        raise ValidationInfoReadingError(
            "No revocation info archival attribute found, or multiple present"
        ) from e

    ocsps = list(revinfo['ocsp'] or ())
    crls = list(revinfo['crl'] or ())
    return ocsps, crls


def apply_adobe_revocation_info(
    signer_info: cms.SignerInfo, validation_context_kwargs=None
) -> ValidationContext:
    """
    Read Adobe-style revocation information from a CMS object, and load it
    into a validation context.

    :param signer_info:
        Signer info CMS object.
    :param validation_context_kwargs:
        Extra kwargs to pass to the ``__init__`` function.
    :return:
        A validation context preloaded with the relevant revocation information.
    """
    validation_context_kwargs = validation_context_kwargs or {}
    ocsps, crls = retrieve_adobe_revocation_info(signer_info)
    return ValidationContext(
        ocsps=ocsps, crls=crls, **validation_context_kwargs
    )
