import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Union

from asn1crypto import cms, crl, x509
from asn1crypto.crl import CRLReason
from asn1crypto.keys import PublicKeyInfo
from cryptography.exceptions import InvalidSignature
from pyhanko_certvalidator._state import ValProcState
from pyhanko_certvalidator.authority import (
    Authority,
    AuthorityWithCert,
    TrustAnchor,
)
from pyhanko_certvalidator.context import ValidationContext
from pyhanko_certvalidator.errors import (
    OCSPNoMatchesError,
    OCSPValidationError,
    OCSPValidationIndeterminateError,
    PathValidationError,
    PSSParameterMismatch,
    RevokedError,
)
from pyhanko_certvalidator.path import ValidationPath
from pyhanko_certvalidator.policy_decl import (
    CertRevTrustPolicy,
    RevocationCheckingPolicy,
    RevocationCheckingRule,
)
from pyhanko_certvalidator.registry import (
    CertificateCollection,
    LayeredCertificateStore,
    SimpleCertificateStore,
)
from pyhanko_certvalidator.revinfo._err_gather import Errors
from pyhanko_certvalidator.revinfo.archival import (
    OCSPContainer,
    RevinfoUsabilityRating,
)
from pyhanko_certvalidator.revinfo.manager import RevinfoManager
from pyhanko_certvalidator.util import (
    ConsList,
    extract_ac_issuer_dir_name,
    validate_sig,
)

OCSP_PROVENANCE_ERR = (
    "Unable to verify OCSP response since response signing "
    "certificate could not be validated"
)


def _delegated_ocsp_response_path(
    responder_cert: x509.Certificate, issuer: Authority, ee_path: ValidationPath
):
    if isinstance(issuer, AuthorityWithCert):
        responder_chain = ee_path.truncate_to_and_append(
            issuer.certificate, responder_cert
        )
    else:
        responder_chain = ValidationPath(
            trust_anchor=TrustAnchor(issuer), interm=[], leaf=responder_cert
        )
    return responder_chain


async def _validate_delegated_ocsp_provenance(
    responder_cert: x509.Certificate,
    issuer: Authority,
    validation_context: ValidationContext,
    ee_path: ValidationPath,
    proc_state: ValProcState,
):
    if proc_state.check_path_verif_recursion(responder_cert):
        # we permit this for CRLs for historical reasons, but there's no
        # sane reason why this would make sense for OCSP responders, so
        # throw an error
        raise PathValidationError.from_state(
            "Recursion detected in OCSP responder authorisation check for "
            "responder certificate %s." % responder_cert.subject.human_friendly,
            proc_state,
        )

    from pyhanko_certvalidator.validate import intl_validate_path

    # OCSP responder certs must be issued directly by the CA on behalf of
    # which they act.
    # Moreover, RFC 6960 says that we don't have to accept OCSP responses signed
    # with a different key than the one used to sign subscriber certificates.
    ocsp_ee_name_override = (
        proc_state.describe_cert(never_def=True) + ' OCSP responder'
    )

    if responder_cert.ocsp_no_check_value is not None:
        # we don't have to check the revocation of the OCSP responder,
        # so do a simplified check
        revinfo_policy = CertRevTrustPolicy(
            revocation_checking_policy=RevocationCheckingPolicy(
                ee_certificate_rule=RevocationCheckingRule.NO_CHECK,
                # this one should never trigger
                intermediate_ca_cert_rule=RevocationCheckingRule.NO_CHECK,
            )
        )
        vc = ValidationContext(
            trust_roots=[TrustAnchor(issuer)],
            allow_fetching=False,
            revinfo_policy=revinfo_policy,
            moment=validation_context.moment,
            algorithm_usage_policy=validation_context.algorithm_policy,
            time_tolerance=validation_context.time_tolerance,
        )

        ocsp_trunc_path = ValidationPath(
            trust_anchor=TrustAnchor(issuer), interm=[], leaf=responder_cert
        )
        ocsp_trunc_proc_state = ValProcState(
            cert_path_stack=proc_state.cert_path_stack.cons(ocsp_trunc_path),
            ee_name_override=ocsp_ee_name_override,
        )
        try:
            # verify the truncated path
            await intl_validate_path(
                vc, path=ocsp_trunc_path, proc_state=ocsp_trunc_proc_state
            )
        except PathValidationError as e:
            raise OCSPValidationError(OCSP_PROVENANCE_ERR) from e
        # record validation in the original VC
        # TODO maybe have an (issuer, [verified_responder]) cache?
        #  caching OCSP responder validation results with everything else is
        #  probably somewhat incorrect

        responder_chain = _delegated_ocsp_response_path(
            responder_cert, issuer, ee_path
        )
        validation_context.record_validation(responder_cert, responder_chain)
    else:
        responder_chain = _delegated_ocsp_response_path(
            responder_cert, issuer, ee_path
        )

        ocsp_proc_state = ValProcState(
            cert_path_stack=proc_state.cert_path_stack.cons(responder_chain),
            ee_name_override=ocsp_ee_name_override,
        )
        try:
            await intl_validate_path(
                validation_context,
                path=responder_chain,
                proc_state=ocsp_proc_state,
            )
        except PathValidationError as e:
            raise OCSPValidationError(OCSP_PROVENANCE_ERR) from e


def _ocsp_allowed(responder_cert: x509.Certificate):
    extended_key_usage = responder_cert.extended_key_usage_value
    return (
        extended_key_usage is not None
        and 'ocsp_signing' in extended_key_usage.native
    )


@dataclass
class _OCSPErrs(Errors):
    mismatch_failures: int = 0


def _match_ocsp_certid(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    issuer: Authority,
    ocsp_response: OCSPContainer,
    errs: _OCSPErrs,
) -> bool:
    cert_response = ocsp_response.extract_single_response()
    if cert_response is None:
        errs.mismatch_failures += 1
        return False

    response_cert_id = cert_response['cert_id']

    issuer_hash_algo = response_cert_id['hash_algorithm']['algorithm'].native

    is_pkc = isinstance(cert, x509.Certificate)
    if is_pkc:
        cert_issuer_name_hash = getattr(cert.issuer, issuer_hash_algo)
        cert_serial_number = cert.serial_number
    else:
        iss_name = extract_ac_issuer_dir_name(cert)
        cert_issuer_name_hash = getattr(iss_name, issuer_hash_algo)
        cert_serial_number = cert['ac_info']['serial_number'].native
    cert_issuer_key_hash = getattr(issuer.public_key, issuer_hash_algo)

    key_hash_mismatch = (
        response_cert_id['issuer_key_hash'].native != cert_issuer_key_hash
    )

    name_mismatch = (
        response_cert_id['issuer_name_hash'].native != cert_issuer_name_hash
    )
    serial_mismatch = (
        response_cert_id['serial_number'].native != cert_serial_number
    )

    if (name_mismatch or serial_mismatch) and key_hash_mismatch:
        errs.mismatch_failures += 1
        return False

    if name_mismatch:
        errs.append(
            'OCSP response issuer name hash does not match', ocsp_response
        )
        return False

    if serial_mismatch:
        errs.append(
            'OCSP response certificate serial number does not match',
            ocsp_response,
        )
        return False

    if key_hash_mismatch:
        errs.append(
            'OCSP response issuer key hash does not match', ocsp_response
        )
        return False
    return True


def _identify_responder_cert(
    ocsp_response: OCSPContainer,
    cert_store: CertificateCollection,
    errs: _OCSPErrs,
) -> Optional[x509.Certificate]:
    # To verify the response as legitimate, the responder cert must be located

    # prioritise the certificates included with the response, if there
    # are any
    response = ocsp_response.extract_basic_ocsp_response()
    # should be ensured by successful extraction earlier
    assert response is not None
    if response['certs']:
        cert_store = LayeredCertificateStore(
            [SimpleCertificateStore.from_certs(response['certs']), cert_store]
        )

    tbs_response = response['tbs_response_data']
    if tbs_response['responder_id'].name == 'by_key':
        key_identifier = tbs_response['responder_id'].native
        responder_cert = cert_store.retrieve_by_key_identifier(key_identifier)
    else:
        candidate_responder_certs = cert_store.retrieve_by_name(
            tbs_response['responder_id'].chosen
        )
        responder_cert = (
            candidate_responder_certs[0] if candidate_responder_certs else None
        )
    if not responder_cert:
        errs.append(
            "Unable to verify OCSP response since response signing "
            "certificate could not be located",
            ocsp_response,
        )
    return responder_cert


def _precheck_ocsp_responder_auth(
    responder_cert: x509.Certificate, issuer: Authority, is_pkc: bool
) -> Optional[bool]:
    """
    This function checks OCSP conditions that don't require path validation
    to pass. If ``None`` is returned, path validation is necessary to proceed.
    """

    # If the cert signing the OCSP response is not the issuer, it must be
    # issued by the cert issuer and be valid for OCSP responses.
    # We currently do _not_ allow naked trust anchor keys to be used in OCSP
    # validation (but that may change in the future). This decision is based on
    # a conservative reading of RFC 6960.
    # First, check whether the certs are the same.
    if (
        isinstance(issuer, AuthorityWithCert)
        and issuer.certificate.issuer_serial == responder_cert.issuer_serial
    ):
        issuer_cert = issuer.certificate
        # let's check whether the certs are actually the same
        # (by comparing the signatures as a proxy)
        # -> literal interpretation of 4.2.2.2 in RFC 6960
        issuer_sig = bytes(issuer_cert['signature_value'])
        responder_sig = bytes(responder_cert['signature_value'])
        return issuer_sig == responder_sig
    # If OCSP is being delegated
    # check whether the relevant OCSP-related extensions are present.
    # Also, explicitly disallow delegation for attribute authorities
    # since they cannot act as CAs and hence can't issue responder certificates.
    # This would otherwise be detected during path validation or while checking
    # the basicConstraints on the AA certificate, but this is more explicit.
    elif not _ocsp_allowed(responder_cert) or not is_pkc:
        return False
    return None


async def _check_ocsp_authorisation(
    responder_cert: x509.Certificate,
    issuer: Authority,
    cert_path: ValidationPath,
    ocsp_response: OCSPContainer,
    validation_context: ValidationContext,
    is_pkc: bool,
    errs: _OCSPErrs,
    proc_state: ValProcState,
) -> bool:
    simple_check = _precheck_ocsp_responder_auth(responder_cert, issuer, is_pkc)

    # we can take an early out in this case
    if simple_check is not None:
        auth_ok = simple_check
    else:
        try:
            await _validate_delegated_ocsp_provenance(
                responder_cert=responder_cert,
                issuer=issuer,
                validation_context=validation_context,
                ee_path=cert_path,
                proc_state=proc_state,
            )
            auth_ok = True
        except OCSPValidationError as e:
            errs.append(e.args[0], ocsp_response)
            auth_ok = False
    if not auth_ok:
        errs.append(
            'Unable to verify OCSP response since response was '
            'signed by an unauthorized certificate',
            ocsp_response,
        )
    return auth_ok


def _check_ocsp_status(
    ocsp_response: OCSPContainer,
    proc_state: ValProcState,
    control_time: Optional[datetime],
) -> bool:
    cert_response = ocsp_response.extract_single_response()
    if cert_response is None:
        return False

    # Finally check to see if the certificate has been revoked
    status = cert_response['cert_status'].name
    if status == 'good':
        return True

    if status == 'revoked':
        revocation_info = cert_response['cert_status'].chosen
        reason: CRLReason = revocation_info['revocation_reason']
        if reason.native is None:
            reason = crl.CRLReason('unspecified')
        revocation_dt: datetime = revocation_info['revocation_time'].native

        if control_time is None or revocation_dt <= control_time:
            raise RevokedError.format(
                reason=reason,
                revocation_dt=revocation_dt,
                revinfo_type='OCSP response',
                proc_state=proc_state,
            )
    return False


def _verify_ocsp_signature(
    responder_key: PublicKeyInfo, ocsp_response: OCSPContainer, errs: _OCSPErrs
) -> bool:
    response = ocsp_response.extract_basic_ocsp_response()
    if response is None:
        return False

    # Verify that the response was properly signed by the validated certificate
    tbs_response = response['tbs_response_data']
    try:
        validate_sig(
            signature=response['signature'].native,
            signed_data=tbs_response.dump(),
            signed_digest_algorithm=response['signature_algorithm'],
            public_key_info=responder_key,
            parameters=response['signature_algorithm']['parameters'],
        )
        return True
    except PSSParameterMismatch:
        errs.append(
            'The signature parameters on the OCSP response do not match '
            'the constraints on the public key',
            ocsp_response,
        )
    except InvalidSignature:
        errs.append('Unable to verify OCSP response signature', ocsp_response)
    return False


def _assess_ocsp_relevance(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    issuer: Authority,
    ocsp_response: OCSPContainer,
    cert_store: CertificateCollection,
    errs: _OCSPErrs,
) -> Optional[x509.Certificate]:
    matched = _match_ocsp_certid(
        cert, issuer=issuer, ocsp_response=ocsp_response, errs=errs
    )
    if not matched:
        return None

    responder_cert = _identify_responder_cert(
        ocsp_response, cert_store=cert_store, errs=errs
    )
    if not responder_cert:
        return None

    signature_ok = _verify_ocsp_signature(
        responder_key=responder_cert.public_key,
        ocsp_response=ocsp_response,
        errs=errs,
    )
    if not signature_ok:
        return None
    return responder_cert


async def _handle_single_ocsp_resp(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    issuer: Authority,
    path: ValidationPath,
    ocsp_response: OCSPContainer,
    validation_context: ValidationContext,
    errs: _OCSPErrs,
    proc_state: ValProcState,
) -> bool:
    responder_cert = _assess_ocsp_relevance(
        cert=cert,
        issuer=issuer,
        ocsp_response=ocsp_response,
        cert_store=validation_context.certificate_registry,
        errs=errs,
    )
    if responder_cert is None:
        return False

    freshness_result = ocsp_response.usable_at(
        policy=validation_context.revinfo_policy,
        timing_params=validation_context.timing_params,
    )
    rating = freshness_result.rating
    if rating != RevinfoUsabilityRating.OK:
        if rating == RevinfoUsabilityRating.STALE:
            msg = 'OCSP response is not recent enough'
            errs.update_stale(freshness_result.last_usable_at)
        elif rating == RevinfoUsabilityRating.TOO_NEW:
            msg = 'OCSP response is too recent'
        else:
            msg = 'OCSP response freshness could not be established'
        errs.append(msg, ocsp_response, is_freshness_failure=True)
        return False

    # check whether the responder cert is authorised
    authorised = await _check_ocsp_authorisation(
        responder_cert,
        issuer=issuer,
        cert_path=path,
        ocsp_response=ocsp_response,
        validation_context=validation_context,
        is_pkc=isinstance(cert, x509.Certificate),
        errs=errs,
        proc_state=proc_state,
    )
    if not authorised:
        return False

    timing = validation_context.timing_params
    control_time = (
        timing.validation_time if timing.point_in_time_validation else None
    )
    return _check_ocsp_status(ocsp_response, proc_state, control_time)


async def verify_ocsp_response(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    path: ValidationPath,
    validation_context: ValidationContext,
    proc_state: Optional[ValProcState] = None,
):
    """
    Verifies an OCSP response, checking to make sure the certificate has not
    been revoked. Fulfills the requirements of
    https://tools.ietf.org/html/rfc6960#section-3.2.

    :param cert:
        An asn1cyrpto.x509.Certificate object or
        an asn1crypto.cms.AttributeCertificateV2 object to verify the OCSP
        response for

    :param path:
        A pyhanko_certvalidator.path.ValidationPath object of the cert's
        validation path, or in the case of an AC, the AA's validation path.

    :param validation_context:
        A pyhanko_certvalidator.context.ValidationContext object to use for
        caching validation information

    :param proc_state:
        Internal state for error reporting and policy application decisions.

    :raises:
        pyhanko_certvalidator.errors.OCSPNoMatchesError - when none of the OCSP responses match the certificate
        pyhanko_certvalidator.errors.OCSPValidationIndeterminateError - when the OCSP response could not be verified
        pyhanko_certvalidator.errors.RevokedError - when the OCSP response indicates the certificate has been revoked
    """

    proc_state = proc_state or ValProcState(cert_path_stack=ConsList.sing(path))

    cert_description = proc_state.describe_cert()

    try:
        cert_issuer = path.find_issuing_authority(cert)
    except LookupError:
        raise OCSPNoMatchesError(
            'Could not determine issuer certificate for %s in path.',
            proc_state.describe_cert(),
        )

    errs = _OCSPErrs()
    ocsp_responses = (
        await validation_context.revinfo_manager.async_retrieve_ocsps(
            cert, cert_issuer
        )
    )

    for ocsp_response in ocsp_responses:
        try:
            ocsp_good = await _handle_single_ocsp_resp(
                cert=cert,
                issuer=cert_issuer,
                path=path,
                ocsp_response=ocsp_response,
                validation_context=validation_context,
                errs=errs,
                proc_state=proc_state,
            )
            if ocsp_good:
                return
        except ValueError as e:
            msg = "Generic processing error while validating OCSP response."
            logging.debug(msg, exc_info=e)
            errs.append(msg, ocsp_response)

    if errs.mismatch_failures == len(ocsp_responses):
        raise OCSPNoMatchesError(
            f"No OCSP responses were issued for {cert_description}."
        )

    raise OCSPValidationIndeterminateError(
        f"Unable to determine if {cert_description} "
        f"is revoked due to insufficient information from OCSP responses.",
        failures=errs.failures,
        suspect_stale=(
            errs.stale_last_usable_at if errs.freshness_failures_only else None
        ),
    )


@dataclass(frozen=True)
class OCSPResponseOfInterest:
    ocsp_response: OCSPContainer
    prov_path: ValidationPath


@dataclass(frozen=True)
class OCSPCollectionResult:
    """
    The result of an OCSP collection operation for AdES point-in-time
    validation purposes.
    """

    responses: List[OCSPResponseOfInterest]
    """
    List of potentially relevant OCSP responses.
    """

    failure_msgs: List[str]
    """
    List of failure messages, for error reporting purposes.
    """


async def collect_relevant_responses_with_paths(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    path: ValidationPath,
    revinfo_manager: RevinfoManager,
    control_time: datetime,
    proc_state: Optional[ValProcState] = None,
) -> OCSPCollectionResult:
    """
    Collect potentially relevant OCSP responses with the associated validation
    paths. Will not perform actual path validation.

    :param cert:
        The certificate under scrutiny.
    :param path:
        The path currently being evaluated.
    :param revinfo_manager:
        The revocation info manager.
    :param control_time:
        The control time before which the validation info should have been
        issued.
    :param proc_state:
        The state of any prior validation process.
    :return:
        A :class:`.OCSPCollectionResult`.
    """

    proc_state = proc_state or ValProcState(cert_path_stack=ConsList.sing(path))
    try:
        cert_issuer_auth = path.find_issuing_authority(cert)
    except LookupError:
        raise OCSPNoMatchesError(
            f"Could not determine issuer certificate "
            f"for {proc_state.describe_cert()} in path."
        )

    relevant = []

    ocsp_responses = await revinfo_manager.async_retrieve_ocsps(
        cert, cert_issuer_auth
    )

    poe_manager = revinfo_manager.poe_manager
    errs = _OCSPErrs()
    for ocsp_response_cont in ocsp_responses:
        issued = ocsp_response_cont.issuance_date
        if (
            issued is None
            or issued > control_time
            or poe_manager[ocsp_response_cont] > control_time
        ):
            # We don't care about responses issued after control_time
            continue
        try:
            responder_cert = _assess_ocsp_relevance(
                cert=cert,
                issuer=cert_issuer_auth,
                ocsp_response=ocsp_response_cont,
                cert_store=revinfo_manager.certificate_registry,
                errs=errs,
            )
            if responder_cert is None:
                continue
            path = _delegated_ocsp_response_path(
                responder_cert, cert_issuer_auth, ee_path=path
            )
            result = OCSPResponseOfInterest(
                ocsp_response=ocsp_response_cont, prov_path=path
            )
            relevant.append(result)
        except ValueError as e:
            msg = "Generic processing error while validating OCSP response."
            logging.debug(msg, exc_info=e)
            errs.append(msg, ocsp_response_cont)
    return OCSPCollectionResult(
        responses=relevant,
        failure_msgs=[f[0] for f in errs.failures],
    )
