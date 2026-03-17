import hashlib
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union

from asn1crypto import cms, crl, x509
from asn1crypto.crl import CRLEntryExtensionId
from cryptography.exceptions import InvalidSignature
from pyhanko_certvalidator._state import ValProcState
from pyhanko_certvalidator.authority import Authority, AuthorityWithCert
from pyhanko_certvalidator.context import ValidationContext
from pyhanko_certvalidator.errors import (
    CertificateFetchError,
    CRLNoMatchesError,
    CRLValidationError,
    CRLValidationIndeterminateError,
    PathValidationError,
    PSSParameterMismatch,
    RevokedError,
)
from pyhanko_certvalidator.ltv.poe import POEManager
from pyhanko_certvalidator.ltv.types import ValidationTimingParams
from pyhanko_certvalidator.path import ValidationPath
from pyhanko_certvalidator.policy_decl import CertRevTrustPolicy
from pyhanko_certvalidator.registry import CertificateRegistry
from pyhanko_certvalidator.revinfo._err_gather import Errors
from pyhanko_certvalidator.revinfo.archival import (
    CRLContainer,
    RevinfoUsabilityRating,
)
from pyhanko_certvalidator.revinfo.constants import (
    KNOWN_CRL_ENTRY_EXTENSIONS,
    KNOWN_CRL_EXTENSIONS,
    VALID_REVOCATION_REASONS,
)
from pyhanko_certvalidator.revinfo.manager import RevinfoManager
from pyhanko_certvalidator.util import (
    ConsList,
    get_ac_extension_value,
    get_issuer_dn,
    validate_sig,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CRLWithPaths:
    """
    A CRL with a number of candidate paths
    """

    crl: CRLContainer
    paths: List[ValidationPath]


async def _find_candidate_crl_issuer_certs(
    crl_authority_name: x509.Name,
    certificate_list: crl.CertificateList,
    *,
    cert_issuer_auth: Authority,
    cert_registry: CertificateRegistry,
) -> List[x509.Certificate]:
    # first, look for certs issued to the issuer named as the entity
    # that signed the CRL.
    # In both cases, we prioritise the next-level issuer in the main path
    # if it matches the criteria.
    delegated_issuer = certificate_list.issuer
    cert_issuer_cert = None
    if isinstance(cert_issuer_auth, AuthorityWithCert):
        cert_issuer_cert = cert_issuer_auth.certificate
    candidates = cert_registry.retrieve_by_name(
        delegated_issuer, cert_issuer_cert
    )
    if not candidates and crl_authority_name != certificate_list.issuer:
        # next, look in the cache for certs issued to the entity named
        # in the issuing distribution point (i.e. the issuing authority)
        candidates = cert_registry.retrieve_by_name(
            crl_authority_name, cert_issuer_cert
        )
    if not candidates and cert_registry.fetcher is not None:
        candidates = []
        valid_names = (crl_authority_name, delegated_issuer)
        # Try to download certificates from URLs in the AIA extension,
        # if there is one
        async for cert in cert_registry.fetcher.fetch_crl_issuers(
            certificate_list
        ):
            # filter by name
            if cert.subject in valid_names:
                candidates.insert(0, cert)
    return candidates


@dataclass
class _CRLIssuerSearchErrs:
    candidate_issuers: int
    candidates_skipped: int = 0
    signatures_failed: int = 0
    unauthorized_certs: int = 0
    path_building_failures: int = 0
    explicit_errors: List[CRLValidationError] = field(default_factory=list)

    def get_exc(self):
        plural = self.candidate_issuers > 1
        if (
            not self.candidate_issuers
            or self.candidates_skipped == self.candidate_issuers
        ):
            return CRLNoMatchesError()
        elif self.signatures_failed == self.candidate_issuers:
            return CRLValidationError('CRL signature could not be verified')
        elif self.unauthorized_certs == self.candidate_issuers:
            return CRLValidationError(
                'The CRL issuers that were identified are not authorized '
                'to sign CRLs'
                if plural
                else 'The CRL issuer that was identified is '
                'not authorized to sign CRLs'
            )
        elif self.path_building_failures == self.candidate_issuers:
            return CRLValidationError(
                'The chain of trust for the CRL issuers that were identified '
                'could not be determined'
                if plural
                else 'The chain of trust for the CRL issuer that was identified '
                'could not be determined'
            )
        elif self.explicit_errors and len(self.explicit_errors) == 1:
            # if there's only one error, throw it
            return self.explicit_errors[0]
        else:
            msg = 'Unable to determine CRL trust status. '
            msg += '; '.join(str(e) for e in self.explicit_errors)
            return CRLValidationError(msg)


async def _validate_crl_issuer_path(
    *,
    candidate_crl_issuer_path: ValidationPath,
    validation_context: ValidationContext,
    issuing_authority_identical: bool,
    proc_state: ValProcState,
):
    # If we have a validation cached (from before, or because the CRL issuer
    #  appears further up in the path) use it.
    # This is not just for efficiency, it also makes for clearer errors when
    #  validation fails due to revocation info issues further up in the path
    if validation_context.check_validation(candidate_crl_issuer_path.last):
        return
    try:
        temp_override = proc_state.ee_name_override
        if not issuing_authority_identical:
            temp_override = (
                proc_state.describe_cert(never_def=True) + ' CRL issuer'
            )
        from pyhanko_certvalidator.validate import intl_validate_path

        new_stack = proc_state.cert_path_stack.cons(candidate_crl_issuer_path)
        await intl_validate_path(
            validation_context,
            candidate_crl_issuer_path,
            proc_state=ValProcState(
                ee_name_override=temp_override, cert_path_stack=new_stack
            ),
        )

    except PathValidationError as e:
        iss_cert = candidate_crl_issuer_path.last
        logger.warning(
            f"Path for CRL issuer {iss_cert.subject.human_friendly} could not "
            f"be validated.",
            exc_info=e,
        )
        raise CRLValidationError(
            f'The CRL issuer certificate path could not be validated. {e}'
        )


async def _find_candidate_crl_paths(
    crl_authority_name: x509.Name,
    certificate_list: crl.CertificateList,
    *,
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    cert_issuer_auth: Authority,
    cert_path: ValidationPath,
    certificate_registry: CertificateRegistry,
    is_indirect: bool,
    proc_state: ValProcState,
) -> Tuple[List[ValidationPath], _CRLIssuerSearchErrs]:
    cert_sha256 = hashlib.sha256(cert.dump()).digest()

    candidate_crl_issuers = await _find_candidate_crl_issuer_certs(
        crl_authority_name,
        certificate_list,
        cert_issuer_auth=cert_issuer_auth,
        cert_registry=certificate_registry,
    )
    cert_issuer_name = cert_issuer_auth.name

    errs = _CRLIssuerSearchErrs(candidate_issuers=len(candidate_crl_issuers))
    candidate_paths = []
    for candidate_crl_issuer in candidate_crl_issuers:
        direct_issuer = candidate_crl_issuer.subject == cert_issuer_name

        # In some cases an indirect CRL issuer is a certificate issued
        # by the certificate issuer. However, we need to ensure that
        # the candidate CRL issuer is not the certificate being checked,
        # otherwise we may be checking an incorrect CRL and produce
        # incorrect results.
        indirect_issuer = (
            candidate_crl_issuer.issuer == cert_issuer_name
            and candidate_crl_issuer.sha256 != cert_sha256
        )

        if not direct_issuer and not indirect_issuer and not is_indirect:
            errs.candidates_skipped += 1
            continue

        key_usage_value = candidate_crl_issuer.key_usage_value
        if key_usage_value and 'crl_sign' not in key_usage_value.native:
            errs.unauthorized_certs += 1
            continue

        try:
            # Step g
            # NOTE: Theoretically this can only be done after full X.509
            # path validation (step f), but that only matters for DSA key
            # inheritance which we don't support anyhow when doing revocation
            # checks.
            _verify_crl_signature(
                certificate_list, candidate_crl_issuer.public_key
            )
        except CRLValidationError:
            errs.signatures_failed += 1
            continue

        cand_path = proc_state.check_path_verif_recursion(candidate_crl_issuer)
        if not cand_path:
            try:
                cand_path = cert_path.truncate_to_issuer_and_append(
                    candidate_crl_issuer
                )
            except LookupError:
                errs.path_building_failures += 1
                continue
        candidate_paths.append(cand_path)
    return candidate_paths, errs


async def _find_crl_issuer(
    crl_authority_name: x509.Name,
    certificate_list: crl.CertificateList,
    *,
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    cert_issuer_auth: Authority,
    cert_path: ValidationPath,
    validation_context: ValidationContext,
    is_indirect: bool,
    proc_state: ValProcState,
) -> ValidationPath:
    candidate_paths, errs = await _find_candidate_crl_paths(
        crl_authority_name,
        certificate_list,
        cert=cert,
        cert_issuer_auth=cert_issuer_auth,
        cert_path=cert_path,
        certificate_registry=validation_context.certificate_registry,
        is_indirect=is_indirect,
        proc_state=proc_state,
    )

    for candidate_crl_issuer_path in candidate_paths:
        candidate_crl_issuer = candidate_crl_issuer_path.last

        # Skip path validation step if we're recursing
        #  (necessary to process CRLs that have their own certificate in-scope,
        #   which is questionable practice, but PKITS has a test case for this
        #   specific wrinkle, and it's not contradicted by anything in RFC 5280,
        #   so it's probably allowed in theory)
        if proc_state.check_path_verif_recursion(candidate_crl_issuer):
            validation_context.revinfo_manager.record_crl_issuer(
                certificate_list, candidate_crl_issuer
            )
            return candidate_crl_issuer_path
        # Step f
        # Note: this is not the same as .truncate_to() if
        # candidate_crl_issuer doesn't appear in the path!
        candidate_crl_issuer_path = cert_path.truncate_to_issuer_and_append(
            candidate_crl_issuer
        )
        try:
            # This check needs to know not only whether the names agree,
            # but also whether the keys are the same, in order to yield
            # the correct error message on failure.
            # (Scenario: CA with separate keys for CRL signing and for
            # certificate issuance, but with the same name on both certs)
            issuing_authority_identical = not is_indirect and (
                cert_issuer_auth is not None
                and cert_issuer_auth.public_key.dump()
                == candidate_crl_issuer.public_key.dump()
            )
            await _validate_crl_issuer_path(
                candidate_crl_issuer_path=candidate_crl_issuer_path,
                validation_context=validation_context,
                issuing_authority_identical=issuing_authority_identical,
                proc_state=proc_state,
            )
            validation_context.revinfo_manager.record_crl_issuer(
                certificate_list, candidate_crl_issuer
            )
            return candidate_crl_issuer_path
        except CRLValidationError as e:
            errs.explicit_errors.append(e)
            continue
    raise errs.get_exc()


@dataclass
class _CRLErrs(Errors):
    issuer_failures: int = 0


def _find_matching_delta_crl(
    delta_lists: List[CRLContainer],
    crl_authority_name: x509.Name,
    crl_idp: crl.IssuingDistributionPoint,
    parent_crl_aki: Optional[bytes],
) -> Optional[CRLContainer]:
    for candidate_delta_cl_cont in delta_lists:
        candidate_delta_cl = candidate_delta_cl_cont.crl_data
        # Step c 1
        if candidate_delta_cl.issuer != crl_authority_name:
            continue

        # Step c 2
        delta_crl_idp = candidate_delta_cl.issuing_distribution_point_value
        if (crl_idp is None and delta_crl_idp is not None) or (
            crl_idp is not None and delta_crl_idp is None
        ):
            continue

        if crl_idp is not None and crl_idp.native != delta_crl_idp.native:
            continue

        # Step c 3
        if parent_crl_aki != candidate_delta_cl.authority_key_identifier:
            continue

        return candidate_delta_cl_cont
    return None


def _match_dps_idp_names(
    crl_idp: crl.IssuingDistributionPoint,
    crl_dps: Optional[x509.CRLDistributionPoints],
    crl_issuer: x509.Certificate,
    crl_authority_name: x509.Name,
) -> bool:
    # Step b 2 i
    has_idp_name = False
    has_dp_name = False
    idp_dp_match = False

    idp_general_names = []
    idp_dp_name = crl_idp['distribution_point']
    if idp_dp_name:
        has_idp_name = True
        if idp_dp_name.name == 'full_name':
            for general_name in idp_dp_name.chosen:
                idp_general_names.append(general_name)
        else:
            inner_extended_issuer_name = crl_issuer.subject.copy()
            inner_extended_issuer_name.chosen.append(idp_dp_name.chosen.untag())
            idp_general_names.append(
                x509.GeneralName(
                    name='directory_name', value=inner_extended_issuer_name
                )
            )

    if crl_dps:
        for dp in crl_dps:
            if idp_dp_match:
                break
            dp_name = dp['distribution_point']
            if dp_name:
                has_dp_name = True
                if dp_name.name == 'full_name':
                    for general_name in dp_name.chosen:
                        if general_name in idp_general_names:
                            idp_dp_match = True
                            break
                else:
                    inner_extended_issuer_name = crl_issuer.subject.copy()
                    inner_extended_issuer_name.chosen.append(
                        dp_name.chosen.untag()
                    )
                    dp_extended_issuer_name = x509.GeneralName(
                        name='directory_name', value=inner_extended_issuer_name
                    )

                    if dp_extended_issuer_name in idp_general_names:
                        idp_dp_match = True

            elif dp['crl_issuer']:
                has_dp_name = True
                for dp_crl_authority_name in dp['crl_issuer']:
                    if dp_crl_authority_name in idp_general_names:
                        idp_dp_match = True
                        break
    else:
        # If there is no DP, we consider the CRL issuer name to be it
        has_dp_name = True
        general_name = x509.GeneralName(
            name='directory_name', value=crl_authority_name
        )
        if general_name in idp_general_names:
            idp_dp_match = True

    return idp_dp_match or not has_idp_name or not has_dp_name


def _handle_crl_idp_ext_constraints(
    cert: x509.Certificate,
    certificate_list: crl.CertificateList,
    crl_issuer: x509.Certificate,
    crl_idp: crl.IssuingDistributionPoint,
    crl_authority_name: x509.Name,
    errs: _CRLErrs,
) -> bool:
    match = _match_dps_idp_names(
        crl_idp=crl_idp,
        crl_dps=cert.crl_distribution_points_value,
        crl_issuer=crl_issuer,
        crl_authority_name=crl_authority_name,
    )
    if not match:
        errs.append(
            "The CRL issuing distribution point extension does not "
            "share any names with the certificate CRL distribution "
            "point extension",
            certificate_list,
        )
        errs.issuer_failures += 1
        return False

    # Step b 2 ii
    if crl_idp['only_contains_user_certs'].native:
        if (
            cert.basic_constraints_value
            and cert.basic_constraints_value['ca'].native
        ):
            errs.append(
                "CRL only contains end-entity certificates and "
                "certificate is a CA certificate",
                certificate_list,
            )
            return False

    # Step b 2 iii
    if crl_idp['only_contains_ca_certs'].native:
        if (
            not cert.basic_constraints_value
            or cert.basic_constraints_value['ca'].native is False
        ):
            errs.append(
                "CRL only contains CA certificates and certificate "
                "is an end-entity certificate",
                certificate_list,
            )
            return False

    # Step b 2 iv
    if crl_idp['only_contains_attribute_certs'].native:
        errs.append(
            'CRL only contains attribute certificates', certificate_list
        )
        return False

    return True


def _handle_attr_cert_crl_idp_ext_constraints(
    certificate_list: crl.CertificateList,
    crl_dps: Optional[x509.CRLDistributionPoints],
    crl_issuer: x509.Certificate,
    crl_idp: crl.IssuingDistributionPoint,
    crl_authority_name: x509.Name,
    errs: _CRLErrs,
) -> bool:
    match = _match_dps_idp_names(
        crl_idp=crl_idp,
        crl_dps=crl_dps,
        crl_issuer=crl_issuer,
        crl_authority_name=crl_authority_name,
    )
    if not match:
        errs.append(
            "The CRL issuing distribution point extension does not "
            "share any names with the attribute certificate's "
            "CRL distribution point extension",
            certificate_list,
        )
        errs.issuer_failures += 1
        return False

    # Step b 2 ii
    pkc_only = (
        crl_idp['only_contains_user_certs'].native
        or crl_idp['only_contains_ca_certs'].native
    )
    if pkc_only:
        errs.append(
            "CRL only contains public-key certificates, but "
            "certificate is an attribute certificate",
            certificate_list,
        )
        return False

    return True


def _check_crl_freshness(
    certificate_list_cont: CRLContainer,
    revinfo_policy: CertRevTrustPolicy,
    timing_params: ValidationTimingParams,
    errs: _CRLErrs,
    is_delta: bool,
):
    freshness_result = certificate_list_cont.usable_at(
        policy=revinfo_policy,
        timing_params=timing_params,
    )
    prefix = "Delta CRL" if is_delta else "CRL"
    rating = freshness_result.rating
    if rating != RevinfoUsabilityRating.OK:
        if rating == RevinfoUsabilityRating.STALE:
            msg = f'{prefix} is not recent enough'
            errs.update_stale(freshness_result.last_usable_at)
        elif rating == RevinfoUsabilityRating.TOO_NEW:
            msg = f'{prefix} is too recent'
        else:
            msg = f'{prefix} freshness could not be established'
        errs.append(msg, certificate_list_cont, is_freshness_failure=True)
        return False
    return True


async def _handle_single_crl(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    cert_issuer_auth: Authority,
    certificate_list_cont: CRLContainer,
    path: ValidationPath,
    validation_context: ValidationContext,
    delta_lists_by_issuer: Dict[str, List[CRLContainer]],
    use_deltas: bool,
    errs: _CRLErrs,
    proc_state: ValProcState,
) -> Optional[Set[str]]:
    certificate_list = certificate_list_cont.crl_data

    try:
        is_indirect, crl_authority_name = _get_crl_authority_name(
            certificate_list_cont,
            cert_issuer_auth.name,
            certificate_registry=validation_context.certificate_registry,
            errs=errs,
        )
    except LookupError:
        # already logged by _get_crl_authority_name
        return None

    # check if we already know the issuer of this CRL
    crl_issuer = validation_context.revinfo_manager.check_crl_issuer(
        certificate_list
    )
    # if not, attempt to determine it
    if not crl_issuer:
        try:
            crl_issuer_path = await _find_crl_issuer(
                crl_authority_name,
                certificate_list,
                cert=cert,
                cert_issuer_auth=cert_issuer_auth,
                cert_path=path,
                validation_context=validation_context,
                is_indirect=is_indirect,
                proc_state=proc_state,
            )
            crl_issuer = crl_issuer_path.last
        except CRLNoMatchesError:
            # this no-match issue will be dealt with at a higher level later
            errs.issuer_failures += 1
            return None
        except (CertificateFetchError, CRLValidationError) as e:
            errs.append(e.args[0], certificate_list)
            return None

    interim_reasons = _get_crl_scope_assuming_authority(
        crl_issuer=crl_issuer,
        cert=cert,
        certificate_list_cont=certificate_list_cont,
        is_indirect=is_indirect,
        errs=errs,
    )

    if interim_reasons is None:
        return None

    if not _check_crl_freshness(
        certificate_list_cont,
        validation_context.revinfo_policy,
        validation_context.timing_params,
        errs,
        is_delta=False,
    ):
        return None

    # Step c
    if use_deltas:
        delta_certificate_list_cont = _maybe_get_delta_crl(
            certificate_list=certificate_list,
            crl_issuer=crl_issuer,
            policy=validation_context.revinfo_policy,
            timing_params=validation_context.timing_params,
            delta_lists_by_issuer=delta_lists_by_issuer,
            errs=errs,
        )
    else:
        delta_certificate_list_cont = None

    try:
        revoked_date, revoked_reason = _check_cert_on_crl_and_delta(
            crl_issuer=crl_issuer,
            cert=cert,
            certificate_list_cont=certificate_list_cont,
            delta_certificate_list_cont=delta_certificate_list_cont,
            errs=errs,
        )
    except NotImplementedError:
        # the subroutine already registered the failure, so just bail
        return None

    timing = validation_context.timing_params
    control_time = (
        timing.validation_time if timing.point_in_time_validation else None
    )
    if revoked_reason and (control_time is None or revoked_date < control_time):
        raise RevokedError.format(
            reason=revoked_reason,
            revocation_dt=revoked_date,
            revinfo_type='CRL',
            proc_state=proc_state,
        )
    return interim_reasons


def _get_crl_authority_name(
    certificate_list_cont: CRLContainer,
    cert_issuer_name: x509.Name,
    certificate_registry: CertificateRegistry,
    errs: _CRLErrs,
) -> Tuple[bool, x509.Name]:
    """
    Figure out the name of the entity on behalf of which the CRL was issued.
    """

    certificate_list = certificate_list_cont.crl_data

    crl_idp: crl.IssuingDistributionPoint = (
        certificate_list.issuing_distribution_point_value
    )
    is_indirect = bool(crl_idp and crl_idp['indirect_crl'].native)
    if not is_indirect:
        crl_authority_name = certificate_list.issuer
    else:
        crl_idp_name = crl_idp['distribution_point']
        if crl_idp_name:
            if crl_idp_name.name == 'full_name':
                crl_authority_name = crl_idp_name.chosen[0].chosen
            else:
                crl_authority_name = cert_issuer_name.copy().chosen.append(
                    crl_idp_name.chosen
                )
        elif certificate_list.authority_key_identifier:
            tmp_crl_issuer = certificate_registry.retrieve_by_key_identifier(
                certificate_list.authority_key_identifier
            )
            crl_authority_name = tmp_crl_issuer.subject
        else:
            errs.append(
                'CRL is marked as an indirect CRL, but provides no '
                'mechanism for locating the CRL issuer certificate',
                certificate_list_cont,
            )
            raise LookupError
    return is_indirect, crl_authority_name


def _maybe_get_delta_crl(
    certificate_list: crl.CertificateList,
    crl_issuer: x509.Certificate,
    delta_lists_by_issuer: Dict[str, List[CRLContainer]],
    errs: _CRLErrs,
    timing_params: Optional[ValidationTimingParams] = None,
    policy: Optional[CertRevTrustPolicy] = None,
) -> Optional[CRLContainer]:
    if (
        not certificate_list.freshest_crl_value
        or len(certificate_list.freshest_crl_value) == 0
    ):
        # nothing to do, return
        return None

    crl_authority_name = crl_issuer.subject
    crl_idp: crl.IssuingDistributionPoint = (
        certificate_list.issuing_distribution_point_value
    )

    candidate_delta_lists = delta_lists_by_issuer.get(
        crl_authority_name.hashable, []
    )
    delta_certificate_list_cont = _find_matching_delta_crl(
        delta_lists=candidate_delta_lists,
        crl_authority_name=crl_authority_name,
        crl_idp=crl_idp,
        parent_crl_aki=certificate_list.authority_key_identifier,
    )
    if not delta_certificate_list_cont:
        raise CRLValidationIndeterminateError(
            "Delta CRL matching Freshest CRL extension not available",
            failures=[],
            suspect_stale=None,
        )

    delta_certificate_list = delta_certificate_list_cont.crl_data

    if not _verify_no_unknown_critical_extensions(
        delta_certificate_list_cont, errs, is_delta=True
    ):
        return None

    # Step h
    try:
        _verify_crl_signature(delta_certificate_list, crl_issuer.public_key)
    except CRLValidationError:
        errs.append(
            'Delta CRL signature could not be verified',
            delta_certificate_list_cont,
        )
        return None

    if policy and timing_params:
        if _check_crl_freshness(
            delta_certificate_list_cont,
            policy,
            timing_params,
            errs,
            is_delta=True,
        ):
            return delta_certificate_list_cont
    return None


def _verify_no_unknown_critical_extensions(
    certificate_list_cont: CRLContainer, errs: _CRLErrs, is_delta: bool
):
    extensions = certificate_list_cont.crl_data.critical_extensions
    if extensions - KNOWN_CRL_EXTENSIONS:
        errs.append(
            f'One or more unrecognized critical extensions are present in '
            f'the {"delta CRL" if is_delta else "CRL"}',
            certificate_list_cont,
        )
        return False
    return True


def _get_crl_scope_assuming_authority(
    crl_issuer: x509.Certificate,
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    certificate_list_cont: CRLContainer,
    is_indirect: bool,
    errs: _CRLErrs,
) -> Optional[Set[str]]:
    certificate_list = certificate_list_cont.crl_data
    crl_idp: crl.IssuingDistributionPoint = (
        certificate_list.issuing_distribution_point_value
    )

    is_pkc = isinstance(cert, x509.Certificate)
    # Step b 1
    has_dp_crl_issuer = False
    dp_match = False

    if is_pkc:
        crl_dps = cert.crl_distribution_points_value
    else:
        crl_dps = get_ac_extension_value(cert, 'crl_distribution_points')
    if crl_dps:
        crl_issuer_general_name = x509.GeneralName(
            name='directory_name', value=crl_issuer.subject
        )
        for dp in crl_dps:
            if dp['crl_issuer']:
                has_dp_crl_issuer = True
                if crl_issuer_general_name in dp['crl_issuer']:
                    dp_match = True

    crl_authority_name = crl_issuer.subject
    cert_issuer_name = get_issuer_dn(cert)
    same_issuer = crl_authority_name == cert_issuer_name
    indirect_match = has_dp_crl_issuer and dp_match and is_indirect
    missing_idp = has_dp_crl_issuer and (not dp_match or not is_indirect)
    indirect_crl_issuer = crl_issuer.issuer == cert_issuer_name

    if (
        not same_issuer and not indirect_match and not indirect_crl_issuer
    ) or missing_idp:
        errs.issuer_failures += 1
        return None

    # Step b 2

    if crl_idp is not None:
        if is_pkc:
            crl_idp_match = _handle_crl_idp_ext_constraints(
                cert=cert,
                certificate_list=certificate_list,
                crl_issuer=crl_issuer,
                crl_idp=crl_idp,
                crl_authority_name=crl_authority_name,
                errs=errs,
            )
        else:
            crl_idp_match = _handle_attr_cert_crl_idp_ext_constraints(
                crl_dps=crl_dps,
                certificate_list=certificate_list,
                crl_issuer=crl_issuer,
                crl_idp=crl_idp,
                crl_authority_name=crl_authority_name,
                errs=errs,
            )
        # error reporting is taken care of in the delegated method
        if not crl_idp_match:
            return None

    # Step d
    idp_reasons = None

    if crl_idp and crl_idp['only_some_reasons'].native is not None:
        idp_reasons = crl_idp['only_some_reasons'].native

    reason_keys = None
    if idp_reasons:
        reason_keys = idp_reasons

    if reason_keys is None:
        interim_reasons = VALID_REVOCATION_REASONS.copy()
    else:
        interim_reasons = reason_keys

    # Step e
    # We don't skip a CRL if it only contains reasons already checked since
    # a certificate issuer can self-issue a new cert that is used for CRLs

    if not _verify_no_unknown_critical_extensions(
        certificate_list_cont, errs, is_delta=False
    ):
        return None

    return interim_reasons


def _check_cert_on_crl_and_delta(
    crl_issuer: x509.Certificate,
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    certificate_list_cont: CRLContainer,
    delta_certificate_list_cont: Optional[CRLContainer],
    errs: _CRLErrs,
):
    certificate_list = certificate_list_cont.crl_data
    # Step i
    revoked_reason = None
    revoked_date = None

    cert_issuer_name = get_issuer_dn(cert)

    if delta_certificate_list_cont:
        delta_certificate_list = delta_certificate_list_cont.crl_data
        try:
            revoked_date, revoked_reason = find_cert_in_list(
                cert,
                cert_issuer_name,
                delta_certificate_list,
                crl_issuer.subject,
            )
        except NotImplementedError:
            errs.append(
                'One or more unrecognized critical extensions are present in '
                'the CRL entry for the certificate',
                delta_certificate_list_cont,
            )
            raise

    # Step j
    if revoked_reason is None:
        try:
            revoked_date, revoked_reason = find_cert_in_list(
                cert, cert_issuer_name, certificate_list, crl_issuer.subject
            )
        except NotImplementedError:
            errs.append(
                'One or more unrecognized critical extensions are present in '
                'the CRL entry for the certificate',
                certificate_list_cont,
            )
            raise

    # Step k
    if revoked_reason and revoked_reason.native == 'remove_from_crl':
        revoked_reason = None
        revoked_date = None

    return revoked_date, revoked_reason


async def _classify_relevant_crls(
    certificate_lists: List[CRLContainer],
    poe_manager: POEManager,
    errs: _CRLErrs,
    control_time: Optional[datetime] = None,
):
    # NOTE: the control_time parameter is only used in the time sliding
    # algorithm code path for AdES validation

    complete_lists_by_issuer = defaultdict(list)
    delta_lists_by_issuer = defaultdict(list)
    for certificate_list_cont in certificate_lists:
        certificate_list = certificate_list_cont.crl_data
        if control_time is not None:
            issued = certificate_list_cont.issuance_date
            if (
                issued is None
                or issued > control_time
                or poe_manager[certificate_list_cont] > control_time
            ):
                # We don't care about stuff issued after control_time
                # or without the right POE
                continue
        try:
            issuer_hashable = certificate_list.issuer.hashable
            if certificate_list.delta_crl_indicator_value is None:
                complete_lists_by_issuer[issuer_hashable].append(
                    certificate_list_cont
                )
            else:
                delta_lists_by_issuer[issuer_hashable].append(
                    certificate_list_cont
                )
        except ValueError as e:
            msg = "Generic processing error while classifying CRL."
            logging.debug(msg, exc_info=e)
            errs.append(msg, certificate_list)
    return complete_lists_by_issuer, delta_lists_by_issuer


def _process_crl_completeness(
    checked_reasons: Set[str],
    total_crls: int,
    errs: _CRLErrs,
    proc_state: ValProcState,
):
    # CRLs should not include this value, but at least one of the examples
    # from the NIST test suite does
    checked_reasons -= {'unused'}

    if checked_reasons != VALID_REVOCATION_REASONS:
        if total_crls == errs.issuer_failures:
            return CRLNoMatchesError(
                f"No CRLs were issued by the issuer of "
                f"{proc_state.describe_cert()}, or any indirect CRL "
                "issuer"
            )

        if not errs.failures:
            errs.append(
                'The available CRLs do not cover all revocation reasons', None
            )

        return CRLValidationIndeterminateError(
            f"Unable to determine if {proc_state.describe_cert()} "
            f"is revoked due to insufficient information from known CRLs",
            failures=errs.failures,
            suspect_stale=(
                errs.stale_last_usable_at
                if errs.freshness_failures_only
                else None
            ),
        )


async def verify_crl(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    path: ValidationPath,
    validation_context: ValidationContext,
    use_deltas=True,
    proc_state: Optional[ValProcState] = None,
):
    """
    Verifies a certificate against a list of CRLs, checking to make sure the
    certificate has not been revoked. Uses the algorithm from
    https://tools.ietf.org/html/rfc5280#section-6.3 as a basis, but the
    implementation differs to allow CRLs from unrecorded locations.

    :param cert:
        An asn1crypto.x509.Certificate or asn1crypto.cms.AttributeCertificateV2
        object to check for in the CRLs

    :param path:
        A pyhanko_certvalidator.path.ValidationPath object of the cert's
        validation path, or in the case of an AC, the AA's validation path.

    :param validation_context:
        A pyhanko_certvalidator.context.ValidationContext object to use for caching
        validation information

    :param use_deltas:
        A boolean indicating if delta CRLs should be used

    :param proc_state:
        Internal state for error reporting and policy application decisions.

    :raises:
        pyhanko_certvalidator.errors.CRLNoMatchesError - when none of the CRLs match the certificate
        pyhanko_certvalidator.errors.CRLValidationError - when any error occurs trying to verify the CertificateList
        pyhanko_certvalidator.errors.RevokedError - when the CRL indicates the certificate has been revoked
    """

    is_pkc = isinstance(cert, x509.Certificate)
    proc_state = proc_state or ValProcState(
        cert_path_stack=ConsList.sing(path),
        ee_name_override="attribute certificate" if not is_pkc else None,
    )

    revinfo_manager = validation_context.revinfo_manager
    errs = _CRLErrs()
    try:
        cert_issuer_auth = path.find_issuing_authority(cert)
    except LookupError:
        raise CRLNoMatchesError(
            f"Could not determine issuer certificate for "
            f"{proc_state.describe_cert()} in path."
        )

    # First, make an attempt to validate without downloading any extra CRLs
    certificate_lists = revinfo_manager.currently_available_crls()
    poe_manager = revinfo_manager.poe_manager
    (
        complete_lists_by_issuer,
        delta_lists_by_issuer,
    ) = await _classify_relevant_crls(certificate_lists, poe_manager, errs)

    # In the main loop, only complete CRLs are processed, so delta CRLs are
    # weeded out of the to-do list
    crls_to_process = []
    for issuer_crls in complete_lists_by_issuer.values():
        crls_to_process.extend(issuer_crls)
    total_crls = len(crls_to_process)

    checked_reasons = set()

    async def _process(crl_container, deltas):
        nonlocal checked_reasons
        nonlocal errs
        try:
            interim_reasons = await _handle_single_crl(
                cert=cert,
                cert_issuer_auth=cert_issuer_auth,
                certificate_list_cont=crl_container,
                path=path,
                validation_context=validation_context,
                delta_lists_by_issuer=deltas,
                use_deltas=use_deltas,
                errs=errs,
                proc_state=proc_state,
            )
            if interim_reasons is not None:
                # Step l
                checked_reasons |= interim_reasons
        except CRLValidationIndeterminateError as e:
            errs.append(e.msg, certificate_list_cont)
        except ValueError as e:
            msg = "Generic processing error while validating CRL."
            logging.debug(msg, exc_info=e)
            errs.append(msg, certificate_list_cont)

    for certificate_list_cont in crls_to_process:
        await _process(certificate_list_cont, delta_lists_by_issuer)

    exc = _process_crl_completeness(
        checked_reasons, total_crls, errs, proc_state
    )
    if exc is None:
        return
    elif not revinfo_manager.fetching_allowed:
        raise exc

    # If we're not done checking CRLs, but we are allowed to fetch more,
    #  let's go download some more CRLs...

    # TODO scan Freshest CRL extensions for delta CRLs?

    extra_certificate_lists = await revinfo_manager.fetch_crls(cert)
    (
        extra_complete_lists_by_issuer,
        extra_delta_lists_by_issuer,
    ) = await _classify_relevant_crls(
        extra_certificate_lists, poe_manager, errs
    )

    combined_deltas = {
        k: delta_lists_by_issuer.get(k, [])
        + extra_delta_lists_by_issuer.get(k, [])
        for k in set(delta_lists_by_issuer.keys()).union(
            extra_delta_lists_by_issuer.keys()
        )
    }

    crls_to_process = []
    for issuer, issuer_crls in complete_lists_by_issuer.items():
        # some of the new deltas might complement CRLs that we already had
        if issuer in extra_delta_lists_by_issuer:
            crls_to_process.extend(issuer_crls)
    for issuer_crls in extra_complete_lists_by_issuer.values():
        crls_to_process.extend(issuer_crls)

    for certificate_list_cont in crls_to_process:
        await _process(certificate_list_cont, combined_deltas)

    total_crls += len(crls_to_process)

    exc = _process_crl_completeness(
        checked_reasons, total_crls, errs, proc_state
    )
    if exc is not None:
        raise exc


@dataclass(frozen=True)
class ProvisionalCRLTrust:
    """
    A provisional CRL path, together with an optional delta CRL that may be
    relevant.
    """

    path: ValidationPath
    """
    A provisional validation path for the CRL. Requires path validation.
    """

    delta: Optional[CRLContainer]
    """
    A delta CRL that may be relevant to the parent CRL for which the path was
    put together.
    """


@dataclass(frozen=True)
class CRLOfInterest:
    """
    A CRL of interest.
    """

    crl: CRLContainer
    """
    The CRL data, packaged in a revocation info container.
    """

    prov_paths: List[ProvisionalCRLTrust]
    """
    Candidate validation paths for the CRL, together with relevant delta CRLs,
    if appropriate.
    """

    is_indirect: bool
    """
    Boolean indicating whether the CRL is an indirect one.
    """

    crl_authority_name: x509.Name
    """
    Distinguished name for the authority for which the CRL controls revocation.
    """


@dataclass(frozen=True)
class CRLCollectionResult:
    """
    The result of a CRL collection operation for AdES point-in-time
    validation purposes.
    """

    crls: List[CRLOfInterest]
    """
    List of potentially relevant CRLs.
    """

    failure_msgs: List[str]
    """
    List of failure messages, for error reporting purposes.
    """


async def _assess_crl_relevance(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    cert_issuer_auth: Authority,
    certificate_list_cont: CRLContainer,
    path: ValidationPath,
    revinfo_manager: RevinfoManager,
    delta_lists_by_issuer: Dict[str, List[CRLContainer]],
    use_deltas: bool,
    errs: _CRLErrs,
    proc_state: ValProcState,
) -> Optional[CRLOfInterest]:
    certificate_list = certificate_list_cont.crl_data
    registry = revinfo_manager.certificate_registry
    try:
        is_indirect, crl_authority_name = _get_crl_authority_name(
            certificate_list_cont,
            cert_issuer_auth.name,
            certificate_registry=registry,
            errs=errs,
        )
    except LookupError:
        # already logged by _get_crl_authority_name
        return None

    try:
        candidate_paths, _ = await _find_candidate_crl_paths(
            crl_authority_name,
            certificate_list,
            cert=cert,
            cert_issuer_auth=cert_issuer_auth,
            cert_path=path,
            certificate_registry=registry,
            is_indirect=is_indirect,
            proc_state=proc_state,
        )
    except CRLNoMatchesError:
        # this no-match issue will be dealt with at a higher level later
        errs.issuer_failures += 1
        return None
    except (CertificateFetchError, CRLValidationError) as e:
        errs.append(e.args[0], certificate_list)
        return None

    provisional_results = []
    for cand_path in candidate_paths:
        putative_issuer = cand_path.last
        interim_reasons = _get_crl_scope_assuming_authority(
            crl_issuer=putative_issuer,
            cert=cert,
            certificate_list_cont=certificate_list_cont,
            is_indirect=is_indirect,
            errs=errs,
        )
        if interim_reasons is None:
            continue

        delta = None
        if use_deltas:
            try:
                delta = _maybe_get_delta_crl(
                    certificate_list=certificate_list,
                    crl_issuer=putative_issuer,
                    delta_lists_by_issuer=delta_lists_by_issuer,
                    errs=errs,
                )
            except CRLValidationIndeterminateError as e:
                errs.append(e.msg, certificate_list)
                continue

        prov = ProvisionalCRLTrust(path=cand_path, delta=delta)
        provisional_results.append(prov)

    if not provisional_results:
        return None
    return CRLOfInterest(
        crl=certificate_list_cont,
        prov_paths=provisional_results,
        is_indirect=is_indirect,
        crl_authority_name=crl_authority_name,
    )


async def collect_relevant_crls_with_paths(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    path: ValidationPath,
    revinfo_manager: RevinfoManager,
    control_time: datetime,
    use_deltas=True,
    proc_state: Optional[ValProcState] = None,
) -> CRLCollectionResult:
    """
    Collect potentially relevant CRLs with the associated validation
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
    :param use_deltas:
        Whether to include delta CRLs.
    :param proc_state:
        The state of any prior validation process.
    :return:
        A :class:`.CRLCollectionResult`.
    """

    proc_state = proc_state or ValProcState(cert_path_stack=ConsList.sing(path))
    errs = _CRLErrs()
    candidate_crls = revinfo_manager.currently_available_crls()
    classify_job = _classify_relevant_crls(
        candidate_crls,
        revinfo_manager.poe_manager,
        errs,
        control_time=control_time,
    )
    complete_lists_by_issuer, delta_lists_by_issuer = await classify_job

    # In the main loop, only complete CRLs are processed, so delta CRLs are
    # weeded out of the to-do list
    crls_to_process = []
    for issuer_crls in complete_lists_by_issuer.values():
        crls_to_process.extend(issuer_crls)

    try:
        cert_issuer_auth = path.find_issuing_authority(cert)
    except LookupError:
        raise CRLNoMatchesError(
            f"Could not determine issuer certificate for "
            f"{proc_state.describe_cert()} in path."
        )

    relevant_crls = []

    for certificate_list_cont in crls_to_process:
        try:
            result = await _assess_crl_relevance(
                cert=cert,
                cert_issuer_auth=cert_issuer_auth,
                certificate_list_cont=certificate_list_cont,
                path=path,
                delta_lists_by_issuer=delta_lists_by_issuer,
                use_deltas=use_deltas,
                revinfo_manager=revinfo_manager,
                errs=errs,
                proc_state=proc_state,
            )
            if result is not None:
                relevant_crls.append(result)
        except ValueError as e:
            msg = "Generic processing error while validating CRL."
            logging.debug(msg, exc_info=e)
            errs.append(msg, certificate_list_cont)

    return CRLCollectionResult(
        crls=relevant_crls,
        failure_msgs=[f[0] for f in errs.failures],
    )


def _verify_crl_signature(certificate_list, public_key):
    """
    Verifies the digital signature on an asn1crypto.crl.CertificateList object

    :param certificate_list:
        An asn1crypto.crl.CertificateList object

    :raises:
        pyhanko_certvalidator.errors.CRLValidationError - when the signature is
        invalid or uses an unsupported algorithm
    """

    try:
        validate_sig(
            signature=certificate_list['signature'].native,
            signed_data=certificate_list['tbs_cert_list'].dump(),
            public_key_info=public_key,
            signed_digest_algorithm=certificate_list['signature_algorithm'],
            parameters=certificate_list['signature_algorithm']['parameters'],
        )
    except PSSParameterMismatch as e:
        raise CRLValidationError(
            'Invalid signature parameters on CertificateList'
        ) from e
    except InvalidSignature:
        raise CRLValidationError(
            'Unable to verify the signature of the CertificateList'
        )


def find_cert_in_list(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
    cert_issuer_name: x509.Name,
    certificate_list: crl.CertificateList,
    crl_authority_name: x509.Name,
):
    """
    Looks for a cert in the list of revoked certificates

    :param cert:
        An asn1crypto.x509.Certificate object of the cert being checked,
        or an asn1crypto.cms.AttributeCertificateV2 object in the case
        of an attribute certificate.

    :param cert_issuer_name:
        The certificate issuer's distinguished name

    :param certificate_list:
        An ans1crypto.crl.CertificateList object to look in for the cert

    :param crl_authority_name:
        The distinguished name of the default authority for which the CRL issues
        certificates.

    :return:
        A tuple of (None, None) if not present, otherwise a tuple of
        (asn1crypto.x509.Time object, asn1crypto.crl.CRLReason object)
        representing the date/time the object was revoked and why
    """

    revoked_certificates = certificate_list['tbs_cert_list'][
        'revoked_certificates'
    ]

    if isinstance(cert, x509.Certificate):
        cert_serial = cert['tbs_certificate']['serial_number'].dump()
    else:
        cert_serial = cert['ac_info']['serial_number'].dump()

    last_issuer_name = crl_authority_name

    cert_issuer_extension_id = CRLEntryExtensionId('certificate_issuer').dump()

    for revoked_cert in revoked_certificates:
        # This looks like a hack, but we have to look up the certificate_issuer
        # extension for every entry, since its value remains in effect for
        # future entries as well! (and PKITS has a test case for that...)
        # Since parsing those extensions every time is expensive for large CRLs,
        # we guard it with a dumb heuristic check: does the binary encoding
        # of that extension's OID appear anywhere in the entry's payload?
        # If not, we move on. If it does appear, we parse the extensions.
        if cert_issuer_extension_id in revoked_cert.dump():
            if revoked_cert.issuer_name:
                last_issuer_name = revoked_cert.issuer_name
        if revoked_cert['user_certificate'].dump() != cert_serial:
            continue
        if last_issuer_name != cert_issuer_name:
            continue

        if not revoked_cert.crl_reason_value:
            crl_reason = crl.CRLReason('unspecified')
        else:
            crl_reason = revoked_cert.crl_reason_value
        # If any unknown critical extensions, the entry can not be used
        if revoked_cert.critical_extensions - KNOWN_CRL_ENTRY_EXTENSIONS:
            raise NotImplementedError()

        return revoked_cert['revocation_date'].native, crl_reason

    return None, None
