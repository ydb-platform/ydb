import asyncio
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Set, Tuple

from asn1crypto import algos, keys, x509
from pyhanko_certvalidator._state import ValProcState
from pyhanko_certvalidator.errors import (
    DisallowedAlgorithmError,
    InsufficientPOEError,
    InsufficientRevinfoError,
    RevokedError,
)
from pyhanko_certvalidator.ltv.types import (
    ValidationTimingInfo,
    ValidationTimingParams,
)
from pyhanko_certvalidator.path import ValidationPath
from pyhanko_certvalidator.policy_decl import (
    AlgorithmUsagePolicy,
    CertRevTrustPolicy,
    RevocationCheckingRule,
)
from pyhanko_certvalidator.revinfo.archival import RevinfoContainer
from pyhanko_certvalidator.revinfo.manager import RevinfoManager
from pyhanko_certvalidator.revinfo.validate_crl import (
    CRLOfInterest,
    _check_cert_on_crl_and_delta,
    _CRLErrs,
    collect_relevant_crls_with_paths,
)
from pyhanko_certvalidator.revinfo.validate_ocsp import (
    OCSPResponseOfInterest,
    _check_ocsp_status,
    collect_relevant_responses_with_paths,
)
from pyhanko_certvalidator.util import ConsList

__all__ = ['time_slide', 'ades_gather_prima_facie_revinfo']


async def ades_gather_prima_facie_revinfo(
    path: ValidationPath,
    revinfo_manager: RevinfoManager,
    control_time: datetime,
    revocation_checking_rule: RevocationCheckingRule,
) -> Tuple[List[CRLOfInterest], List[OCSPResponseOfInterest]]:
    """
    Gather potentially relevant revocation information for the leaf
    certificate of a candidate validation path.
    Only the scope of the revocation information will be checked, no
    detailed validation will occur.

    :param path:
        The candidate validation path.
    :param revinfo_manager:
        The revocation info manager.
    :param control_time:
        The time horizon that serves as a relevance cutoff.
    :param revocation_checking_rule:
        Revocation info rule controlling which kind(s) of revocation
        information will be fetched.
    :return:
        A 2-element tuple containing a list of the fetched CRLs and
        OCSP responses, respectively.
    """

    cert = path.leaf
    if revocation_checking_rule.ocsp_relevant:
        ocsp_result = await collect_relevant_responses_with_paths(
            cert, path, revinfo_manager, control_time
        )
        ocsps = ocsp_result.responses
    else:
        ocsps = []

    if revocation_checking_rule.crl_relevant:
        crl_result = await collect_relevant_crls_with_paths(
            cert, path, revinfo_manager, control_time
        )
        crls = crl_result.crls
    else:
        crls = []
    return crls, ocsps


def _tails(path: ValidationPath):
    cur_path = path
    yield cur_path, True
    while cur_path.pkix_len > 1:
        cur_path = cur_path.copy_and_drop_leaf()
        yield cur_path, False


def _apply_algo_policy(
    algo_policy: AlgorithmUsagePolicy,
    algo_used: algos.SignedDigestAlgorithm,
    control_time: datetime,
    public_key: keys.PublicKeyInfo,
    val_proc_state: ValProcState,
):
    sig_constraint = algo_policy.signature_algorithm_allowed(
        algo_used, control_time, public_key
    )
    algo_name = algo_used['algorithm'].native
    if not sig_constraint.allowed:
        if sig_constraint.not_allowed_after:
            # rewind the clock up until the point where the algorithm
            # was actually permissible
            control_time = min(control_time, sig_constraint.not_allowed_after)
        else:
            msg = (
                f"Algorithm {algo_name} is banned outright without "
                f"time constraints."
            )
            if sig_constraint.failure_reason is not None:
                msg += f" Reason: {sig_constraint.failure_reason}"
            raise DisallowedAlgorithmError.from_state(
                msg,
                val_proc_state,
                banned_since=None,
            )
    return control_time


def _update_control_time_for_unrevoked(
    control_time: datetime,
    revinfo_container: RevinfoContainer,
    rev_trust_policy: CertRevTrustPolicy,
    time_tolerance: timedelta,
):
    # if the cert is not on the list, we need the freshness check
    usability = revinfo_container.usable_at(
        rev_trust_policy,
        ValidationTimingParams(
            timing_info=ValidationTimingInfo(
                validation_time=control_time,
                best_signature_time=control_time,
                point_in_time_validation=True,
            ),
            time_tolerance=time_tolerance,
        ),
    )
    issuance_date = revinfo_container.issuance_date
    if not usability.rating.usable_ades:
        # set the control time to the issuance date / last usable date
        # (note: the TOO_NEW check is to prevent problems
        #  with freshness policies involving cooldown periods,
        #  which aren't really supported in the time sliding
        #  algorithm, but hey)
        # NOTE: the spec mandates using the issuance date here, but I believe
        # that's wrong: the last date at which the revinfo is still considered
        # fresh should be used instead. This distinction matters, since
        # (especially when CRLs are used) the issuance date of the revinfo
        # is often before the signature time.
        cutoff_date = usability.last_usable_at or issuance_date
        if cutoff_date is not None:
            control_time = min(cutoff_date, control_time)
    return control_time


def _update_control_time(
    revoked_date: Optional[datetime],
    control_time: datetime,
    revinfo_container: RevinfoContainer,
    algo_policy: Optional[AlgorithmUsagePolicy],
    issuer_public_key: keys.PublicKeyInfo,
    val_proc_state: ValProcState,
):
    if revoked_date:
        # this means we have to update control_time
        control_time = min(revoked_date, control_time)
    algo_used = revinfo_container.revinfo_sig_mechanism_used
    if algo_policy is not None and algo_used is not None:
        control_time = _apply_algo_policy(
            algo_policy,
            algo_used,
            control_time,
            issuer_public_key,
            val_proc_state,
        )
    return control_time


async def _time_slide(
    path: ValidationPath,
    init_control_time: datetime,
    revinfo_manager: RevinfoManager,
    rev_trust_policy: CertRevTrustPolicy,
    algo_usage_policy: Optional[AlgorithmUsagePolicy],
    # TODO use policy objects
    time_tolerance: timedelta,
    cert_stack: ConsList[bytes],
    path_stack: ConsList[ValidationPath],
) -> datetime:
    control_time = init_control_time
    checking_policy = rev_trust_policy.revocation_checking_policy

    # For zero-length paths, there is nothing to check
    if path.pkix_len == 0:
        return init_control_time

    # The ETSI algorithm requires us to collect revinfo for each
    # cert in the path, starting with the first (after the root).
    # Since our revinfo collection methods require paths instead of individual
    # certs, we instead loop over partial paths
    partial_paths = list(reversed(list(_tails(path))))
    poe_manager = revinfo_manager.poe_manager
    for current_path, is_ee in partial_paths:
        crls, ocsps = await ades_gather_prima_facie_revinfo(
            current_path,
            revinfo_manager=revinfo_manager,
            control_time=control_time,
            revocation_checking_rule=(
                checking_policy.ee_certificate_rule
                if is_ee
                else checking_policy.intermediate_ca_cert_rule
            ),
        )
        cert = current_path.leaf
        new_cert_stack = cert_stack.cons(cert.dump())
        new_path_stack = path_stack.cons(path)

        proc_state = ValProcState(cert_path_stack=new_path_stack)

        if poe_manager[cert] > control_time:
            raise InsufficientPOEError.from_state(
                f"No proof of existence available for certificate "
                f"{cert.subject.human_friendly} at control time "
                f"{control_time.isoformat()}.",
                proc_state,
            )
        if not crls and not ocsps:
            if isinstance(cert, x509.Certificate):
                ident = cert.subject.human_friendly
            else:
                ident = "attribute certificate"

            # don't raise an error for revo-exempt certs (OCSP responders)
            if cert.ocsp_no_check_value is None:
                raise InsufficientRevinfoError.from_state(
                    f"No revocation info from before {control_time.isoformat()}"
                    f" found for certificate {ident}.",
                    proc_state,
                )

        once_revoked = False
        most_recent_crl = None
        # We always take the chain of trust of a CRL/OCSP response
        # at face value
        for crl_of_interest in crls:
            # skip CRLs that are no longer relevant
            issued = crl_of_interest.crl.issuance_date
            if (
                not issued
                or issued > control_time
                or poe_manager[crl_of_interest.crl] > control_time
            ):
                continue
            sub_paths = crl_of_interest.prov_paths

            # recurse into the paths associated with the CRL and adjust
            # the control time accordingly
            # don't bother checking issuers that already appear
            # in the chain of trust that we're currently looking into
            sub_path_skip_list: Set[bytes] = set(new_cert_stack) | set(
                cert.dump() for cert in current_path
            )
            sub_path_control_times = await asyncio.gather(
                *(
                    _time_slide(
                        crl_path.path,
                        control_time,
                        revinfo_manager,
                        rev_trust_policy,
                        algo_usage_policy,
                        time_tolerance,
                        cert_stack=new_cert_stack,
                        path_stack=new_path_stack,
                    )
                    for crl_path in sub_paths
                    if (
                        crl_path.path.leaf
                        and crl_path.path.leaf.dump() not in sub_path_skip_list
                    )
                )
            )
            control_time = min([control_time, *sub_path_control_times])

            for candidate_crl_path in sub_paths:
                revoked_date, revoked_reason = _check_cert_on_crl_and_delta(
                    crl_issuer=candidate_crl_path.path.leaf,
                    cert=cert,
                    certificate_list_cont=crl_of_interest.crl,
                    delta_certificate_list_cont=candidate_crl_path.delta,
                    errs=_CRLErrs(),
                )
                crl_iss_cert = candidate_crl_path.path.leaf
                assert isinstance(crl_iss_cert, x509.Certificate)

                once_revoked |= revoked_date is not None

                crl_container = crl_of_interest.crl
                if most_recent_crl is None:
                    most_recent_crl = crl_container
                else:
                    if (
                        most_recent_crl.issuance_date
                        and crl_container.issuance_date
                        and most_recent_crl.issuance_date
                        < crl_container.issuance_date
                    ):
                        most_recent_crl = crl_container
                control_time = _update_control_time(
                    revoked_date,
                    control_time,
                    revinfo_container=crl_container,
                    algo_policy=algo_usage_policy,
                    issuer_public_key=crl_iss_cert.public_key,
                    val_proc_state=proc_state,
                )

        most_recent_ocsp = None
        for ocsp_of_interest in ocsps:
            ocsp_container = ocsp_of_interest.ocsp_response
            issued = ocsp_container.issuance_date
            if (
                not issued
                or issued > control_time
                or poe_manager[ocsp_of_interest.ocsp_response] > control_time
            ):
                continue

            control_time = await _time_slide(
                ocsp_of_interest.prov_path,
                control_time,
                revinfo_manager,
                rev_trust_policy,
                algo_usage_policy,
                time_tolerance,
                cert_stack=new_cert_stack,
                path_stack=new_path_stack,
            )
            try:
                _check_ocsp_status(
                    ocsp_response=ocsp_container,
                    proc_state=ValProcState(cert_path_stack=new_path_stack),
                    control_time=control_time,
                )
                revoked_date = None
            except RevokedError as e:
                revoked_date = e.revocation_dt

            once_revoked |= revoked_date is not None
            ocsp_iss_cert = ocsp_of_interest.prov_path.leaf
            assert isinstance(ocsp_iss_cert, x509.Certificate)
            if most_recent_ocsp is None or (
                most_recent_ocsp.issuance_date
                and most_recent_ocsp.issuance_date < issued
            ):
                most_recent_ocsp = ocsp_container
            control_time = _update_control_time(
                revoked_date,
                control_time,
                revinfo_container=ocsp_container,
                algo_policy=algo_usage_policy,
                issuer_public_key=ocsp_iss_cert.public_key,
                val_proc_state=proc_state,
            )
        # check the algorithm constraints for the certificate itself
        if algo_usage_policy is not None:
            leaf_ca = list(current_path.iter_authorities())[-1]
            control_time = _apply_algo_policy(
                algo_usage_policy,
                cert['signature_algorithm'],
                control_time,
                leaf_ca.public_key,
                val_proc_state=proc_state,
            )

        # (c) if the certificate was not marked as revoked -> update
        # based on the freshness of the most recent piece of revinfo
        if not once_revoked:
            revinfo_items: Iterable[RevinfoContainer] = [
                x for x in (most_recent_ocsp, most_recent_crl) if x is not None
            ]
            most_recent_revinfo = max(
                revinfo_items,
                key=lambda x: x.issuance_date or control_time,
                default=None,
            )
            if most_recent_revinfo is not None:
                control_time = _update_control_time_for_unrevoked(
                    control_time=control_time,
                    revinfo_container=most_recent_revinfo,
                    rev_trust_policy=rev_trust_policy,
                    time_tolerance=time_tolerance,
                )

    return control_time


async def time_slide(
    path: ValidationPath,
    init_control_time: datetime,
    revinfo_manager: RevinfoManager,
    rev_trust_policy: CertRevTrustPolicy,
    algo_usage_policy: Optional[AlgorithmUsagePolicy],
    time_tolerance: timedelta,
) -> datetime:
    """
    Execute the ETSI EN 319 102-1 time slide algorithm against the given path.

    .. warning::
        This is incubating internal API.

    .. note::
        This implementation will also attempt to take into account chains of
        trust of indirect CRLs. This is not a requirement of the specification,
        but also somewhat unlikely to arise in practice in cases where AdES
        compliance actually matters.

    :param path:
        The prospective validation path against which to execute the time slide
        algorithm.
    :param init_control_time:
        The initial control time, typically the current time.
    :param revinfo_manager:
        The revocation info manager.
    :param rev_trust_policy:
        The trust policy for revocation information.
    :param algo_usage_policy:
        The algorithm usage policy.
    :param time_tolerance:
        The tolerance to apply when evaluating time-related constraints.
    :return:
        The resulting control time.
    """
    return await _time_slide(
        path,
        init_control_time,
        revinfo_manager,
        rev_trust_policy,
        algo_usage_policy,
        time_tolerance,
        cert_stack=ConsList.empty(),
        path_stack=ConsList.empty(),
    )
