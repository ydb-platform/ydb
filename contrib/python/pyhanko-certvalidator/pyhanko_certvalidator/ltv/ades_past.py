import dataclasses
import logging
from datetime import datetime, timezone
from typing import Optional

from pyhanko_certvalidator.context import (
    CertValidationPolicySpec,
    ValidationDataHandlers,
)
from pyhanko_certvalidator.errors import ValidationError
from pyhanko_certvalidator.ltv.errors import (
    PastValidatePrecheckFailure,
    TimeSlideFailure,
)
from pyhanko_certvalidator.ltv.time_slide import time_slide
from pyhanko_certvalidator.ltv.types import ValidationTimingInfo
from pyhanko_certvalidator.path import ValidationPath
from pyhanko_certvalidator.policy_decl import (
    NO_REVOCATION,
    AcceptAllAlgorithms,
    CertRevTrustPolicy,
)
from pyhanko_certvalidator.validate import async_validate_path

__all__ = ['past_validate']

logger = logging.getLogger(__name__)


async def _past_validate_precheck(
    path: ValidationPath,
    validation_policy_spec: CertValidationPolicySpec,
):
    # The past validation algorithm requires us to run the "regular"
    # validation algorithm without regard for revocation and expiration
    # on a known-good time

    # Shell model: intersect the validity windows of all certs in the path
    certs = list(path.iter_certs(include_root=False))
    lower_bound = max(c.not_valid_before for c in certs)
    upper_bound = min(c.not_valid_after for c in certs)

    if lower_bound >= upper_bound:
        raise PastValidatePrecheckFailure(
            "The intersection of the validity periods of the certificates "
            "in the path is empty or degenerate."
        )

    ref_time = ValidationTimingInfo(
        validation_time=upper_bound,
        best_signature_time=upper_bound,
        point_in_time_validation=True,
    )

    validation_context = dataclasses.replace(
        validation_policy_spec,
        revinfo_policy=CertRevTrustPolicy(
            revocation_checking_policy=NO_REVOCATION
        ),
        algorithm_usage_policy=AcceptAllAlgorithms(),
    ).build_validation_context(timing_info=ref_time, handlers=None)

    try:
        await async_validate_path(
            validation_context,
            path,
            validation_policy_spec.pkix_validation_params,
        )
    except ValidationError as e:
        raise PastValidatePrecheckFailure(
            "Elementary path validation routine failed during pre-check "
            "for past point-in-time validation"
        ) from e


async def past_validate(
    path: ValidationPath,
    validation_policy_spec: CertValidationPolicySpec,
    validation_data_handlers: ValidationDataHandlers,
    init_control_time: Optional[datetime] = None,
    best_signature_time: Optional[datetime] = None,
) -> datetime:
    """
    Execute the ETSI EN 319 102-1 past certificate validation algorithm
    against the given path (ETSI EN 319 102-1, § 5.6.2.1).

    Instead of merely evaluating X.509 validation constraints, the algorithm
    will perform a full point-in-time reevaluation of the path at the
    control time mandated by the specification. This implies that a caller
    implementing the past signature validation algorithm no longer needs to
    explicitly reevaluate CA certificate revocation times and/or algorithm
    constraints based on POEs.

    .. warning::
        This is incubating internal API.

    :param path:
        The prospective validation path against which to execute the algorithm.
    :param validation_policy_spec:
        The validation policy specification.
    :param validation_data_handlers:
        The handlers used to manage collected certificates,revocation
        information and proof-of-existence records.
    :param init_control_time:
        Initial control time; defaults to the current time.
    :param best_signature_time:
        Usage time to use in freshness computations.
    :return:
        The control time returned by the time sliding algorithm.
        Informally, the last time at which the certificate was known to be
        valid.
    """

    await _past_validate_precheck(
        path,
        validation_policy_spec,
    )

    try:
        # time slide
        init_control_time = init_control_time or datetime.now(tz=timezone.utc)
        control_time = await time_slide(
            path,
            init_control_time=init_control_time,
            rev_trust_policy=validation_policy_spec.revinfo_policy,
            algo_usage_policy=validation_policy_spec.algorithm_usage_policy,
            time_tolerance=validation_policy_spec.time_tolerance,
            revinfo_manager=validation_data_handlers.revinfo_manager,
        )
        logger.info(
            f"AdES time slide yields %s as the control time for path with "
            f"leaf {path.describe_leaf()}",
            control_time,
        )
    except ValidationError as e:
        raise TimeSlideFailure(
            f"Failed to get control time for point-in-time validation for path "
            f"with leaf {path.describe_leaf()}"
        ) from e

    ref_time = ValidationTimingInfo(
        validation_time=control_time,
        best_signature_time=best_signature_time or control_time,
        point_in_time_validation=True,
    )

    # -> validate
    validation_context = validation_policy_spec.build_validation_context(
        timing_info=ref_time, handlers=validation_data_handlers
    )

    # Maintenance note:
    #  Doing a full point-in-time re-validation of the path is much more
    #  heavy-handed than what the AdES spec requires. We really only have to
    #  evaluate the chain constraints here.
    #  However, the past signature validation algorithm needs information about
    #  revocations up the chain and algorithm usage for _all_ operations in
    #  the validation process which is hard to pass on given the current
    #  architecture of certvalidator. Reevaluating with a time in the past
    #  is easier, and the POE enforcement is the same either way.

    await async_validate_path(
        validation_context,
        path,
        parameters=validation_policy_spec.pkix_validation_params,
    )

    return control_time
