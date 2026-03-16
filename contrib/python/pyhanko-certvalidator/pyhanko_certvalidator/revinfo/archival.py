import abc
import enum
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional, TypeVar, Union

from asn1crypto import algos, crl, ocsp
from pyhanko_certvalidator._types import type_name
from pyhanko_certvalidator.ltv.types import (
    IssuedItemContainer,
    ValidationTimingParams,
)
from pyhanko_certvalidator.policy_decl import (
    FRESHNESS_FALLBACK_VALIDITY_DEFAULT,
    CertRevTrustPolicy,
    FreshnessReqType,
)

__all__ = [
    'RevinfoUsabilityRating',
    'RevinfoUsability',
    'RevinfoContainer',
    'OCSPContainer',
    'CRLContainer',
    'sort_freshest_first',
    'process_legacy_crl_input',
    'process_legacy_ocsp_input',
]


class RevinfoUsabilityRating(enum.Enum):
    """
    Description of whether a piece of revocation information
    is considered usable in the circumstances provided.
    """

    OK = enum.auto()
    """
    The revocation information is usable.
    """

    STALE = enum.auto()
    """
    The revocation information is stale/too old.
    """

    TOO_NEW = enum.auto()
    """
    The revocation information is too recent.

    .. note::
        This is never an issue in the AdES validation model.
    """

    UNCLEAR = enum.auto()
    """
    The usability of the revocation information could not be
    assessed unambiguously.
    """

    @property
    def usable_ades(self) -> bool:
        """
        Boolean indicating whether the assigned rating corresponds to
        a "fresh" judgment in AdES.
        """

        return self in (
            RevinfoUsabilityRating.OK,
            RevinfoUsabilityRating.TOO_NEW,
        )


@dataclass(frozen=True)
class RevinfoUsability:
    """
    Usability rating and cutoff date for a particular piece of
    revocation information.
    """

    rating: RevinfoUsabilityRating
    """
    The rating assigned.
    """

    last_usable_at: Optional[datetime] = None
    """
    The last date at which the revocation information could have been
    considered usable, if applicable.
    """


class RevinfoContainer(IssuedItemContainer, abc.ABC):
    """
    A container for a piece of revocation information.
    """

    def usable_at(
        self, policy: CertRevTrustPolicy, timing_params: ValidationTimingParams
    ) -> RevinfoUsability:
        """
        Assess the usability of the revocation information given a
        revocation information trust policy and timing parameters.

        :param policy:
            The revocation information trust policy.
        :param timing_params:
            Timing-related information.
        :return:
            A :class:`.RevinfoUsability` judgment.
        """
        raise NotImplementedError

    @property
    def revinfo_sig_mechanism_used(
        self,
    ) -> Optional[algos.SignedDigestAlgorithm]:
        """
        Extract the signature mechanism used to guarantee the authenticity
        of the revocation information, if applicable.
        """
        raise NotImplementedError


RevInfoType = TypeVar('RevInfoType', bound=RevinfoContainer)


def sort_freshest_first(lst: Iterable[RevInfoType]) -> List[RevInfoType]:
    """
    Sort a list of revocation information containers in freshest-first order.

    Revocation information that does not have a well-defined issuance date
    will be grouped at the end.

    :param lst:
        A list of :class:`.RevinfoContainer` objects of the same type.
    :return:
        The same list sorted from fresh to stale.
    """

    def _key(container: RevinfoContainer):
        dt = container.issuance_date
        # if dt is None ---> (0, None)
        # else ---> (1, dt)
        # This ensures that None is never compared to anything (which would
        #  cause a TypeError), and that (0, None) gets sorted before everything
        #  else. Since we sort reversed, the "unknown issuance date" ones
        #  are dumped at the end of the list.
        return dt is not None, dt

    return sorted(lst, key=_key, reverse=True)


def _freshness_delta(policy, this_update, next_update, time_tolerance):
    freshness_delta = policy.freshness
    if freshness_delta is None:
        if next_update is not None and next_update >= this_update:
            freshness_delta = next_update - this_update
    if freshness_delta is not None:
        freshness_delta = abs(freshness_delta) + time_tolerance
    return freshness_delta


def _judge_revinfo(
    this_update: Optional[datetime],
    next_update: Optional[datetime],
    policy: CertRevTrustPolicy,
    timing_params: ValidationTimingParams,
) -> RevinfoUsability:
    if this_update is None:
        return RevinfoUsability(RevinfoUsabilityRating.UNCLEAR)

    validation_time = timing_params.validation_time
    time_tolerance = timing_params.time_tolerance

    # Revinfo issued after the validation time may need to be considered
    # in AdES point-in-time validation.
    # In the legacy "default" policy, this is controlled by the retroactive
    # revinfo switch.

    # see 5.2.5.4 in ETSI EN 319 102-1
    if policy.freshness_req_type == FreshnessReqType.TIME_AFTER_SIGNATURE:
        # check whether the revinfo was generated sufficiently long _after_
        # the (presumptive) signature time
        freshness_delta = _freshness_delta(
            policy, this_update, next_update, time_tolerance
        )
        if freshness_delta is None:
            return RevinfoUsability(RevinfoUsabilityRating.UNCLEAR)
        signature_poe_time = timing_params.best_signature_time
        if this_update - signature_poe_time < freshness_delta:
            return RevinfoUsability(
                RevinfoUsabilityRating.STALE,
                last_usable_at=this_update + freshness_delta,
            )
    elif (
        policy.freshness_req_type
        == FreshnessReqType.MAX_DIFF_REVOCATION_VALIDATION
    ):
        # check whether the difference between thisUpdate
        # and the validation time is small enough

        # add time_tolerance to allow for additional time drift
        freshness_delta = _freshness_delta(
            policy, this_update, next_update, time_tolerance
        )
        if freshness_delta is None:
            return RevinfoUsability(RevinfoUsabilityRating.UNCLEAR)

        # See ETSI EN 319 102-1, § 5.2.5.4, item 2)
        #  in particular, "too recent" doesn't seem to apply;
        #  the result is pass/fail
        if this_update < validation_time - freshness_delta:
            return RevinfoUsability(
                RevinfoUsabilityRating.STALE,
                last_usable_at=this_update + freshness_delta,
            )
    elif policy.freshness_req_type == FreshnessReqType.DEFAULT:
        # check whether the validation time falls within the
        # thisUpdate-nextUpdate window (non-AdES!!)
        if next_update is None:
            # OCSP semantics of nextUpdate = VOID is "please request
            # another update whenever you like".
            # In our default/legacy validation model this is difficult to
            # interpret.
            # for historical point-in-time validation, this is disqualifying
            next_update = this_update + FRESHNESS_FALLBACK_VALIDITY_DEFAULT

        retroactive = policy.retroactive_revinfo

        if not retroactive and validation_time < this_update - time_tolerance:
            return RevinfoUsability(RevinfoUsabilityRating.TOO_NEW)
        if validation_time > next_update + time_tolerance:
            return RevinfoUsability(
                RevinfoUsabilityRating.STALE,
                last_usable_at=next_update + time_tolerance,
            )
    else:  # pragma: nocover
        raise NotImplementedError
    return RevinfoUsability(RevinfoUsabilityRating.OK)


def _extract_basic_ocsp_response(
    ocsp_response,
) -> Optional[ocsp.BasicOCSPResponse]:
    # Make sure that we get a valid response back from the OCSP responder
    status = ocsp_response['response_status'].native
    if status != 'successful':
        return None

    response_bytes = ocsp_response['response_bytes']
    if response_bytes['response_type'].native != 'basic_ocsp_response':
        return None

    return response_bytes['response'].parsed


@dataclass(frozen=True)
class OCSPContainer(RevinfoContainer):
    """
    Container for an OCSP response.
    """

    ocsp_response_data: ocsp.OCSPResponse
    """
    The OCSP response value.
    """

    index: int = 0
    """
    The index of the ``SingleResponse`` payload in the original OCSP
    response object retrieved from the server, if applicable.
    """

    @classmethod
    def load_multi(
        cls, ocsp_response: ocsp.OCSPResponse
    ) -> List['OCSPContainer']:
        """
        Turn an OCSP response object into one or more :class:`.OCSPContainer`
        objects. If a :class:`.OCSPContainer` contains more than one
        ``SingleResponse``, then the same OCSP response will be duplicated
        into multiple containers, each with a different ``index`` value.

        :param ocsp_response:
            An OCSP response.
        :return:
            A list of :class:`.OCSPContainer` objects, one for each
            ``SingleResponse`` value.
        """

        basic_ocsp_response = _extract_basic_ocsp_response(ocsp_response)
        if basic_ocsp_response is None:
            return []
        tbs_response = basic_ocsp_response['tbs_response_data']

        return [
            OCSPContainer(ocsp_response_data=ocsp_response, index=ix)
            for ix in range(len(tbs_response['responses']))
        ]

    @property
    def issuance_date(self) -> Optional[datetime]:
        cert_response = self.extract_single_response()
        if cert_response is None:
            return None

        return cert_response['this_update'].native

    def usable_at(
        self, policy: CertRevTrustPolicy, timing_params: ValidationTimingParams
    ) -> RevinfoUsability:
        cert_response = self.extract_single_response()
        if cert_response is None:
            return RevinfoUsability(RevinfoUsabilityRating.UNCLEAR)

        this_update = cert_response['this_update'].native
        next_update = cert_response['next_update'].native
        return _judge_revinfo(
            this_update,
            next_update,
            policy=policy,
            timing_params=timing_params,
        )

    def extract_basic_ocsp_response(self) -> Optional[ocsp.BasicOCSPResponse]:
        """
        Extract the ``BasicOCSPResponse``, assuming there is one (i.e.
        the OCSP response is a standard, non-error response).
        """

        return _extract_basic_ocsp_response(self.ocsp_response_data)

    def extract_single_response(self) -> Optional[ocsp.SingleResponse]:
        """
        Extract the unique ``SingleResponse`` value identified by the
        index.
        """

        basic_ocsp_response = self.extract_basic_ocsp_response()
        if basic_ocsp_response is None:
            return None
        tbs_response = basic_ocsp_response['tbs_response_data']

        if len(tbs_response['responses']) <= self.index:
            return None
        return tbs_response['responses'][self.index]

    @property
    def revinfo_sig_mechanism_used(
        self,
    ) -> Optional[algos.SignedDigestAlgorithm]:
        basic_resp = self.extract_basic_ocsp_response()
        return None if basic_resp is None else basic_resp['signature_algorithm']


@dataclass(frozen=True)
class CRLContainer(RevinfoContainer):
    """
    Container for a certificate revocation list (CRL).
    """

    crl_data: crl.CertificateList
    """
    The CRL data.
    """

    def usable_at(
        self, policy: CertRevTrustPolicy, timing_params: ValidationTimingParams
    ) -> RevinfoUsability:
        tbs_cert_list = self.crl_data['tbs_cert_list']
        this_update = tbs_cert_list['this_update'].native
        next_update = tbs_cert_list['next_update'].native
        return _judge_revinfo(
            this_update, next_update, policy=policy, timing_params=timing_params
        )

    @property
    def issuance_date(self) -> Optional[datetime]:
        tbs_cert_list = self.crl_data['tbs_cert_list']
        return tbs_cert_list['this_update'].native

    @property
    def revinfo_sig_mechanism_used(self) -> algos.SignedDigestAlgorithm:
        return self.crl_data['signature_algorithm']


LegacyCompatCRL = Union[bytes, crl.CertificateList, CRLContainer]
LegacyCompatOCSP = Union[bytes, ocsp.OCSPResponse, OCSPContainer]


def process_legacy_crl_input(
    crls: Iterable[LegacyCompatCRL],
) -> List[CRLContainer]:
    """
    Internal function to process legacy CRL data into one or more
    :class:`.CRLContainer`.

    :param crls:
        Legacy CRL input data.
    :return:
        A list of :class:`.CRLContainer` objects.
    """

    new_crls = []
    for crl_ in crls:
        if isinstance(crl_, bytes):
            crl_ = crl.CertificateList.load(crl_)
        if isinstance(crl_, crl.CertificateList):
            crl_ = CRLContainer(crl_)
        if isinstance(crl_, CRLContainer):
            new_crls.append(crl_)
        else:
            raise TypeError(
                f"crls must be a list of byte strings or "
                f"asn1crypto.crl.CertificateList objects, not {type_name(crl_)}"
            )
    return new_crls


def process_legacy_ocsp_input(
    ocsps: Iterable[LegacyCompatOCSP],
) -> List[OCSPContainer]:
    """
    Internal function to process legacy OCSP data into one or more
    :class:`.OCSPContainer`.

    :param ocsps:
        Legacy OCSP input data.
    :return:
        A list of :class:`.OCSPContainer` objects.
    """

    new_ocsps = []
    for ocsp_ in ocsps:
        if isinstance(ocsp_, bytes):
            ocsp_ = ocsp.OCSPResponse.load(ocsp_)
        if isinstance(ocsp_, ocsp.OCSPResponse):
            extr = OCSPContainer.load_multi(ocsp_)
            new_ocsps.extend(extr)
        elif isinstance(ocsp_, OCSPContainer):
            new_ocsps.append(ocsp_)
        else:
            raise TypeError(
                f"ocsps must be a list of byte strings or "
                f"asn1crypto.ocsp.OCSPResponse objects, not {type_name(ocsp_)}"
            )
    return new_ocsps
