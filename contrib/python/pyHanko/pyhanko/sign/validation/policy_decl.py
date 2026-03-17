import dataclasses
import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Generator, List, Optional

from asn1crypto import x509
from pyhanko_certvalidator.context import (
    CertValidationPolicySpec,
    ValidationDataHandlers,
)
from pyhanko_certvalidator.context import (
    bootstrap_validation_data_handlers as _certvalidator_bootstrap_handlers,
)
from pyhanko_certvalidator.fetchers import FetcherBackend
from pyhanko_certvalidator.fetchers.requests_fetchers import (
    RequestsFetcherBackend,
)
from pyhanko_certvalidator.ltv.poe import (
    KnownPOE,
    POEManager,
    POEType,
    ValidationObject,
    ValidationObjectType,
    digest_for_poe,
)
from pyhanko_certvalidator.ltv.types import ValidationTimingInfo
from pyhanko_certvalidator.policy_decl import NonRevokedStatusAssertion
from pyhanko_certvalidator.revinfo.archival import CRLContainer, OCSPContainer

from pyhanko.sign.diff_analysis import DEFAULT_DIFF_POLICY, DiffPolicy
from pyhanko.sign.validation import KeyUsageConstraints
from pyhanko.sign.validation.utils import CMSAlgorithmUsagePolicy

__all__ = [
    'SignatureValidationSpec',
    'PdfSignatureValidationSpec',
    'RevinfoOnlineFetchingRule',
    'LocalKnowledge',
    'RevocationInfoGatheringSpec',
    'CMSAlgorithmUsagePolicy',
    'bootstrap_validation_data_handlers',
]


class RevinfoOnlineFetchingRule(enum.Enum):
    ALWAYS_FETCH = enum.auto()
    """
    Always permit fetching revocation information from online sources.
    """

    NO_HISTORICAL_FETCH = enum.auto()
    """
    Only attempt to fetch revocation information when performing validation
    of a signature at the current time, and use the local cache in past
    validation evaluation.
    """
    # TODO document precisely what this means, esp. for the legacy API

    LOCAL_ONLY = enum.auto()
    """
    Only use locally cached revocation information.
    """


@dataclass(frozen=True)
class RevocationInfoGatheringSpec:
    online_fetching_rule: RevinfoOnlineFetchingRule = (
        RevinfoOnlineFetchingRule.NO_HISTORICAL_FETCH
    )
    fetcher_backend: FetcherBackend = field(
        default_factory=RequestsFetcherBackend
    )


@dataclass(frozen=True)
class LocalKnowledge:
    known_ocsps: List[OCSPContainer] = field(default_factory=list)
    known_crls: List[CRLContainer] = field(default_factory=list)
    known_certs: List[x509.Certificate] = field(default_factory=list)
    known_poes: List[KnownPOE] = field(default_factory=list)
    nonrevoked_assertions: List[NonRevokedStatusAssertion] = field(
        default_factory=list
    )

    def add_to_poe_manager(self, poe_manager: POEManager):
        for poe in self.known_poes:
            poe_manager.register_known_poe(poe)

    def assert_existence_known_at(
        self, dt: datetime
    ) -> Generator[KnownPOE, None, None]:
        for poe in self.known_poes:
            yield dataclasses.replace(
                poe,
                poe_time=min(poe.poe_time, dt),
                poe_type=POEType.PROVIDED,
            )
        for crl in self.known_crls:
            yield KnownPOE(
                digest=digest_for_poe(crl.crl_data.dump()),
                poe_time=dt,
                poe_type=POEType.PROVIDED,
                validation_object=ValidationObject(
                    ValidationObjectType.CRL, crl
                ),
            )
        for ocsp in self.known_ocsps:
            yield KnownPOE(
                digest=digest_for_poe(ocsp.ocsp_response_data.dump()),
                poe_time=dt,
                poe_type=POEType.PROVIDED,
                validation_object=ValidationObject(
                    ValidationObjectType.OCSP_RESPONSE, ocsp
                ),
            )
        for cert in self.known_certs:
            yield KnownPOE(
                digest=digest_for_poe(cert.dump()),
                poe_time=dt,
                poe_type=POEType.PROVIDED,
                validation_object=ValidationObject(
                    ValidationObjectType.CERTIFICATE, cert
                ),
            )


@dataclass(frozen=True)
class SignatureValidationSpec:
    cert_validation_policy: CertValidationPolicySpec
    revinfo_gathering_policy: RevocationInfoGatheringSpec = (
        RevocationInfoGatheringSpec()
    )
    ts_cert_validation_policy: Optional[CertValidationPolicySpec] = None
    ac_validation_policy: Optional[CertValidationPolicySpec] = None
    local_knowledge: LocalKnowledge = LocalKnowledge()
    key_usage_settings: KeyUsageConstraints = KeyUsageConstraints()
    signature_algorithm_policy: Optional[CMSAlgorithmUsagePolicy] = None


@dataclass(frozen=True)
class PdfSignatureValidationSpec:
    signature_validation_spec: SignatureValidationSpec
    diff_policy: Optional[DiffPolicy] = DEFAULT_DIFF_POLICY


def _backend_if_necessary(
    hist: bool, rule: RevinfoOnlineFetchingRule, backend: FetcherBackend
) -> Optional[FetcherBackend]:
    if rule == RevinfoOnlineFetchingRule.LOCAL_ONLY:
        return None
    elif rule == RevinfoOnlineFetchingRule.NO_HISTORICAL_FETCH and hist:
        return None
    else:
        return backend


def bootstrap_validation_data_handlers(
    spec: SignatureValidationSpec,
    timing_info: Optional[ValidationTimingInfo] = None,
    is_historical: Optional[bool] = None,
    poe_manager_override: Optional[POEManager] = None,
) -> ValidationDataHandlers:
    if is_historical is None:
        hist = (
            timing_info.point_in_time_validation
            if timing_info is not None
            else False
        )
    else:
        hist = is_historical
    revinfo_policy = spec.revinfo_gathering_policy

    fetcher_backend = _backend_if_necessary(
        hist=hist,
        rule=revinfo_policy.online_fetching_rule,
        backend=revinfo_policy.fetcher_backend,
    )

    knowledge = spec.local_knowledge
    handlers = _certvalidator_bootstrap_handlers(
        fetchers=fetcher_backend,
        crls=knowledge.known_crls,
        ocsps=knowledge.known_ocsps,
        certs=knowledge.known_certs,
        poe_manager=poe_manager_override,
        nonrevoked_assertions=knowledge.nonrevoked_assertions,
    )
    return handlers
