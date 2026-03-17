"""
.. versionadded:: 0.20.0
"""

import abc
import enum
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import FrozenSet, Iterable, Optional

from asn1crypto import algos, keys

from .name_trees import PKIXSubtrees

__all__ = [
    'RevocationCheckingRule',
    'RevocationCheckingPolicy',
    'FreshnessReqType',
    'CertRevTrustPolicy',
    'PKIXValidationParams',
    'AlgorithmUsageConstraint',
    'AlgorithmUsagePolicy',
    'DisallowWeakAlgorithmsPolicy',
    'AcceptAllAlgorithms',
    'NonRevokedStatusAssertion',
    'DEFAULT_WEAK_HASH_ALGOS',
    'REQUIRE_REVINFO',
    'NO_REVOCATION',
]


DEFAULT_WEAK_HASH_ALGOS = frozenset(['md2', 'md5', 'sha1'])
"""
Digest algorithms considered weak by default.
"""

FRESHNESS_FALLBACK_VALIDITY_DEFAULT = timedelta(minutes=30)
"""
Default freshness used by the default/legacy freshness policy
when the revocation information does not specify a next update
time. In practice this only applies to OCSP responses.
"""


@dataclass(frozen=True)
class NonRevokedStatusAssertion:
    """
    Assert that a certificate was not revoked at some given date.
    """

    cert_sha256: bytes
    """
    SHA-256 hash of the certificate.
    """

    at: datetime
    """
    Moment in time at which the assertion is to be considered valid.
    """


@enum.unique
class RevocationCheckingRule(enum.Enum):
    """
    Rules determining in what circumstances revocation data has to be checked,
    and what kind.
    """

    # yes, this is consistently misspelled in all parts of the
    # ETSI TS 119 172 series...
    CRL_REQUIRED = "clrcheck"
    """
    Check CRLs.
    """

    OCSP_REQUIRED = "ocspcheck"
    """
    Check OCSP.
    """

    CRL_AND_OCSP_REQUIRED = "bothcheck"
    """
    Check CRL and OCSP.
    """

    CRL_OR_OCSP_REQUIRED = "eithercheck"
    """
    Check CRL or OCSP.
    """

    NO_CHECK = "nocheck"
    """
    Do not check.
    """

    CHECK_IF_DECLARED = "ifdeclaredcheck"
    """
    Check revocation information if declared in the certificate.
    
    .. warning::
        This is not an ESI check type, but is preserved for 
        compatibility with the 'hard-fail' mode in certvalidator.

    .. note::
        In this mode, cached CRLs will _not_ be checked if the certificate
        does not list any distribution points.
    """

    CHECK_IF_DECLARED_SOFT = "ifdeclaredsoftcheck"
    """
    Check revocation information if declared in the certificate, but
    do not fail validation if the check fails.

    .. warning::
        This is not an ESI check type, but is preserved for 
        compatibility with the 'soft-fail' mode in certvalidator.

    .. note::
        In this mode, cached CRLs will _not_ be checked if the certificate
        does not list any distribution points.
    """

    @property
    def strict(self) -> bool:
        # note that this is not quite the same as (not self.tolerant)!
        return self not in (
            RevocationCheckingRule.CHECK_IF_DECLARED,
            RevocationCheckingRule.CHECK_IF_DECLARED_SOFT,
            RevocationCheckingRule.NO_CHECK,
        )

    @property
    def tolerant(self) -> bool:
        return self in (
            RevocationCheckingRule.CHECK_IF_DECLARED_SOFT,
            RevocationCheckingRule.NO_CHECK,
        )

    @property
    def crl_mandatory(self) -> bool:
        return self in (
            RevocationCheckingRule.CRL_REQUIRED,
            RevocationCheckingRule.CRL_AND_OCSP_REQUIRED,
        )

    @property
    def crl_relevant(self) -> bool:
        return self not in (
            RevocationCheckingRule.NO_CHECK,
            RevocationCheckingRule.OCSP_REQUIRED,
        )

    @property
    def ocsp_mandatory(self) -> bool:
        return self in (
            RevocationCheckingRule.OCSP_REQUIRED,
            RevocationCheckingRule.CRL_AND_OCSP_REQUIRED,
        )

    @property
    def ocsp_relevant(self) -> bool:
        return self not in (
            RevocationCheckingRule.NO_CHECK,
            RevocationCheckingRule.CRL_REQUIRED,
        )


@dataclass(frozen=True)
class RevocationCheckingPolicy:
    """
    Class describing a revocation checking policy
    based on the types defined in the ETSI TS 119 172 series.
    """

    ee_certificate_rule: RevocationCheckingRule
    """
    Revocation rule applied to end-entity certificates.
    """

    intermediate_ca_cert_rule: RevocationCheckingRule
    """
    Revocation rule applied to certificates further up the path.
    """

    @classmethod
    def from_legacy(cls, policy: str):
        try:
            return LEGACY_POLICY_MAP[policy]
        except KeyError:
            raise ValueError(f"'{policy}' is not a valid revocation mode")

    @property
    def essential(self) -> bool:
        return not (
            self.ee_certificate_rule.tolerant
            and self.ee_certificate_rule.tolerant
        )


REQUIRE_REVINFO = RevocationCheckingPolicy(
    RevocationCheckingRule.CRL_OR_OCSP_REQUIRED,
    RevocationCheckingRule.CRL_OR_OCSP_REQUIRED,
)
"""
Policy indicating that revocation information is always required, but either
OCSP or CRL-based revocation information is OK.
"""


NO_REVOCATION = RevocationCheckingPolicy(
    ee_certificate_rule=RevocationCheckingRule.NO_CHECK,
    intermediate_ca_cert_rule=RevocationCheckingRule.NO_CHECK,
)
"""
Policy indicating that revocation information is never required.
"""


LEGACY_POLICY_MAP = {
    'soft-fail': RevocationCheckingPolicy(
        RevocationCheckingRule.CHECK_IF_DECLARED_SOFT,
        RevocationCheckingRule.CHECK_IF_DECLARED_SOFT,
    ),
    'hard-fail': RevocationCheckingPolicy(
        RevocationCheckingRule.CHECK_IF_DECLARED,
        RevocationCheckingRule.CHECK_IF_DECLARED,
    ),
    'require': REQUIRE_REVINFO,
}
"""
Mapping of legacy ``certvalidator`` revocation modes to
:class:`RevocationCheckingPolicy` objects.
"""


@enum.unique
class FreshnessReqType(enum.Enum):
    """
    Freshness requirement type.
    """

    DEFAULT = enum.auto()
    """
    The default freshness policy, i.e. the ``certvalidator`` legacy policy.
    This policy considers revocation info valid between its ``thisUpdate``
    and ``nextUpdate`` times, but not outside of that window.
    """

    MAX_DIFF_REVOCATION_VALIDATION = enum.auto()
    """
    Freshness policy requiring that the validation time, if later than the
    issuance date of the revocation info, be sufficiently close to that
    issuance date.
    """

    TIME_AFTER_SIGNATURE = enum.auto()
    """
    Freshness policy requiring that the revocation info be issued after a
    predetermined "cooldown period" after the certificate was used to produce
    a signature.
    """


@dataclass(frozen=True)
class CertRevTrustPolicy:
    """
    Class describing conditions for trusting revocation info.
    Based on CertificateRevTrust in ETSI TS 119 172-3.
    """

    revocation_checking_policy: RevocationCheckingPolicy
    """
    The revocation checking policy requirements.
    """

    freshness: Optional[timedelta] = None
    """
    Freshness interval. If not specified, this defaults to the distance
    between ``thisUpdate`` and ``nextUpdate`` for the given piece of revocation
    information.
    If the ``nextUpdate`` field is not present, then the effective default
    is 30 minutes.
    """

    freshness_req_type: FreshnessReqType = FreshnessReqType.DEFAULT
    """
    Controls whether the freshness requirement applies relatively to the
    signing time or to the validation time.
    """

    expected_post_expiry_revinfo_time: Optional[timedelta] = None
    """
    Duration for which the issuing CA is expected to supply status information
    after a certificate expires.
    """

    retroactive_revinfo: bool = False
    """
    Treat revocation info as retroactively valid, i.e. ignore the
    ``this_update`` field in CRLs and OCSP responses.
    This parameter is not taken into account for freshness policies other than
    :attr:`FreshnessReqType.DEFAULT`, and is ``False`` by default in those
    cases.

    .. warning::
        Be careful with this option, since it will cause incorrect
        behaviour for CAs that make use of certificate holds or other
        reversible revocation methods.
    """


def intersect_policy_sets(
    a_pols: FrozenSet[str], b_pols: FrozenSet[str]
) -> FrozenSet[str]:
    """
    Intersect two sets of policies, taking into account the special
    'any_policy'.

    :param a_pols:
        A set of policies.
    :param b_pols:
        Another set of policies.
    :return:
        The intersection of both.
    """

    a_any = 'any_policy' in a_pols
    b_any = 'any_policy' in b_pols

    if a_any and b_any:
        return frozenset(['any_policy'])
    elif a_any:
        return b_pols
    elif b_any:
        return b_pols
    else:
        return b_pols & a_pols


@dataclass(frozen=True)
class PKIXValidationParams:
    user_initial_policy_set: frozenset = frozenset(['any_policy'])
    """
    Set of policies that the user is willing to accept. By default, any policy
    is acceptable.
    
    When setting this parameter to a non-default value, you probably want to
    set :attr:`initial_explicit_policy` as well.
    
    .. note::
        These are specified in the policy domain of the trust root(s), and
        subject to policy mapping by intermediate certificate authorities.
    """

    initial_policy_mapping_inhibit: bool = False
    """
    Flag indicating whether policy mapping is forbidden along the entire    
    certification chains. By default, policy mapping is permitted.
    
    .. note::
        Policy constraints on intermediate certificates may force policy mapping
        to be inhibited from some point onwards.
    """

    initial_explicit_policy: bool = False
    """
    Flag indicating whether path validation must terminate with at least one
    permissible policy; see :attr:`user_initial_policy_set`.
    By default, no such requirement is imposed.
    
    .. note::
        If :attr:`user_initial_policy_set` is set to its default value of
        ``{'any_policy'}``, the effect is that the path validation must accept
        at least one policy, without specifying which.
        
    .. warning::
        Due to widespread mis-specification of policy extensions in the wild,
        many real-world certification chains terminate with an empty set
        (or rather, tree) of valid policies. Therefore, this flag is set to 
        ``False`` by default.
    """

    initial_any_policy_inhibit: bool = False
    """
    Flag indicating whether ``anyPolicy`` should be left unprocessed when it
    appears in a certificate. By default, ``anyPolicy`` is always processed
    when it appears.
    """

    initial_permitted_subtrees: Optional[PKIXSubtrees] = None
    """
    Set of permitted subtrees for each name type, indicating restrictions
    to impose on subject names (and alternative names) in the certification
    path.
    
    By default, all names are permitted.
    This behaviour can be modified by name constraints on intermediate CA
    certificates.
    """

    initial_excluded_subtrees: Optional[PKIXSubtrees] = None
    """
    Set of excluded subtrees for each name type, indicating restrictions
    to impose on subject names (and alternative names) in the certification
    path.

    By default, no names are excluded.
    This behaviour can be modified by name constraints on intermediate CA
    certificates.
    """

    def merge(self, other: 'PKIXValidationParams') -> 'PKIXValidationParams':
        """
        Combine the conditions of these PKIX validation params with another
        set of parameters, producing the most lenient set of parameters that
        is stricter than both inputs.

        :param other:
            Another set of PKIX validation parameters.
        :return:
            A combined set of PKIX validation parameters.
        """

        if 'any_policy' in self.user_initial_policy_set:
            init_policy_set = other.user_initial_policy_set
        elif 'any_policy' in other.user_initial_policy_set:
            init_policy_set = self.user_initial_policy_set
        else:
            init_policy_set = (
                other.user_initial_policy_set & self.user_initial_policy_set
            )

        initial_any_policy_inhibit = (
            self.initial_any_policy_inhibit and other.initial_any_policy_inhibit
        )
        initial_explicit_policy = (
            self.initial_explicit_policy and other.initial_explicit_policy
        )
        initial_policy_mapping_inhibit = (
            self.initial_policy_mapping_inhibit
            and other.initial_policy_mapping_inhibit
        )
        return PKIXValidationParams(
            user_initial_policy_set=init_policy_set,
            initial_any_policy_inhibit=initial_any_policy_inhibit,
            initial_explicit_policy=initial_explicit_policy,
            initial_policy_mapping_inhibit=initial_policy_mapping_inhibit,
        )


@dataclass(frozen=True)
class AlgorithmUsageConstraint:
    """
    Expression of a constraint on the usage of an algorithm (possibly with
    parameter choices).
    """

    allowed: bool
    """
    Flag indicating whether the algorithm can be used.
    """

    not_allowed_after: Optional[datetime] = None
    """
    Date indicating when the algorithm became unavailable (given the relevant
    choice of parameters, if applicable).
    """

    failure_reason: Optional[str] = None
    """
    A human-readable description of the failure reason, if applicable.
    """

    def __bool__(self):
        return self.allowed


class AlgorithmUsagePolicy(abc.ABC):
    """
    Abstract interface defining a usage policy for cryptographic algorithms.
    """

    def digest_algorithm_allowed(
        self, algo: algos.DigestAlgorithm, moment: Optional[datetime]
    ) -> AlgorithmUsageConstraint:
        """
        Determine if the indicated digest algorithm can be used at the point
        in time indicated.

        :param algo:
            A digest algorithm description in ASN.1 form.
        :param moment:
            The point in time at which the algorithm should be usable.
            If ``None``, then the returned judgment applies at all times.
        :return:
            A :class:`.AlgorithmUsageConstraint` expressing the judgment.
        """
        raise NotImplementedError

    def signature_algorithm_allowed(
        self,
        algo: algos.SignedDigestAlgorithm,
        moment: Optional[datetime],
        public_key: Optional[keys.PublicKeyInfo],
    ) -> AlgorithmUsageConstraint:
        """
        Determine if the indicated signature algorithm (including the associated
        digest function and any parameters, if applicable) can be used at the
        point in time indicated.

        :param algo:
            A signature mechanism description in ASN.1 form.
        :param moment:
            The point in time at which the algorithm should be usable.
            If ``None``, then the returned judgment applies at all times.
        :param public_key:
            The public key associated with the operation, if available.

            .. note::
                This parameter can be used to enforce key size limits or
                to filter out keys with known structural weaknesses.
        :return:
            A :class:`.AlgorithmUsageConstraint` expressing the judgment.
        """
        raise NotImplementedError


class DisallowWeakAlgorithmsPolicy(AlgorithmUsagePolicy):
    """
    Primitive usage policy that forbids a list of user-specified
    "weak" algorithms and allows everything else.
    It also ignores the time parameter completely.

    .. note::
        This denial-based strategy is supplied to provide a backwards-compatible
        default.
        In many scenarios, an explicit allow-based strategy is more appropriate.
        Users with specific security requirements are encouraged to implement
        :class:`.AlgorithmUsagePolicy` themselves.

    :param weak_hash_algos:
        The list of digest algorithms considered weak.
        Defaults to :const:`.DEFAULT_WEAK_HASH_ALGOS`.
    :param weak_signature_algos:
        The list of digest algorithms considered weak.
        Defaults to the empty set.
    :param rsa_key_size_threshold:
        The key length threshold for RSA keys, in bits.
    :param dsa_key_size_threshold:
        The key length threshold for DSA keys, in bits.
    """

    def __init__(
        self,
        weak_hash_algos=DEFAULT_WEAK_HASH_ALGOS,
        weak_signature_algos=frozenset(),
        rsa_key_size_threshold=2048,
        # TODO is this a reasonable default?
        dsa_key_size_threshold=3192,
    ):
        self.weak_hash_algos = weak_hash_algos
        self.weak_signature_algos = weak_signature_algos
        self.rsa_key_size_threshold = rsa_key_size_threshold
        self.dsa_key_size_threshold = dsa_key_size_threshold

    def digest_algorithm_allowed(
        self, algo: algos.DigestAlgorithm, moment: Optional[datetime]
    ) -> AlgorithmUsageConstraint:
        return AlgorithmUsageConstraint(
            algo['algorithm'].native not in self.weak_hash_algos
        )

    def signature_algorithm_allowed(
        self,
        algo: algos.SignedDigestAlgorithm,
        moment: Optional[datetime],
        public_key: Optional[keys.PublicKeyInfo],
    ) -> AlgorithmUsageConstraint:
        algo_name = algo.signature_algo
        algo_allowed = algo_name not in self.weak_signature_algos
        is_rsa = algo_name.startswith('rsa')
        is_dsa = algo_name == 'dsa'
        if algo_allowed and public_key is not None and (is_rsa or is_dsa):
            key_sz = public_key.bit_size
            failed_threshold = None
            if is_rsa and key_sz < self.rsa_key_size_threshold:
                failed_threshold = self.rsa_key_size_threshold
            elif is_dsa and key_sz < self.dsa_key_size_threshold:
                failed_threshold = self.dsa_key_size_threshold
            if failed_threshold is not None:
                return AlgorithmUsageConstraint(
                    allowed=False,
                    failure_reason=(
                        f"Key size {key_sz} for algorithm {algo_name} is "
                        f"considered too small; "
                        f"policy mandates >= {failed_threshold}"
                    ),
                )
        try:
            hash_algo = algo.hash_algo
        except ValueError:
            hash_algo = None
        if algo_allowed and hash_algo is not None:
            digest_allowed = self.digest_algorithm_allowed(
                algos.DigestAlgorithm({'algorithm': algo.hash_algo}), moment
            )
            if not digest_allowed:
                return AlgorithmUsageConstraint(
                    allowed=False,
                    failure_reason=(
                        f"Digest algorithm {digest_allowed} is not allowed, "
                        f"which disqualifies the signature mechanism "
                        f"{algo['algorithm'].native} as well."
                    ),
                    not_allowed_after=digest_allowed.not_allowed_after,
                )
        return AlgorithmUsageConstraint(allowed=algo_allowed)


class AcceptAllAlgorithms(AlgorithmUsagePolicy):
    def digest_algorithm_allowed(
        self, algo: algos.DigestAlgorithm, moment: Optional[datetime]
    ) -> AlgorithmUsageConstraint:
        return AlgorithmUsageConstraint(allowed=True)

    def signature_algorithm_allowed(
        self,
        algo: algos.SignedDigestAlgorithm,
        moment: Optional[datetime],
        public_key: Optional[keys.PublicKeyInfo],
    ) -> AlgorithmUsageConstraint:
        return AlgorithmUsageConstraint(allowed=True)
