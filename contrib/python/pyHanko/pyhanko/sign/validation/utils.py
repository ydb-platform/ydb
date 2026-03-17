import abc
from datetime import datetime
from typing import Optional

from asn1crypto import algos, cms, keys, x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.dsa import DSAPublicKey
from cryptography.hazmat.primitives.asymmetric.ec import (
    ECDSA,
    EllipticCurvePublicKey,
)
from cryptography.hazmat.primitives.asymmetric.ed448 import Ed448PublicKey
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from pyhanko_certvalidator.policy_decl import (
    AlgorithmUsageConstraint,
    AlgorithmUsagePolicy,
    DisallowWeakAlgorithmsPolicy,
)

from ..general import (
    MultivaluedAttributeError,
    NonexistentAttributeError,
    find_unique_cms_attribute,
    get_pyca_cryptography_hash_for_signing,
    process_pss_params,
)
from .errors import DisallowedAlgorithmError, SignatureValidationError


def _ensure_digest_match(
    signature_algo: algos.SignedDigestAlgorithm,
    message_digest_algo: algos.DigestAlgorithm,
) -> AlgorithmUsageConstraint:
    if signature_algo['algorithm'].native == 'ed448':
        # be a bit more tolerant here, also don't check parameters because
        # we only support one length anyway
        algo = message_digest_algo['algorithm'].native
        if algo in ('shake256', 'shake256_len'):
            return AlgorithmUsageConstraint(allowed=True)
        else:
            return AlgorithmUsageConstraint(
                allowed=False,
                failure_reason=(
                    f"Digest algorithm {algo} "
                    f"does not match value implied by signature algorithm ed448"
                ),
            )

    try:
        sig_hash_algo_obj = algos.DigestAlgorithm(
            {'algorithm': signature_algo.hash_algo}
        )
    except ValueError:
        sig_hash_algo_obj = None

    if (
        sig_hash_algo_obj is not None
        and sig_hash_algo_obj.dump() != message_digest_algo.dump()
    ):
        return AlgorithmUsageConstraint(
            allowed=False,
            failure_reason=(
                f"Digest algorithm {message_digest_algo['algorithm'].native} "
                f"does not match value implied by signature algorithm "
                f"{signature_algo['algorithm'].native}"
            ),
        )
    return AlgorithmUsageConstraint(allowed=True)


class CMSAlgorithmUsagePolicy(AlgorithmUsagePolicy, abc.ABC):
    """
    Algorithm usage policy for CMS signatures.
    """

    def digest_combination_allowed(
        self,
        signature_algo: algos.SignedDigestAlgorithm,
        message_digest_algo: algos.DigestAlgorithm,
        moment: Optional[datetime],
    ) -> AlgorithmUsageConstraint:
        """
        Verify whether a digest algorithm is compatible with the digest
        algorithm implied by the provided signature algorithm, if any.

        By default, this enforces the convention (requirement in RFC 8933) that
        the message digest must be computed using the same digest algorithm
        as the one used by the signature, if applicable.

        Checking whether the individual algorithms are allowed is not the
        responsibility of this method.

        :param signature_algo:
            A signature mechanism to use
        :param message_digest_algo:
            The digest algorithm used for the message digest
        :param moment:
            The point in time for which the assessment needs to be made.
        :return:
            A usage constraint.
        """
        return _ensure_digest_match(signature_algo, message_digest_algo)

    @staticmethod
    def lift_policy(policy: AlgorithmUsagePolicy) -> 'CMSAlgorithmUsagePolicy':
        """
        Lift a 'base' :class:`.AlgorithmUsagePolicy` to a CMS usage algorithm
        policy with default settings. If the policy passed in is already
        a :class:`.CMSAlgorithmUsagePolicy`, return it as-is.

        :param policy:
            The underlying original policy
        :return:
            The lifted policy
        """
        if isinstance(policy, CMSAlgorithmUsagePolicy):
            return policy
        else:
            return _DefaultPolicyMixin(policy)


class _DefaultPolicyMixin(CMSAlgorithmUsagePolicy):
    def __init__(self, underlying_policy: AlgorithmUsagePolicy):
        self._policy = underlying_policy

    def digest_algorithm_allowed(
        self, algo: algos.DigestAlgorithm, moment: Optional[datetime]
    ) -> AlgorithmUsageConstraint:
        return self._policy.digest_algorithm_allowed(algo, moment)

    def signature_algorithm_allowed(
        self,
        algo: algos.SignedDigestAlgorithm,
        moment: Optional[datetime],
        public_key: Optional[keys.PublicKeyInfo],
    ) -> AlgorithmUsageConstraint:
        return self._policy.signature_algorithm_allowed(
            algo, moment, public_key
        )


DEFAULT_WEAK_HASH_ALGORITHMS = frozenset({'sha1', 'md5', 'md2'})

DEFAULT_ALGORITHM_USAGE_POLICY = CMSAlgorithmUsagePolicy.lift_policy(
    DisallowWeakAlgorithmsPolicy(DEFAULT_WEAK_HASH_ALGORITHMS)
)


def validate_raw(
    signature: bytes,
    signed_data: bytes,
    cert: x509.Certificate,
    signature_algorithm: algos.SignedDigestAlgorithm,
    md_algorithm: str,
    prehashed=False,
    algorithm_policy: Optional[
        CMSAlgorithmUsagePolicy
    ] = DEFAULT_ALGORITHM_USAGE_POLICY,
    time_indic: Optional[datetime] = None,
):
    """
    Validate a raw signature. Internal API.
    """
    if algorithm_policy is not None:
        sig_algo_allowed = algorithm_policy.signature_algorithm_allowed(
            signature_algorithm, moment=time_indic, public_key=cert.public_key
        )
        if not sig_algo_allowed:
            msg = (
                f"Signature algorithm "
                f"{signature_algorithm['algorithm'].native} is not allowed "
                f"by the current usage policy."
            )
            if sig_algo_allowed.failure_reason is not None:
                msg += f" Reason: {sig_algo_allowed.failure_reason}"
            raise DisallowedAlgorithmError(
                msg, permanent=sig_algo_allowed.not_allowed_after is None
            )

        digest_compatible = algorithm_policy.digest_combination_allowed(
            signature_algo=signature_algorithm,
            message_digest_algo=algos.DigestAlgorithm(
                {'algorithm': md_algorithm}
            ),
            moment=None,
        )
        if not digest_compatible:
            raise DisallowedAlgorithmError(
                failure_message=digest_compatible.failure_reason,
                permanent=digest_compatible.not_allowed_after is None,
            )

    try:
        verify_md_algo = signature_algorithm.hash_algo
    except ValueError:
        verify_md_algo = md_algorithm

    verify_md = get_pyca_cryptography_hash_for_signing(
        verify_md_algo, prehashed=prehashed
    )

    pub_key = serialization.load_der_public_key(cert.public_key.dump())

    sig_algo = signature_algorithm.signature_algo
    if sig_algo == 'rsassa_pkcs1v15':
        assert isinstance(pub_key, RSAPublicKey)
        pub_key.verify(signature, signed_data, padding.PKCS1v15(), verify_md)
    elif sig_algo == 'rsassa_pss':
        assert isinstance(pub_key, RSAPublicKey)
        pss_padding, hash_algo = process_pss_params(
            signature_algorithm['parameters'], md_algorithm, prehashed=prehashed
        )
        pub_key.verify(signature, signed_data, pss_padding, hash_algo)
    elif sig_algo == 'dsa':
        assert isinstance(pub_key, DSAPublicKey)
        pub_key.verify(signature, signed_data, verify_md)
    elif sig_algo == 'ecdsa':
        assert isinstance(pub_key, EllipticCurvePublicKey)
        pub_key.verify(signature, signed_data, ECDSA(verify_md))
    elif sig_algo in 'ed25519':
        assert isinstance(pub_key, Ed25519PublicKey)
        pub_key.verify(signature, signed_data)
    elif sig_algo in 'ed448':
        assert isinstance(pub_key, Ed448PublicKey)
        pub_key.verify(signature, signed_data)
    else:  # pragma: nocover
        raise NotImplementedError(
            f"Signature mechanism {sig_algo} is not supported."
        )


def extract_message_digest(signer_info: cms.SignerInfo):
    try:
        embedded_digest = find_unique_cms_attribute(
            signer_info['signed_attrs'], 'message_digest'
        )
        return embedded_digest.native
    except (NonexistentAttributeError, MultivaluedAttributeError):
        raise SignatureValidationError(
            'Message digest not found in signature, or multiple message '
            'digest attributes present.'
        )
