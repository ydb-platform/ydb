import asyncio
import binascii
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set, Union

from asn1crypto import crl, ocsp, x509
from asn1crypto.util import timezone

from .authority import AuthorityWithCert, CertTrustAnchor
from .fetchers import FetcherBackend, Fetchers, default_fetcher_backend
from .fetchers.requests_fetchers import RequestsFetcherBackend
from .ltv.poe import POEManager
from .ltv.types import ValidationTimingInfo, ValidationTimingParams
from .path import ValidationPath
from .policy_decl import (
    AlgorithmUsagePolicy,
    CertRevTrustPolicy,
    DisallowWeakAlgorithmsPolicy,
    NonRevokedStatusAssertion,
    PKIXValidationParams,
    RevocationCheckingPolicy,
)
from .registry import (
    CertificateRegistry,
    PathBuilder,
    SimpleTrustManager,
    TrustManager,
    TrustRootList,
)
from .revinfo.archival import (
    CRLContainer,
    OCSPContainer,
    process_legacy_crl_input,
    process_legacy_ocsp_input,
)
from .revinfo.manager import RevinfoManager


@dataclass(frozen=True)
class ACTargetDescription:
    """
    Value type to guide attribute certificate targeting checks, for
    attribute certificates that use the target information extension.

    As stipulated in RFC 5755, an AC targeting check passes if the
    information in the relevant :class:`.AATargetDescription` matches
    at least one ``Target`` in the AC's target information extension.
    """

    validator_names: List[x509.GeneralName] = field(default_factory=list)
    """
    The validating entity's names.

    This value is matched directly against any ``Target``s that use the
    ``targetName`` alternative.
    """

    group_memberships: List[x509.GeneralName] = field(default_factory=list)
    """
    The validating entity's group memberships.

    This value is matched against any ``Target``s that use the ``targetGroup``
    alternative.
    """


class ValidationContext:
    def __init__(
        self,
        trust_roots: Optional[TrustRootList] = None,
        extra_trust_roots: Optional[TrustRootList] = None,
        other_certs: Optional[Iterable[x509.Certificate]] = None,
        whitelisted_certs: Optional[Iterable[Union[bytes, str]]] = None,
        moment: Optional[datetime] = None,
        best_signature_time: Optional[datetime] = None,
        allow_fetching: bool = False,
        crls: Optional[Iterable[Union[bytes, crl.CertificateList]]] = None,
        ocsps: Optional[Iterable[Union[bytes, ocsp.OCSPResponse]]] = None,
        revocation_mode: str = "soft-fail",
        revinfo_policy: Optional[CertRevTrustPolicy] = None,
        weak_hash_algos: Optional[Iterable[str]] = None,
        time_tolerance: timedelta = timedelta(seconds=1),
        retroactive_revinfo: bool = False,
        fetcher_backend: Optional[FetcherBackend] = None,
        acceptable_ac_targets: Optional[ACTargetDescription] = None,
        poe_manager: Optional[POEManager] = None,
        revinfo_manager: Optional[RevinfoManager] = None,
        certificate_registry: Optional[CertificateRegistry] = None,
        trust_manager: Optional[TrustManager] = None,
        algorithm_usage_policy: Optional[AlgorithmUsagePolicy] = None,
        fetchers: Optional[Fetchers] = None,
    ):
        """
        :param trust_roots:
            If the operating system's trust list should not be used, instead
            pass a list of byte strings containing DER or PEM-encoded X.509
            certificates, or asn1crypto.x509.Certificate objects. These
            certificates will be used as the trust roots for the path being
            built.

        :param extra_trust_roots:
            If the operating system's trust list should be used, but augmented
            with one or more extra certificates. This should be a list of byte
            strings containing DER or PEM-encoded X.509 certificates, or
            asn1crypto.x509.Certificate objects.

        :param other_certs:
            A list of byte strings containing DER or PEM-encoded X.509
            certificates, or a list of asn1crypto.x509.Certificate objects.
            These other certs are usually provided by the service/item being
            validated. In TLS, these would be intermediate chain certs.

        :param whitelisted_certs:
            None or a list of byte strings or unicode strings of the SHA-1
            fingerprint of one or more certificates. The fingerprint is a hex
            encoding of the SHA-1 byte string, optionally separated into pairs
            by spaces or colons. These whilelisted certificates will not be
            checked for validity dates. If one of the certificates is an
            end-entity certificate in a certificate path, any TLS hostname
            mismatches, key usage errors or extended key usage errors will also
            be ignored.

        :param moment:
            If certificate validation should be performed based on a date and
            time other than right now. A datetime.datetime object with a tzinfo
            value. If this parameter is specified, then the only way to check
            OCSP and CRL responses is to pass them via the crls and ocsps
            parameters. Can not be combined with allow_fetching=True.

        :param best_signature_time:
            The presumptive time at which the certificate was used.
            Assumed equal to :class:`moment` if unspecified.

            .. note::
                The difference is significant in some point-in-time validation
                models, where the signature is validated after a
                "cooldown period" of sorts.

        :param crls:
            None or a list/tuple of asn1crypto.crl.CertificateList objects of
            pre-fetched/cached CRLs to be utilized during validation of paths

        :param ocsps:
            None or a list/tuple of asn1crypto.ocsp.OCSPResponse objects of
            pre-fetched/cached OCSP responses to be utilized during validation
            of paths

        :param allow_fetching:
            A bool - if HTTP requests should be made to fetch CRLs and OCSP
            responses. If this is True and certificates contain the location of
            a CRL or OCSP responder, an HTTP request will be made to obtain
            information for revocation checking.

        :param revocation_mode:
            A unicode string of the revocation mode to use: "soft-fail" (the
            default), "hard-fail" or "require". In "soft-fail" mode, any sort of
            error in fetching or locating revocation information is ignored. In
            "hard-fail" mode, if a certificate has a known CRL or OCSP and it
            can not be checked, it is considered a revocation failure. In
            "require" mode, every certificate in the certificate path must have
            a CRL or OCSP.

        :param weak_hash_algos:
            A set of unicode strings of hash algorithms that should be
            considered weak.

        :param time_tolerance:
            Time delta tolerance allowed in validity checks.
            Defaults to one second.

        :param retroactive_revinfo:
            Treat revocation info as retroactively valid, i.e. ignore the
            ``this_update`` field in CRLs and OCSP responses.
            Defaults to ``False``.

            .. warning::
                Be careful with this option, since it will cause incorrect
                behaviour for CAs that make use of certificate holds or other
                reversible revocation methods.
        :param revinfo_manager:
            Internal API, to be elaborated.
        :param trust_manager:
            Internal API, to be elaborated.
        :param certificate_registry:
            Internal API, to be elaborated.
        :param algorithm_usage_policy:
            Internal API, to be elaborated.
        """

        if revinfo_policy is None:
            revinfo_policy = CertRevTrustPolicy(
                RevocationCheckingPolicy.from_legacy(revocation_mode),
                retroactive_revinfo=retroactive_revinfo,
            )
        elif revinfo_policy.expected_post_expiry_revinfo_time is not None:
            raise NotImplementedError(
                "Dealing with post-expiry revocation info has not been "
                "implemented yet."
            )
        self._revinfo_policy = revinfo_policy

        rev_essential = revinfo_policy.revocation_checking_policy.essential
        if (
            not allow_fetching
            and not revinfo_manager
            and crls is None
            and ocsps is None
            and rev_essential
        ):
            raise ValueError(
                "revocation data is not optional and allow_fetching is False, "
                "however crls and ocsps are both None, meaning "
                "that no validation can happen"
            )

        if moment is None:
            moment = datetime.now(timezone.utc)
            point_in_time_validation = False
        elif moment.utcoffset() is None:
            raise ValueError(
                "moment is a naive datetime object, meaning the tzinfo "
                "attribute is not set to a valid timezone"
            )
        else:
            point_in_time_validation = True

        if best_signature_time is None:
            best_signature_time = moment
        elif best_signature_time.utcoffset() is None:
            raise ValueError(
                "best_signature_time is a naive datetime object, meaning the tzinfo "
                "attribute is not set to a valid timezone"
            )

        self._whitelisted_certs: Set[bytes] = set()
        if whitelisted_certs is not None:
            for whitelisted_cert in whitelisted_certs:
                if isinstance(whitelisted_cert, bytes):
                    whitelisted_cert = whitelisted_cert.decode('ascii')
                # Allow users to copy from various OS and browser info dialogs,
                # some of which separate the hex char pairs via spaces or colons
                whitelisted_cert = whitelisted_cert.replace(' ', '').replace(
                    ':', ''
                )
                self._whitelisted_certs.add(
                    binascii.unhexlify(whitelisted_cert.encode('ascii'))
                )

        if algorithm_usage_policy is None:
            if weak_hash_algos is not None:
                algorithm_usage_policy = DisallowWeakAlgorithmsPolicy(
                    frozenset(weak_hash_algos)
                )
            else:
                algorithm_usage_policy = DisallowWeakAlgorithmsPolicy()
        self.algorithm_policy = algorithm_usage_policy

        cert_fetcher = None
        if allow_fetching:
            # not None -> externally managed fetchers
            if fetchers is None:
                # fetcher managed by this validation context,
                # but backend possibly managed externally
                if fetcher_backend is None:
                    # in this case, we load the default requests-based
                    # backend, since the caller doesn't do any resource
                    # management
                    fetcher_backend = default_fetcher_backend()
                fetchers = fetcher_backend.get_fetchers()
            cert_fetcher = fetchers.cert_fetcher
        else:
            fetchers = None

        if certificate_registry is None:
            certificate_registry = CertificateRegistry.build(
                other_certs or (), cert_fetcher=cert_fetcher
            )

        self.certificate_registry: CertificateRegistry = certificate_registry

        if trust_manager is None:
            trust_manager = SimpleTrustManager.build(
                trust_roots=trust_roots, extra_trust_roots=extra_trust_roots
            )
        if isinstance(trust_manager, SimpleTrustManager):
            for root in trust_manager.iter_certs():
                certificate_registry.register(root)

        self.path_builder = PathBuilder(
            trust_manager=trust_manager, registry=certificate_registry
        )
        crls = process_legacy_crl_input(crls) if crls else ()
        ocsps = process_legacy_ocsp_input(ocsps) if ocsps else ()

        if revinfo_manager is None:
            revinfo_manager = RevinfoManager(
                certificate_registry=certificate_registry,
                poe_manager=poe_manager or POEManager(),
                crls=crls,
                ocsps=ocsps,
                fetchers=fetchers,
            )
        self._revinfo_manager = revinfo_manager

        self._validate_map: Dict[bytes, ValidationPath] = {}

        self._soft_fail_exceptions: List[Exception] = []
        time_tolerance = abs(time_tolerance) if time_tolerance else timedelta(0)
        self.timing_params = ValidationTimingParams(
            ValidationTimingInfo(
                validation_time=moment,
                best_signature_time=best_signature_time,
                point_in_time_validation=point_in_time_validation,
            ),
            time_tolerance=time_tolerance,
        )

        self._acceptable_ac_targets = acceptable_ac_targets

    @property
    def revinfo_manager(self) -> RevinfoManager:
        return self._revinfo_manager

    @property
    def revinfo_policy(self) -> CertRevTrustPolicy:
        return self._revinfo_policy

    @property
    def retroactive_revinfo(self) -> bool:
        return self._revinfo_policy.retroactive_revinfo

    @property
    def time_tolerance(self) -> timedelta:
        return self.timing_params.time_tolerance

    @property
    def moment(self) -> datetime:
        return self.timing_params.validation_time

    @property
    def best_signature_time(self) -> datetime:
        return self.timing_params.best_signature_time

    @property
    def fetching_allowed(self) -> bool:
        return self.revinfo_manager.fetching_allowed

    @property
    def crls(self) -> List[crl.CertificateList]:
        """
        A list of all cached :class:`crl.CertificateList` objects
        """
        return self._revinfo_manager.crls

    @property
    def ocsps(self) -> List[ocsp.OCSPResponse]:
        """
        A list of all cached :class:`ocsp.OCSPResponse` objects
        """

        return self._revinfo_manager.ocsps

    @property
    def soft_fail_exceptions(self):
        """
        A list of soft-fail exceptions that were ignored during checks
        """

        return self._soft_fail_exceptions

    def is_whitelisted(self, cert):
        """
        Checks to see if a certificate has been whitelisted

        :param cert:
            An asn1crypto.x509.Certificate object

        :return:
            A bool - if the certificate is whitelisted
        """

        return cert.sha1 in self._whitelisted_certs

    def _report_soft_fail(self, e: Exception):
        self._soft_fail_exceptions.append(e)

    async def async_retrieve_crls(self, cert):
        """
        :param cert:
            An asn1crypto.x509.Certificate object

        :return:
            A list of asn1crypto.crl.CertificateList objects
        """
        results = await self._revinfo_manager.async_retrieve_crls(cert)
        return [res.crl_data for res in results]

    def retrieve_crls(self, cert):
        """
        .. deprecated:: 0.17.0
            Use :meth:`async_retrieve_crls` instead.

        :param cert:
            An asn1crypto.x509.Certificate object

        :return:
            A list of asn1crypto.crl.CertificateList objects
        """

        warnings.warn(
            "'retrieve_crls' is deprecated, use 'async_retrieve_crls' instead",
            DeprecationWarning,
        )
        if not self.revinfo_manager.fetching_allowed:
            return self.revinfo_manager.crls
        return asyncio.run(self.async_retrieve_crls(cert))

    async def async_retrieve_ocsps(self, cert, issuer):
        """
        :param cert:
            An asn1crypto.x509.Certificate object

        :param issuer:
            An asn1crypto.x509.Certificate object of cert's issuer

        :return:
            A list of asn1crypto.ocsp.OCSPResponse objects
        """
        results = await self._revinfo_manager.async_retrieve_ocsps(
            cert, AuthorityWithCert(issuer)
        )
        return [res.ocsp_response_data for res in results]

    def retrieve_ocsps(self, cert, issuer):
        """
        .. deprecated:: 0.17.0
            Use :meth:`async_retrieve_ocsps` instead.

        :param cert:
            An asn1crypto.x509.Certificate object

        :param issuer:
            An asn1crypto.x509.Certificate object of cert's issuer

        :return:
            A list of asn1crypto.ocsp.OCSPResponse objects
        """

        warnings.warn(
            "'retrieve_ocsps' is deprecated, use "
            "'async_retrieve_ocsps' instead",
            DeprecationWarning,
        )

        if not self.revinfo_manager.fetching_allowed:
            return self.revinfo_manager.ocsps
        return asyncio.run(self.async_retrieve_ocsps(cert, issuer))

    def record_validation(self, cert, path):
        """
        Records that a certificate has been validated, along with the path that
        was used for validation. This helps reduce duplicate work when
        validating a ceritifcate and related resources such as CRLs and OCSPs.

        :param cert:
            An ans1crypto.x509.Certificate object

        :param path:
            A pyhanko_certvalidator.path.ValidationPath object
        """

        self._validate_map[cert.signature] = path

    def check_validation(self, cert):
        """
        Checks to see if a certificate has been validated, and if so, returns
        the ValidationPath used to validate it.

        :param cert:
            An asn1crypto.x509.Certificate object

        :return:
            None if not validated, or a pyhanko_certvalidator.path.ValidationPath
            object of the validation path
        """

        if (
            self.path_builder.trust_manager.is_root(cert)
            and cert.signature not in self._validate_map
        ):
            self._validate_map[cert.signature] = ValidationPath(
                trust_anchor=CertTrustAnchor(cert), interm=[], leaf=None
            )

        return self._validate_map.get(cert.signature)

    def clear_validation(self, cert):
        """
        Clears the record that a certificate has been validated

        :param cert:
            An ans1crypto.x509.Certificate object
        """

        if cert.signature in self._validate_map:
            del self._validate_map[cert.signature]

    @property
    def acceptable_ac_targets(self) -> Optional[ACTargetDescription]:
        return self._acceptable_ac_targets


@dataclass(frozen=True)
class ValidationDataHandlers:
    """
    Value class to hold 'manager'/'registry' objects. These are responsible for
    accumulating and exposing various data collections that are relevant
    for certificate validation.
    """

    revinfo_manager: RevinfoManager
    """
    The revocation information manager.
    """

    poe_manager: POEManager
    """
    The proof-of-existence record manager.
    """

    cert_registry: CertificateRegistry
    """
    The certificate registry.

    .. note::
        The certificate registry is a trustless construct. It only holds
        certificates, but does mark them as trusted or store information
        related to how the certificates fit together.
    """


def bootstrap_validation_data_handlers(
    fetchers: Union[Fetchers, FetcherBackend, None] = RequestsFetcherBackend(),
    crls: Iterable[CRLContainer] = (),
    ocsps: Iterable[OCSPContainer] = (),
    certs: Iterable[x509.Certificate] = (),
    poe_manager: Optional[POEManager] = None,
    nonrevoked_assertions: Iterable[NonRevokedStatusAssertion] = (),
) -> ValidationDataHandlers:
    """
    Simple bootstrapping method for a :class:`.ValidationDataHandlers`
    instance with reasonable defaults.

    :param fetchers:
        Data fetcher implementation and/or backend to use.
        If ``None``, remote fetching is disabled. The ``requests``-based
        implementation is the default.
    :param crls:
        Initial collection of CRLs to feed to the revocation info manager.
    :param ocsps:
        Initial collection of OCSP responses to feed to the revocation info
        manager.
    :param certs:
        Initial collection of certificates to add to the certificate registry.
    :param poe_manager:
        Explicit POE manager. Will instantiate an empty one if left unspecified.
    :param nonrevoked_assertions:
        Assertions about the non-revoked status of certain certificates
        that will be taken as true by fiat.
    :return:
        A :class:`.ValidationDataHandlers` object.
    """

    _fetchers: Optional[Fetchers]
    if isinstance(fetchers, FetcherBackend):
        _fetchers = fetchers.get_fetchers()
    elif isinstance(fetchers, Fetchers):
        _fetchers = fetchers
    else:
        _fetchers = None

    poe_manager = poe_manager or POEManager()
    cert_registry = CertificateRegistry(
        cert_fetcher=_fetchers.cert_fetcher if _fetchers is not None else None
    )
    cert_registry.register_multiple(certs)
    revinfo_manager = RevinfoManager(
        certificate_registry=cert_registry,
        poe_manager=poe_manager,
        crls=crls,
        ocsps=ocsps,
        fetchers=_fetchers,
        assertions=nonrevoked_assertions,
    )
    return ValidationDataHandlers(
        revinfo_manager=revinfo_manager,
        poe_manager=poe_manager,
        cert_registry=cert_registry,
    )


@dataclass(frozen=True)
class CertValidationPolicySpec:
    """
    Policy object describing how to validate certificates at a high
    level.

    .. note::
        A certificate validation policy differs from a validation context
        in that :class:`ValidationContext` objects keep state as well.
        This is not the case for a certificate validation policy, which makes
        them suitable for reuse in complex validation workflows where the
        same policy needs to be applied independently in multiple steps.

    .. warning::
        While a certification policy spec is intended to be stateless,
        some of its fields are abstract classes. As such, the true behaviour
        may depend on the underlying implementation.
    """

    trust_manager: TrustManager
    """
    The trust manager that defines this policy's trust anchors.
    """

    revinfo_policy: CertRevTrustPolicy
    """
    The policy describing how to handle certificate revocation and associated
    revocation information.
    """

    time_tolerance: timedelta = timedelta(seconds=1)
    """
    The time drift tolerated during validation. Defaults to one second.
    """

    acceptable_ac_targets: Optional[ACTargetDescription] = None
    """
    Targets to accept when evaluating the scope of an attribute certificate.
    """

    algorithm_usage_policy: Optional[AlgorithmUsagePolicy] = field(
        default=DisallowWeakAlgorithmsPolicy()
    )
    """
    Policy on cryptographic algorithm usage. If left unspecified, a
    default will be used.
    """

    pkix_validation_params: Optional[PKIXValidationParams] = None
    """
    The PKIX validation parameters to use, as defined in :rfc:`5280`.
    """

    def build_validation_context(
        self,
        timing_info: ValidationTimingInfo,
        handlers: Optional[ValidationDataHandlers],
    ) -> ValidationContext:
        """
        Build a validation context from this policy, validation timing info
        and a set of validation data handlers.

        :param timing_info:
            Timing settings.
        :param handlers:
            Optionally specify validation data handlers. A reasonable default
            will be supplied if absent.
        :return:
            A new :class:`ValidationContext` reflecting the parameters.
        """

        if handlers is None:
            cert_registry = CertificateRegistry()
            poe_manager = POEManager()
            revinfo_manager = RevinfoManager(
                certificate_registry=cert_registry,
                poe_manager=poe_manager,
                crls=[],
                ocsps=[],
            )
        else:
            cert_registry = handlers.cert_registry
            poe_manager = handlers.poe_manager
            revinfo_manager = handlers.revinfo_manager

        return ValidationContext(
            trust_manager=self.trust_manager,
            revinfo_policy=self.revinfo_policy,
            revinfo_manager=revinfo_manager,
            certificate_registry=cert_registry,
            poe_manager=poe_manager,
            algorithm_usage_policy=self.algorithm_usage_policy,
            moment=timing_info.validation_time,
            best_signature_time=timing_info.best_signature_time,
            time_tolerance=self.time_tolerance,
            acceptable_ac_targets=self.acceptable_ac_targets,
            allow_fetching=revinfo_manager.fetching_allowed,
        )
