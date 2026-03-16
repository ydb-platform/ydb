from datetime import datetime
from typing import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
)

from asn1crypto import crl, ocsp, x509
from pyhanko_certvalidator.authority import Authority
from pyhanko_certvalidator.errors import CRLFetchError, OCSPFetchError
from pyhanko_certvalidator.fetchers import Fetchers
from pyhanko_certvalidator.ltv.poe import (
    KnownPOE,
    POEManager,
    POEType,
    ValidationObject,
    ValidationObjectType,
    digest_for_poe,
)
from pyhanko_certvalidator.policy_decl import NonRevokedStatusAssertion
from pyhanko_certvalidator.registry import CertificateRegistry
from pyhanko_certvalidator.revinfo.archival import (
    CRLContainer,
    OCSPContainer,
    sort_freshest_first,
)


class RevinfoManager:
    """
    .. versionadded:: 0.20.0

    Class to manage and potentially fetch revocation information.

    :param certificate_registry:
        The associated certificate registry.
    :param poe_manager:
        The proof-of-existence (POE) data manager.
    :param crls:
        CRL data.
    :param ocsps:
        OCSP response data.
    :param fetchers:
        Fetchers for collecting revocation information.
        If ``None``, no fetching will be performed.
    """

    def __init__(
        self,
        certificate_registry: CertificateRegistry,
        poe_manager: POEManager,
        crls: Iterable[CRLContainer],
        ocsps: Iterable[OCSPContainer],
        assertions: Iterable[NonRevokedStatusAssertion] = (),
        fetchers: Optional[Fetchers] = None,
    ):
        self._certificate_registry = certificate_registry
        self._poe_manager = poe_manager

        self._revocation_certs: Dict[bytes, x509.Certificate] = {}
        self._crl_issuer_map: Dict[bytes, x509.Certificate] = {}

        self._crls: List[CRLContainer] = []
        if crls:
            self._crls = sort_freshest_first(crls)

        self._ocsps: List[OCSPContainer] = []
        if ocsps:
            self._ocsps = ocsps = sort_freshest_first(ocsps)
            for ocsp_response in ocsps:
                self._extract_ocsp_certs(ocsp_response)

        self._fetchers = fetchers
        self._assertions: Dict[bytes, NonRevokedStatusAssertion] = {
            assertion.cert_sha256: assertion for assertion in assertions
        }

    @property
    def poe_manager(self) -> POEManager:
        """
        The proof-of-existence (POE) data manager.
        """
        return self._poe_manager

    @property
    def certificate_registry(self) -> CertificateRegistry:
        """
        The associated certificate registry.
        """
        return self._certificate_registry

    @property
    def fetching_allowed(self) -> bool:
        """
        Boolean indicating whether fetching is allowed.
        """
        return self._fetchers is not None

    @property
    def crls(self) -> List[crl.CertificateList]:
        """
        A list of all cached :class:`crl.CertificateList` objects
        """

        raw_crls = [cont.crl_data for cont in self._crls]
        if not self._fetchers:
            return raw_crls
        return list(self._fetchers.crl_fetcher.fetched_crls()) + raw_crls

    @property
    def ocsps(self) -> List[ocsp.OCSPResponse]:
        """
        A list of all cached :class:`ocsp.OCSPResponse` objects
        """

        raw_ocsps = [cont.ocsp_response_data for cont in self._ocsps]
        if not self._fetchers:
            return raw_ocsps

        return list(self._fetchers.ocsp_fetcher.fetched_responses()) + raw_ocsps

    @property
    def new_revocation_certs(self) -> List[x509.Certificate]:
        """
        A list of newly-fetched :class:`x509.Certificate` objects that were
        obtained from OCSP responses and CRLs
        """

        return list(self._revocation_certs.values())

    def _extract_ocsp_certs(self, ocsp_response: OCSPContainer):
        """
        Extracts any certificates included with an OCSP response and adds them
        to the certificate registry

        :param ocsp_response:
            An asn1crypto.ocsp.OCSPResponse object to look for certs inside of
        """

        poe_man = self._poe_manager
        ocsp_poe_time = poe_man[ocsp_response]

        registry = self._certificate_registry
        revo_certs = self._revocation_certs

        basic = ocsp_response.extract_basic_ocsp_response()
        if basic is not None and basic['certs']:
            for other_cert in basic['certs']:
                if registry.register(other_cert):
                    revo_certs[other_cert.issuer_serial] = other_cert
                    poe_man.register_known_poe(
                        KnownPOE(
                            poe_type=POEType.VALIDATION,
                            digest=digest_for_poe(other_cert.dump()),
                            # register with the same POE time as the OCSP
                            # response
                            poe_time=ocsp_poe_time,
                            validation_object=ValidationObject(
                                object_type=ValidationObjectType.CERTIFICATE,
                                value=other_cert,
                            ),
                        )
                    )

    def record_crl_issuer(self, certificate_list, cert):
        """
        Records the certificate that issued a certificate list. Used to reduce
        processing code when dealing with self-issued certificates and multiple
        CRLs.

        :param certificate_list:
            An ans1crypto.crl.CertificateList object

        :param cert:
            An ans1crypto.x509.Certificate object
        """

        self._crl_issuer_map[certificate_list.signature] = cert

    def check_crl_issuer(self, certificate_list) -> Optional[x509.Certificate]:
        """
        Checks to see if the certificate that signed a certificate list has
        been found

        :param certificate_list:
            An ans1crypto.crl.CertificateList object

        :return:
            None if not found, or an asn1crypto.x509.Certificate object of the
            issuer
        """

        return self._crl_issuer_map.get(certificate_list.signature)

    def currently_available_crls(self) -> List[CRLContainer]:
        """
        .. versionadded:: 0.27.0

        Return all currently available CRLs.

        :return:
            A list of :class:`CRLContainer` objects
        """
        result = list(self._crls)
        if self._fetchers:
            crls = self._fetchers.crl_fetcher.fetched_crls()
            result.extend(CRLContainer(crl_data) for crl_data in crls)
        return result

    async def fetch_crls(self, cert) -> List[CRLContainer]:
        """
        .. versionadded:: 0.27.0

        Download all relevant CRLs for a given certificate.

        :param cert:
            An asn1crypto.x509.Certificate object

        :return:
            A list of :class:`CRLContainer` objects
        """
        if not self._fetchers:
            raise CRLFetchError("No CRL fetcher available")

        fetchers = self._fetchers
        try:
            crls = fetchers.crl_fetcher.fetched_crls_for_cert(cert)
        except KeyError:
            crls = await fetchers.crl_fetcher.fetch(cert)
        conts = [CRLContainer(crl_data) for crl_data in crls]
        return conts

    async def async_retrieve_crls(self, cert) -> List[CRLContainer]:
        """
        .. versionadded:: 0.20.0

        :param cert:
            An asn1crypto.x509.Certificate object

        :return:
            A list of :class:`CRLContainer` objects
        """
        crls = self.currently_available_crls()
        if self._fetchers:
            crls.extend(await self.fetch_crls(cert))
        return crls

    async def async_retrieve_ocsps(
        self, cert, authority: Authority
    ) -> List[OCSPContainer]:
        """
        .. versionadded:: 0.20.0

        :param cert:
            An asn1crypto.x509.Certificate object

        :param authority:
            The issuing authority for the certificate

        :return:
            A list of :class:`OCSPContainer` objects
        """

        if not self._fetchers:
            return self._ocsps

        fetchers = self._fetchers
        ocsps = [
            OCSPContainer(resp)
            for resp in fetchers.ocsp_fetcher.fetched_responses_for_cert(cert)
        ]
        if not ocsps:
            ocsp_response_data = await fetchers.ocsp_fetcher.fetch(
                cert, authority
            )
            ocsps = OCSPContainer.load_multi(ocsp_response_data)

            # Responses can contain certificates that are useful in
            # validating the response itself. We can use these since they
            # will be validated using the local trust roots.
            for resp in ocsps:
                try:
                    self._extract_ocsp_certs(resp)
                except ValueError:
                    raise OCSPFetchError(
                        "Failed to extract certificates from "
                        "fetched OCSP response"
                    )

        return ocsps + self._ocsps

    def evict_ocsps(self, hashes_to_evict: Set[bytes]):
        """
        Internal API to eliminate local OCSP records from consideration.

        :param hashes_to_evict:
            A collection of OCSP response hashes; see :func:`.digest_for_poe`.
        """

        def p(container: OCSPContainer):
            digest = digest_for_poe(container.ocsp_response_data.dump())
            return digest not in hashes_to_evict

        self._ocsps = list(filter(p, self._ocsps))

    def evict_crls(self, hashes_to_evict: Set[bytes]):
        """
        Internal API to eliminate local CRLs from consideration.

        :param hashes_to_evict:
            A collection of CRL hashes; see :func:`.digest_for_poe`.
        """

        def p(container: CRLContainer):
            digest = digest_for_poe(container.crl_data.dump())
            return digest not in hashes_to_evict

        self._crls = list(filter(p, self._crls))

    def check_asserted_unrevoked(
        self, cert: x509.Certificate, at: datetime
    ) -> bool:
        try:
            return at <= self._assertions[cert.sha256].at
        except KeyError:
            return False
