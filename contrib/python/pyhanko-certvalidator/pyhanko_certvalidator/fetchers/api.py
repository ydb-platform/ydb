"""
Asynchronous API for fetching OCSP responses, CRLs and certificates.
"""

import abc
from dataclasses import dataclass
from typing import AsyncGenerator, Iterable, Union

from asn1crypto import cms, crl, ocsp, x509
from pyhanko_certvalidator.authority import Authority
from pyhanko_certvalidator.version import __version__

__all__ = [
    'OCSPFetcher',
    'CRLFetcher',
    'CertificateFetcher',
    'Fetchers',
    'FetcherBackend',
    'DEFAULT_USER_AGENT',
]

DEFAULT_USER_AGENT = 'pyhanko_certvalidator %s' % __version__


class OCSPFetcher(abc.ABC):
    """Utility interface to fetch and cache OCSP responses."""

    async def fetch(
        self,
        cert: Union[x509.Certificate, cms.AttributeCertificateV2],
        authority: Authority,
    ) -> ocsp.OCSPResponse:
        """
        Fetch an OCSP response for a certificate.

        :param cert:
            The certificate for which an OCSP response has to be fetched.
        :param authority:
            The issuing authority.
        :raises:
            OCSPFetchError - Raised if an OCSP response could not be obtained.
        :return:
            An OCSP response.
        """
        raise NotImplementedError

    def fetched_responses(self) -> Iterable[ocsp.OCSPResponse]:
        """
        Return all responses fetched by this OCSP fetcher.
        """
        raise NotImplementedError

    def fetched_responses_for_cert(
        self, cert: Union[x509.Certificate, cms.AttributeCertificateV2]
    ) -> Iterable[ocsp.OCSPResponse]:
        """
        Return all responses fetched by this OCSP fetcher that are relevant
        to determine the revocation status of the given certificate.
        """
        raise NotImplementedError


class CRLFetcher(abc.ABC):
    """Utility interface to fetch and cache CRLs."""

    async def fetch(
        self,
        cert: Union[x509.Certificate, cms.AttributeCertificateV2],
        *,
        use_deltas=None,
    ) -> Iterable[crl.CertificateList]:
        """
        Fetches the CRLs for a certificate.

        :param cert:
            An asn1crypto.x509.Certificate object to get the CRL for

        :param use_deltas:
            A boolean indicating if delta CRLs should be fetched

        :raises:
            CRLFetchError - when a network/IO error or decoding error occurs

        :return:
            An iterable of CRLs fetched.
        """
        # side note: we don't want this to be a generator, because in principle,
        #  we always need to consider CRLs from all distribution points together
        #  anyway, so there's no "stream processing" to speak of.
        # (this is currently not 100% efficient in the default implementation,
        #  see comments below)
        raise NotImplementedError

    def fetched_crls(self) -> Iterable[crl.CertificateList]:
        """
        Return all CRLs fetched by this CRL fetcher.
        """
        raise NotImplementedError

    def fetched_crls_for_cert(
        self, cert: Union[x509.Certificate, cms.AttributeCertificateV2]
    ) -> Iterable[crl.CertificateList]:
        """
        Return all relevant fetched CRLs for the given certificate

        :param cert:
            A certificate.
        :return:
            An iterable of CRLs
        :raise KeyError:
            if no fetch operations have been performed for this certificate
        """
        raise NotImplementedError


class CertificateFetcher(abc.ABC):
    """Utility interface to fetch and cache certificates."""

    def fetch_cert_issuers(
        self, cert: Union[x509.Certificate, cms.AttributeCertificateV2]
    ) -> AsyncGenerator[x509.Certificate, None]:
        """
        Fetches certificates from the authority information access extension of
        a certificate.

        :param cert:
            A certificate

        :raises:
            CertificateFetchError - when a network I/O or decoding error occurs

        :return:
            An asynchronous generator yielding asn1crypto.x509.Certificate
            objects that were fetched.
        """
        raise NotImplementedError

    def fetch_crl_issuers(
        self, certificate_list
    ) -> AsyncGenerator[x509.Certificate, None]:
        """
        Fetches certificates from the authority information access extension of
        an asn1crypto.crl.CertificateList.

        :param certificate_list:
            An asn1crypto.crl.CertificateList object

        :raises:
            CertificateFetchError - when a network I/O or decoding error occurs

        :return:
            An asynchronous generator yielding asn1crypto.x509.Certificate
            objects that were fetched.
        """
        raise NotImplementedError

    def fetched_certs(self) -> Iterable[x509.Certificate]:
        """
        Return all certificates retrieved by this certificate fetcher.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class Fetchers:
    """
    Models a collection of fetchers to be used by a validation context.

    The intention is that these can share resources (like a connection pool)
    in a unified, controlled manner. See also :class:`.FetcherBackend`.
    """

    ocsp_fetcher: OCSPFetcher
    crl_fetcher: CRLFetcher
    cert_fetcher: CertificateFetcher


class FetcherBackend(abc.ABC):
    """
    Generic, bare-bones interface to help abstract away instantiation logic for
    fetcher implementations.

    Intended to operate as an asynchronous context manager, with
    `async with backend_obj as fetchers: ...` putting the resulting
    :class:`.Fetchers` object in to the variable named `fetchers`.

    .. note::
        The initialisation part of the API is necessarily synchronous,
        for backwards compatibility with the old ``ValidationContext`` API.
        If you need asynchronous resource management, handle it elsewhere,
        or use some form of lazy resource provisioning.

        Alternatively, you can pass :class:`Fetchers` objects to the validation
        context yourself, and forgo use of the :class:`.FetcherBackend`
        API altogether.
    """

    def get_fetchers(self) -> Fetchers:
        """
        Set up fetchers synchronously.

        .. note::
            This is a synchronous method
        """
        raise NotImplementedError

    async def close(self):
        """
        Clean up the resources associated with this fetcher backend,
        asynchronously.
        """
        pass

    async def __aenter__(self) -> Fetchers:
        return self.get_fetchers()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.close()
