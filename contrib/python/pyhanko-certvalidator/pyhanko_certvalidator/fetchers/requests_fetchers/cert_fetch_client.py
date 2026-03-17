import logging
from typing import Iterable, Union

import requests
from asn1crypto import cms, x509

from ...errors import CertificateFetchError
from ..api import CertificateFetcher
from ..common_utils import (
    ACCEPTABLE_CERT_DER_ALIASES,
    ACCEPTABLE_CERT_PEM_ALIASES,
    ACCEPTABLE_PKCS7_DER_ALIASES,
    ACCEPTABLE_STRICT_CERT_CONTENT_TYPES,
    complete_certificate_fetch_jobs,
    gather_aia_issuer_urls,
    unpack_cert_content,
)
from .util import RequestsFetcherMixin

logger = logging.getLogger(__name__)


class RequestsCertificateFetcher(CertificateFetcher, RequestsFetcherMixin):
    """
    Implementation of async CertificateFetcher API using requests, for backwards
    compatibility. This class does not require resource management.
    """

    def __init__(
        self, user_agent=None, per_request_timeout=10, permit_pem=True
    ):
        super().__init__(user_agent, per_request_timeout)
        self.permit_pem = permit_pem

    async def fetch_certs(self, url, url_origin_type):
        """
        Fetch one or more certificates from a URL.

        :param url:
            URL to fetch.
        :param url_origin_type:
            Parameter indicating where the URL came from (e.g. 'CRL'),
            for error reporting purposes.
        :raises:
            CertificateFetchError - when a network I/O or decoding error occurs
        :return:
            An iterable of asn1crypto.x509.Certificate objects.
        """

        async def task():
            try:
                logger.info(f"Fetching certificates from {url}...")
                results = await self._grab_certs(
                    url, url_origin_type=url_origin_type
                )
            except (ValueError, requests.RequestException) as e:
                msg = f"Failed to fetch certificate(s) from url {url}."
                logger.debug(msg, exc_info=e)
                raise CertificateFetchError(msg)
            return results

        return await self._perform_fetch(url, task)

    def fetch_cert_issuers(
        self, cert: Union[x509.Certificate, cms.AttributeCertificateV2]
    ):
        fetch_jobs = [
            self.fetch_certs(url, url_origin_type='certificate')
            for url in gather_aia_issuer_urls(cert)
        ]

        if isinstance(cert, x509.Certificate):
            target = cert.subject.human_friendly
        else:
            # TODO log audit ID
            target = "attribute certificate"
        logger.info(f"Retrieving issuer certs for {target}...")
        return complete_certificate_fetch_jobs(fetch_jobs)

    def fetch_crl_issuers(self, certificate_list):
        fetch_jobs = [
            self.fetch_certs(url, url_origin_type='CRL')
            for url in certificate_list.issuer_cert_urls
        ]
        return complete_certificate_fetch_jobs(fetch_jobs)

    def fetched_certs(self) -> Iterable[x509.Certificate]:
        return self.get_results()

    async def _grab_certs(self, url, *, url_origin_type):
        """
        Grab one or more certificates from a caIssuers URL.

        We accept two types of content in the response:
          - A single DER-encoded X.509 certificate
          - A PKCS#7 'certs-only' SignedData message
          - PEM-encoded certificates (if permit_pem=True)

        Note: strictly speaking, you're not supposed to use PEM to serve certs
        for AIA purposes in PEM format, but people do it anyway.
        """

        permit_pem = self.permit_pem
        if permit_pem:
            acceptable_cts = (
                ACCEPTABLE_STRICT_CERT_CONTENT_TYPES
                | ACCEPTABLE_CERT_PEM_ALIASES
                | ACCEPTABLE_CERT_DER_ALIASES
                | ACCEPTABLE_PKCS7_DER_ALIASES
            )
        else:
            acceptable_cts = ACCEPTABLE_STRICT_CERT_CONTENT_TYPES

        response = await self._get(url, acceptable_content_types=acceptable_cts)
        try:
            content_type = response.headers['Content-Type'].strip()
            if content_type not in acceptable_cts:
                ct_err = (
                    f"Unacceptable content type '{repr(content_type)}' "
                    f"when fetching issuer certificate for {url_origin_type} "
                    f"from URL {url}."
                )
                raise requests.RequestException(ct_err)
        except KeyError:
            content_type = None
        certs = unpack_cert_content(
            response.content, content_type, url, permit_pem
        )
        return list(certs)
