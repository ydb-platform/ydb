import logging
from typing import Iterable, Union

import requests
from asn1crypto import cms, crl, pem, x509

from ... import errors
from ...util import get_relevant_crl_dps, issuer_serial
from ..api import CRLFetcher
from ..common_utils import (
    crl_job_results_as_completed,
    enumerate_delivery_point_urls,
)
from .util import RequestsFetcherMixin

logger = logging.getLogger(__name__)


class RequestsCRLFetcher(CRLFetcher, RequestsFetcherMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._by_cert = {}

    async def fetch(
        self,
        cert: Union[x509.Certificate, cms.AttributeCertificateV2],
        *,
        use_deltas=True,
    ):
        iss_serial = issuer_serial(cert)
        try:
            return self._by_cert[iss_serial]
        except KeyError:
            pass

        results = []
        async for fetched_crl in self._fetch(cert, use_deltas=use_deltas):
            results.append(fetched_crl)
        self._by_cert[iss_serial] = results
        return results

    async def _fetch_single(self, url):
        async def task():
            logger.info(f"Requesting CRL from {url}...")
            try:
                response = await self._get(
                    url, acceptable_content_types=('application/pkix-crl',)
                )
                data = response.content
                if pem.detect(data):
                    _, _, data = pem.unarmor(data)
                return crl.CertificateList.load(data)
            except (ValueError, requests.RequestException) as e:
                raise errors.CRLFetchError(
                    f"Failure to fetch CRL from URL {url}"
                ) from e

        return await self._perform_fetch(url, task)

    async def _fetch(self, cert: x509.Certificate, *, use_deltas):
        sources = get_relevant_crl_dps(cert, use_deltas=use_deltas)

        def _fetch_jobs():
            for distribution_point in sources:
                for url in enumerate_delivery_point_urls(distribution_point):
                    yield self._fetch_single(url)

        async for result in crl_job_results_as_completed(_fetch_jobs()):
            yield result

    def fetched_crls(self) -> Iterable[crl.CertificateList]:
        return {crl_ for crl_ in self.get_results()}

    def fetched_crls_for_cert(self, cert) -> Iterable[crl.CertificateList]:
        return self._by_cert[issuer_serial(cert)]
