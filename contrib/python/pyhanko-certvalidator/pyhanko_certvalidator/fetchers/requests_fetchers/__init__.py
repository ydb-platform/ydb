"""
Fetcher implementation using the ``requests`` library for backwards
compatibility. This fetcher backend doesn't take advantage of asyncio, but
has the advantage of not requiring any resource management on the caller's part.
"""

from ..api import FetcherBackend, Fetchers
from .cert_fetch_client import RequestsCertificateFetcher
from .crl_client import RequestsCRLFetcher
from .ocsp_client import RequestsOCSPFetcher

__all__ = ['RequestsFetcherBackend']


class RequestsFetcherBackend(FetcherBackend):
    def __init__(self, per_request_timeout=10):
        self.per_request_timeout = per_request_timeout

    def get_fetchers(self) -> Fetchers:
        to = self.per_request_timeout
        return Fetchers(
            ocsp_fetcher=RequestsOCSPFetcher(per_request_timeout=to),
            crl_fetcher=RequestsCRLFetcher(per_request_timeout=to),
            cert_fetcher=RequestsCertificateFetcher(per_request_timeout=to),
        )

    async def close(self):
        # don't need to do anything
        return
