from typing import Optional

import aiohttp

from ..api import FetcherBackend, Fetchers
from .cert_fetch_client import AIOHttpCertificateFetcher
from .crl_client import AIOHttpCRLFetcher
from .ocsp_client import AIOHttpOCSPFetcher
from .util import LazySession

__all__ = ['AIOHttpFetcherBackend']


class AIOHttpFetcherBackend(FetcherBackend):
    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        per_request_timeout=10,
    ):
        self.session = session or LazySession()
        self.per_request_timeout = per_request_timeout

    def get_fetchers(self) -> Fetchers:
        session = self.session
        to = self.per_request_timeout
        return Fetchers(
            ocsp_fetcher=AIOHttpOCSPFetcher(session, per_request_timeout=to),
            crl_fetcher=AIOHttpCRLFetcher(session, per_request_timeout=to),
            cert_fetcher=AIOHttpCertificateFetcher(
                session, per_request_timeout=to
            ),
        )

    async def close(self):
        session = self.session
        # only close the session if it's a lazy session;
        # a session passed in by the caller is their own responsibility
        if isinstance(session, LazySession):
            await session.close()
