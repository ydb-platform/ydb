from .api import *

__all__ = [
    'Fetchers',
    'FetcherBackend',
    'OCSPFetcher',
    'CRLFetcher',
    'CertificateFetcher',
    'default_fetcher_backend',
]


def default_fetcher_backend() -> FetcherBackend:
    """
    Instantiate a default fetcher backend that doesn't require any resource
    management, but is less efficient than a fully asynchronous fetcher
    would be.
    """

    from .requests_fetchers import RequestsFetcherBackend

    return RequestsFetcherBackend()
