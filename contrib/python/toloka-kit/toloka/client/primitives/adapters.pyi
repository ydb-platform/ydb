__all__ = [
    'httpx_exception_to_urllib3_exception',
    'map_urllib3_exception_for_retrying',
]
import httpx


def httpx_exception_to_urllib3_exception(exception: httpx.HTTPError) -> BaseException:
    """Maps the httpx exception to the corresponding urllib3 exception.
    """
    ...


def map_urllib3_exception_for_retrying(exception: BaseException) -> BaseException:
    """Follows the exception mapping logic from the urllib3.connectionpool.HTTPConnectionPool.urlopen as of
    urllib3==1.26.15

    SSL-related exceptions are not supported.
    """
    ...
