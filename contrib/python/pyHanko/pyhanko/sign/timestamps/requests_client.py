import requests
from asn1crypto import tsp
from pyhanko_certvalidator._asyncio_compat import to_thread

from .api import TimeStamper
from .common_utils import TimestampRequestError, set_tsp_headers

__all__ = ['HTTPTimeStamper']


class HTTPTimeStamper(TimeStamper):
    """
    Standard HTTP-based timestamp client.
    """

    def __init__(self, url, https=False, timeout=5, auth=None, headers=None):
        """
        Initialise the timestamp client.

        :param url:
            URL where the server listens for timestamp requests.
        :param https:
            Enforce HTTPS.
        :param timeout:
            Timeout (in seconds)
        :param auth:
            Value of HTTP ``Authorization`` header
        :param headers:
            Other headers to include.
        """
        if https and not url.startswith('https:'):  # pragma: nocover
            raise ValueError('Timestamp URL is not HTTPS.')
        self.url = url
        self.timeout = timeout
        self.auth = auth
        self.headers = headers
        super().__init__()

    def request_headers(self) -> dict:
        """
        Format the HTTP request headers.

        :return:
            Header dictionary.
        """
        return set_tsp_headers(self.headers or {})

    async def async_request_tsa_response(
        self, req: tsp.TimeStampReq
    ) -> tsp.TimeStampResp:
        def task():
            try:
                raw_res = requests.post(
                    self.url,
                    req.dump(),
                    headers=self.request_headers(),
                    auth=self.auth,
                    timeout=self.timeout,
                )
            except IOError as e:
                raise TimestampRequestError(
                    'Error in communication with timestamp server',
                ) from e

            if (
                raw_res.headers.get('Content-Type')
                != 'application/timestamp-reply'
            ):
                raise TimestampRequestError(
                    'Timestamp server response is malformed.', raw_res
                )
            return tsp.TimeStampResp.load(raw_res.content)

        response = await to_thread(task)
        return response
