from typing import Optional, Union

from asn1crypto import cms, tsp
from pyhanko_certvalidator.fetchers.aiohttp_fetchers.util import LazySession

from pyhanko.sign.timestamps import TimeStamper, TimestampRequestError
from pyhanko.sign.timestamps.common_utils import set_tsp_headers

try:
    import aiohttp
except ImportError as _e:  # pragma: nocover
    raise ImportError(
        "Install pyHanko with the [async_http] optional dependency group"
    ) from _e


class AIOHttpTimeStamper(TimeStamper):
    def __init__(
        self,
        url,
        session: Union[aiohttp.ClientSession, LazySession],
        https=False,
        timeout=5,
        headers=None,
        auth: Optional[aiohttp.BasicAuth] = None,
    ):
        """
        Initialise the timestamp client.

        :param url:
            URL where the server listens for timestamp requests.
        :param https:
            Enforce HTTPS.
        :param timeout:
            Timeout (in seconds)
        :param auth:
            `aiohttp.BasicAuth` object with authentication credentials.
        :param headers:
            Other headers to include.
        """
        if https and not url.startswith('https:'):  # pragma: nocover
            raise ValueError('Timestamp URL is not HTTPS.')
        self.url = url
        self.timeout = timeout
        self.auth = auth
        self.headers = headers
        self._session = session
        super().__init__()

    async def async_request_headers(self) -> dict:
        """
        Format the HTTP request headers.
        Subclasses can override this to perform their own header generation
        logic.

        :return:
            Header dictionary.
        """
        return set_tsp_headers(self.headers or {})

    async def get_session(self) -> aiohttp.ClientSession:
        session = self._session
        if isinstance(session, LazySession):
            return await session.get_session()
        else:
            return session

    async def async_timestamp(
        self, message_digest, md_algorithm
    ) -> cms.ContentInfo:
        return await super().async_timestamp(message_digest, md_algorithm)

    async def async_request_tsa_response(
        self, req: tsp.TimeStampReq
    ) -> tsp.TimeStampResp:
        session = await self.get_session()

        cl_timeout = aiohttp.ClientTimeout(total=self.timeout)
        headers = await self.async_request_headers()
        try:
            async with session.post(
                url=self.url,
                headers=headers,
                data=req.dump(),
                auth=self.auth,
                raise_for_status=True,
                timeout=cl_timeout,
            ) as response:
                response_data = await response.read()
                ct = response.headers.get('Content-Type')
                if ct != 'application/timestamp-reply':
                    msg = (
                        f'Bad content type. Expected '
                        f'application/timestamp-reply,but got {ct}.'
                    )
                    raise aiohttp.ContentTypeError(
                        response.request_info,
                        response.history,
                        message=msg,
                        headers=response.headers,
                    )
        except aiohttp.ClientError as e:
            raise TimestampRequestError(
                "Error while contacting timestamp service",
            ) from e
        return tsp.TimeStampResp.load(response_data)
