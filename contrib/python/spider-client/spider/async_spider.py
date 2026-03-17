import os, tenacity, json, aiohttp, logging
from typing import Optional, Dict, Any, AsyncIterator, Callable
from aiohttp import ClientSession, ClientResponse
from types import TracebackType
from typing import Type
from spider.spider_types import RequestParamsDict, JsonCallback, QueryRequest


class AsyncSpider:
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the Spider with an API key.

        :param api_key: A string of the API key for Spider. Defaults to the SPIDER_API_KEY environment variable.
        :raises ValueError: If no API key is provided.
        """
        self.api_key = api_key or os.getenv("SPIDER_API_KEY")
        if self.api_key is None:
            raise ValueError("No API key provided")
        self.session: Optional[ClientSession] = None

    async def __aenter__(self) -> "AsyncSpider":
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.session:
            await self.session.close()

    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=60),
        stop=tenacity.stop_after_attempt(5)
    )
    async def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> AsyncIterator[Any]:
        if not self.session:
            raise RuntimeError(
                "Session not initialized. Use AsyncSpider as a context manager."
            )

        headers = self._prepare_headers(content_type)
        url = f"https://api.spider.cloud/v1/{endpoint}"

        if params:
            params = {
                k: str(v).lower() if isinstance(v, bool) else v
                for k, v in params.items()
            }
        async with self.session.request(
            method, url, json=data, params=params, headers=headers
        ) as response:
            if stream:
                async for chunk in response.content.iter_any():
                    yield chunk
            else:
                if response.status >= 200 and response.status < 300:
                    if "application/json" in response.headers.get("Content-Type", ""):
                        yield await response.json()
                    else:
                        new_text = await response.text()
                        new_json = json.loads(new_text)
                        yield new_json
                else:
                    await self._handle_error(response, f"{method} to {endpoint}")

    async def scrape_url(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> AsyncIterator[Any]:
        """
        Scrape data from the specified URL.

        :param url: The URL from which to scrape data.
        :param params: Optional dictionary of additional parameters for the scrape request.
        :return: JSON response containing the scraping results.
        """
        data = {"url": url, "limit": 1, **(params or {})}
        async for response in self._request(
            "POST", "crawl", data=data, stream=stream, content_type=content_type
        ):
            yield response

    async def crawl_url(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
        callback: Optional[JsonCallback] = None,
    ) -> AsyncIterator[Any]:
        """
        Start crawling at the specified URL.

        :param url: The URL to begin crawling.
        :param params: Optional dictionary with additional parameters to customize the crawl.
        :param stream: Boolean indicating if the response should be streamed. Defaults to False.
        :return: JSON response or the raw response stream if streaming enabled.
        """
        data = {"url": url, **(params or {})}
        if stream and callback:
            content_type = "application/jsonl"

        async for response in self._request(
            "POST", "crawl", data=data, stream=stream, content_type=content_type
        ):
            if stream and callback:
                await self._stream_reader(response, callback)
            else:
                yield response

    async def links(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> AsyncIterator[Any]:
        """
        Retrieve links from the specified URL.

        :param url: The URL from which to extract links.
        :param params: Optional parameters for the link retrieval request.
        :return: JSON response containing the links.
        """
        data = {"url": url, **(params or {})}
        async for response in self._request(
            "POST", "links", data=data, stream=stream, content_type=content_type
        ):
            yield response

    async def screenshot(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> AsyncIterator[Any]:
        """
        Take a screenshot of the specified URL.

        :param url: The URL to capture a screenshot from.
        :param params: Optional parameters to customize the screenshot capture.
        :return: JSON response with screenshot data.
        """
        data = {"url": url, **(params or {})}
        async for response in self._request(
            "POST", "screenshot", data=data, stream=stream, content_type=content_type
        ):
            yield response

    async def search(
        self,
        q: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> AsyncIterator[Any]:
        """
        Perform a search and gather a list of websites to start crawling and collect resources.

        :param search: The search query.
        :param params: Optional parameters to customize the search.
        :return: JSON response or the raw response stream if streaming enabled.
        """
        data = {"search": q, **(params or {})}
        async for response in self._request(
            "POST", "search", data=data, stream=stream, content_type=content_type
        ):
            yield response

    async def transform(
        self,
        data: list,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> AsyncIterator[Any]:
        """
        Transform HTML to Markdown or text. You can send up to 10MB of data at once.

        :param data: The data to transform a list of objects with the 'html' key and an optional 'url' key only used readability mode.
        :param params: Optional parameters to customize the search.
        :return: JSON response or the raw response stream if streaming enabled.
        """
        payload = {"data": data, **(params or {})}
        async for response in self._request(
            "POST", "transform", data=payload, stream=stream, content_type=content_type
        ):
            yield response

    async def get_credits(self) -> AsyncIterator[Any]:
        """
        Retrieve the account's remaining credits.

        :return: JSON response containing the number of credits left.
        """
        async for response in self._request("GET", "data/credits"):
            yield response

    async def data_post(
        self, table: str, data: Optional[RequestParamsDict] = {}
    ) -> AsyncIterator[Any]:
        """
        Send data to a specific table via POST request.

        :param table: The table name to which the data will be posted.
        :param data: A dictionary representing the data to be posted.
        :return: The JSON response from the server.
        """
        async for response in self._request("POST", f"data/{table}", data=data):
            yield response

    async def data_get(
        self, table: str, params: Optional[RequestParamsDict] = None
    ) -> AsyncIterator[Any]:
        """
        Retrieve data from a specific table via GET request.

        :param table: The table name from which to retrieve data.
        :param params: Optional parameters to modify the query.
        :return: The JSON response from the server.
        """
        async for response in self._request("GET", f"data/{table}", params=params):
            yield response

    async def _stream_reader(
        self, response: Any, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        buffer = ""
        async for chunk in response.aiter_bytes():
            buffer += chunk.decode("utf-8", errors="replace")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                try:
                    json_data = json.loads(line)
                    callback(json_data)
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding JSON: {e}")
                    logging.error(f"Problematic line: {line}")

        if buffer.strip():
            try:
                json_data = json.loads(buffer)
                callback(json_data)
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON in remaining buffer: {e}")
                logging.error(f"Problematic data: {buffer}")

    def _prepare_headers(
        self, content_type: str = "application/json"
    ) -> Dict[str, str]:
        return {
            "Content-Type": content_type,
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": "AsyncSpider-Client/0.1.85",
        }

    async def _handle_error(self, response: ClientResponse, action: str) -> None:
        if response.status in [402, 409, 500]:
            error_message = (await response.json()).get(
                "error", "Unknown error occurred"
            )
            raise Exception(
                f"Failed to {action}. Status code: {response.status}. Error: {error_message}"
            )
        else:
            raise Exception(
                f"Unexpected error occurred while trying to {action}. Status code: {response.status}. Here is the response: {await response.text()}"
            )
