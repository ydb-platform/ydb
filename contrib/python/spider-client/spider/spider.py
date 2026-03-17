import os, requests, logging, ijson, tenacity
from typing import Optional, Dict
from spider.spider_types import (
    RequestParamsDict,
    SearchRequestParams,
    RequestParamsTransform,
    JsonCallback,
    QueryRequest,
)


class Spider:
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the Spider with an API key.

        :param api_key: A string of the API key for Spider. Defaults to the SPIDER_API_KEY environment variable.
        :raises ValueError: If no API key is provided.
        """
        self.api_key = api_key or os.getenv("SPIDER_API_KEY")
        if self.api_key is None:
            raise ValueError("No API key provided")

    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=60),
        stop=tenacity.stop_after_attempt(5),
    )
    def api_post(
        self,
        endpoint: str,
        data: dict,
        stream: bool = False,
        content_type: str = "application/json",
    ):
        """
        Send a POST request to the specified API endpoint.

        :param endpoint: The API endpoint to which the POST request is sent.
        :param data: The data (dictionary) to be sent in the POST request.
        :param stream: Boolean indicating if the response should be streamed.
        :return: The JSON response or the raw response stream if stream is True.
        """
        headers = self._prepare_headers(content_type)
        response = self._post_request(
            f"https://api.spider.cloud/{endpoint}", data, headers, stream
        )
        if stream:
            return response
        elif 200 <= response.status_code < 300:
            return response.json()
        else:
            self._handle_error(response, f"post to {endpoint}")

    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=60),
        stop=tenacity.stop_after_attempt(5),
    )
    def api_get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ):
        """
        Send a GET request to the specified endpoint.

        :param endpoint: The API endpoint from which to retrieve data.
        :param params: Query parameters to attach to the URL.
        :return: The JSON decoded response.
        """
        headers = self._prepare_headers(content_type)
        response = requests.get(
            f"https://api.spider.cloud/{endpoint}",
            headers=headers,
            params=params,
            stream=stream,
        )
        if 200 <= response.status_code < 300:
            return response.json()
        else:
            self._handle_error(response, f"get from {endpoint}")

    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, min=1, max=60),
        stop=tenacity.stop_after_attempt(5),
    )
    def api_delete(
        self,
        endpoint: str,
        params: Optional[RequestParamsDict] = None,
        stream: Optional[bool] = False,
        content_type: Optional[str] = "application/json",
    ):
        """
        Send a DELETE request to the specified endpoint.

        :param endpoint: The API endpoint from which to retrieve data.
        :param params: Optional parameters to include in the DELETE request.
        :param stream: Boolean indicating if the response should be streamed.
        :param content_type: The content type of the request.
        :return: The JSON decoded response.
        """
        headers = self._prepare_headers(content_type)
        response = self._delete_request(
            f"https://api.spider.cloud/v1/{endpoint}",
            headers=headers,
            json=params,
            stream=stream,
        )
        if 200 <= response.status_code < 300:
            return response.json()
        else:
            self._handle_error(response, f"delete from {endpoint}")

    def scrape_url(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ):
        """
        Scrape data from the specified URL.

        :param url: The URL from which to scrape data.
        :param params: Optional dictionary of additional parameters for the scrape request.
        :return: JSON response containing the scraping results.
        """
        return self.api_post(
            "crawl", {"url": url, "limit": 1, **(params or {})}, stream, content_type
        )

    def crawl_url(
        self,
        url: str,
        params: Optional[RequestParamsDict],
        stream: Optional[bool] = False,
        content_type: Optional[str] = "application/json",
        callback: Optional[JsonCallback] = None,
    ):
        """
        Start crawling at the specified URL.

        :param url: The URL to begin crawling.
        :param params: Optional dictionary with additional parameters to customize the crawl.
        :param stream: Optional Boolean indicating if the response should be streamed. Defaults to False.
        :param content_type: Optional str to determine the content-type header of the request.
        :param callback: Optional callback to use with streaming. This will only send the data via callback.

        :return: JSON response or the raw response stream if streaming enabled.
        """
        jsonl = stream and callable(callback)

        if jsonl:
            content_type = "application/jsonl"

        response = self.api_post(
            "crawl", {"url": url, **(params or {})}, stream, content_type
        )

        if jsonl:
            return self.stream_reader(response, callback)
        else:
            return response

    def links(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ):
        """
        Retrieve links from the specified URL.

        :param url: The URL from which to extract links.
        :param params: Optional parameters for the link retrieval request.
        :return: JSON response containing the links.
        """
        return self.api_post(
            "links", {"url": url, **(params or {})}, stream, content_type
        )

    def screenshot(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ):
        """
        Take a screenshot of the specified URL.

        :param url: The URL to capture a screenshot from.
        :param params: Optional parameters to customize the screenshot capture.
        :return: JSON response with screenshot data.
        """
        return self.api_post(
            "screenshot", {"url": url, **(params or {})}, stream, content_type
        )

    def search(
        self,
        q: str,
        params: Optional[SearchRequestParams] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ):
        """
        Perform a search and gather a list of websites to start crawling and collect resources.

        :param search: The search query.
        :param params: Optional parameters to customize the search.
        :return: JSON response or the raw response stream if streaming enabled.
        """
        return self.api_post(
            "search", {"search": q, **(params or {})}, stream, content_type
        )

    def transform(
        self,
        data,
        params: Optional[RequestParamsTransform] = None,
        stream=False,
        content_type="application/json",
    ):
        """
        Transform HTML to Markdown or text. You can send up to 10MB of data at once.

        :param data: The data to transform a list of objects with the 'html' key and an optional 'url' key only used readability mode.
        :param params: Optional parameters to customize the search.
        :return: JSON response or the raw response stream if streaming enabled.
        """
        return self.api_post(
            "transform", {"data": data, **(params or {})}, stream, content_type
        )

    def unblock_url(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ):
        """
        Unblock data from the specified URL.

        :param url: The URL from which to scrape data.
        :param params: Optional dictionary of additional parameters for the scrape request.
        :return: JSON response containing the scraping results.
        """
        return self.api_post(
            "unblocker", {"url": url, "limit": 1, **(params or {})}, stream, content_type
        )

    def get_credits(self):
        """
        Retrieve the account's remaining credits.

        :return: JSON response containing the number of credits left.
        """
        return self.api_get("data/credits")

    def data_post(self, table: str, data: Optional[RequestParamsDict] = None):
        """
        Send data to a specific table via POST request.
        :param table: The table name to which the data will be posted.
        :param data: A dictionary representing the data to be posted.
        :return: The JSON response from the server.
        """
        return self.api_post(f"data/{table}", data, stream=False)

    def data_get(
        self,
        table: str,
        params: Optional[RequestParamsDict] = None,
    ):
        """
        Retrieve data from a specific table via GET request.
        :param table: The table name from which to retrieve data.
        :param params: Optional parameters to modify the query.
        :return: The JSON response from the server.
        """
        return self.api_get(f"data/{table}", params)

    def stream_reader(self, response, callback):
        response.raise_for_status()

        try:
            for json_obj in ijson.items(response.raw, "", multiple_values=True):
                callback(json_obj)

        except Exception as e:
            logging.error(f"An error occurred while parsing JSON: {e}")

    def _prepare_headers(self, content_type: str = "application/json"):
        return {
            "Content-Type": content_type,
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": f"Spider-Client/0.1.85",
        }

    def _post_request(self, url: str, data, headers, stream=False):
        return requests.post(url, headers=headers, json=data, stream=stream)

    def _get_request(self, url: str, headers, stream=False, params=None):
        return requests.get(url, headers=headers, stream=stream, params=params)

    def _delete_request(self, url: str, headers, json=None, stream=False):
        return requests.delete(url, headers=headers, json=json, stream=stream)

    def _handle_error(self, response, action):
        if response.status_code in [402, 409, 500]:
            error_message = response.json().get("error", "Unknown error occurred")
            raise Exception(
                f"Failed to {action}. Status code: {response.status_code}. Error: {error_message}"
            )
        else:
            raise Exception(
                f"Unexpected error occurred while trying to {action}. Status code: {response.status_code}. Here is the response: {response.text}"
            )
