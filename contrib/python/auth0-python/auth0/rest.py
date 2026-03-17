from __future__ import annotations

import base64
import platform
import sys
from json import dumps, loads
from random import randint
from time import sleep
from typing import TYPE_CHECKING, Any, Mapping
from urllib.parse import urlencode

import requests

from auth0.exceptions import Auth0Error, RateLimitError
from auth0.types import RequestData, TimeoutType

if TYPE_CHECKING:
    from auth0.rest_async import RequestsResponse

UNKNOWN_ERROR = "a0.sdk.internal.unknown"


class RestClientOptions:
    """Configuration object for RestClient. Used for configuring
            additional RestClient options, such as rate-limit
            retries.

    Args:
        telemetry (bool, optional): Enable or disable Telemetry
            (defaults to True)
        timeout (float or tuple, optional): Change the requests
            connect and read timeout. Pass a tuple to specify
            both values separately or a float to set both to it.
            (defaults to 5.0 for both)
        retries (integer): In the event an API request returns a
            429 response header (indicating rate-limit has been
            hit), the RestClient will retry the request this many
            times using an exponential backoff strategy, before
            raising a RateLimitError exception. 10 retries max.
            (defaults to 3)
    """

    def __init__(
        self,
        telemetry: bool = True,
        timeout: TimeoutType = 5.0,
        retries: int = 3,
    ) -> None:
        self.telemetry = telemetry
        self.timeout = timeout
        self.retries = retries


class RestClient:
    """Provides simple methods for handling all RESTful api endpoints.

    Args:
        jwt (str, optional): The JWT to be used with the RestClient.
        telemetry (bool, optional): Enable or disable Telemetry
            (defaults to True)
        timeout (float or tuple, optional): Change the requests
            connect and read timeout. Pass a tuple to specify
            both values separately or a float to set both to it.
            (defaults to 5.0 for both)
        options (RestClientOptions): Pass an instance of
            RestClientOptions to configure additional RestClient
            options, such as rate-limit retries. Overrides matching
            options passed to the constructor.
            (defaults to 3)
    """

    def __init__(
        self,
        jwt: str | None,
        telemetry: bool = True,
        timeout: TimeoutType = 5.0,
        options: RestClientOptions | None = None,
    ) -> None:
        if options is None:
            options = RestClientOptions(telemetry=telemetry, timeout=timeout)

        self.options = options
        self.jwt = jwt

        self._metrics = {"retries": 0, "backoff": []}
        self._skip_sleep = False

        self.base_headers = {
            "Content-Type": "application/json",
        }

        if jwt is not None:
            self.base_headers["Authorization"] = f"Bearer {self.jwt}"

        if options.telemetry:
            py_version = platform.python_version()
            version = sys.modules["auth0"].__version__

            auth0_client = dumps(
                {
                    "name": "auth0-python",
                    "version": version,
                    "env": {
                        "python": py_version,
                    },
                }
            ).encode("utf-8")

            self.base_headers.update(
                {
                    "User-Agent": f"Python/{py_version}",
                    "Auth0-Client": base64.b64encode(auth0_client).decode(),
                }
            )

        # Cap the maximum number of retries to 10 or fewer. Floor the retries at 0.
        self._retries = min(self.MAX_REQUEST_RETRIES(), max(0, options.retries))

        # For backwards compatibility reasons only
        # TODO: Deprecate in the next major so we can prune these arguments. Guidance should be to use RestClient.options.*
        self.telemetry = options.telemetry
        self.timeout = options.timeout

    # Returns a hard cap for the maximum number of retries allowed (10)
    def MAX_REQUEST_RETRIES(self) -> int:
        return 10

    # Returns the maximum amount of jitter to introduce in milliseconds (100ms)
    def MAX_REQUEST_RETRY_JITTER(self) -> int:
        return 100

    # Returns the maximum delay window allowed (1000ms)
    def MAX_REQUEST_RETRY_DELAY(self) -> int:
        return 1000

    # Returns the minimum delay window allowed (100ms)
    def MIN_REQUEST_RETRY_DELAY(self) -> int:
        return 100

    def _request(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: RequestData | None = None,
        json: RequestData | None = None,
        headers: dict[str, str] | None = None,
        files: dict[str, Any] | None = None,
    ) -> Any:
        # Track the API request attempt number
        attempt = 0

        # Reset the metrics tracker
        self._metrics = {"retries": 0, "backoff": []}

        if data is None and json is not None and headers:
            content_type = headers.get("Content-Type", "").lower()  # Get Content-Type 
            if "application/x-www-form-urlencoded" in content_type:
                data = urlencode(json)  # Copy JSON data into data
                json = None  # Prevent JSON from being sent

        kwargs = {
            k: v
            for k, v in {
                "params": params,
                "json": json,
                "data": data,
                "headers": headers,
                "files": files,
                "timeout": self.options.timeout,
            }.items()
            if v is not None
        }

        while True:
            # Increment attempt number
            attempt += 1

            # Issue the request
            response = requests.request(method, url, **kwargs)

            # If the response did not have a 429 header, or the attempt number is greater than the configured retries, break
            if response.status_code != 429 or attempt > self._retries:
                break

            wait = self._calculate_wait(attempt)

            # Skip calling sleep() when running unit tests
            if self._skip_sleep is False:
                # sleep() functions in seconds, so convert the milliseconds formula above accordingly
                sleep(wait / 1000)

        # Return the final Response
        return self._process_response(response)

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        request_headers = self.base_headers.copy()
        request_headers.update(headers or {})
        return self._request("GET", url, params=params, headers=request_headers)

    def post(
        self,
        url: str,
        data: RequestData | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        request_headers = self.base_headers.copy()
        request_headers.update(headers or {})
        return self._request("POST", url, json=data, headers=request_headers)

    def file_post(
        self,
        url: str,
        data: RequestData | None = None,
        files: dict[str, Any] | None = None,
    ) -> Any:
        headers = self.base_headers.copy()
        headers.pop("Content-Type", None)
        return self._request("POST", url, data=data, files=files, headers=headers)

    def patch(self, url: str, data: RequestData | None = None) -> Any:
        headers = self.base_headers.copy()
        return self._request("PATCH", url, json=data, headers=headers)

    def put(self, url: str, data: RequestData | None = None) -> Any:
        headers = self.base_headers.copy()
        return self._request("PUT", url, json=data, headers=headers)

    def delete(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        data: RequestData | None = None,
    ) -> Any:
        headers = self.base_headers.copy()
        return self._request("DELETE", url, params=params, json=data, headers=headers)

    def _calculate_wait(self, attempt: int) -> int:
        # Retry the request. Apply a exponential backoff for subsequent attempts, using this formula:
        # max(MIN_REQUEST_RETRY_DELAY, min(MAX_REQUEST_RETRY_DELAY, (100ms * (2 ** attempt - 1)) + random_between(1, MAX_REQUEST_RETRY_JITTER)))

        # Increases base delay by (100ms * (2 ** attempt - 1))
        wait = 100 * 2 ** (attempt - 1)

        # Introduces jitter to the base delay; increases delay between 1ms to MAX_REQUEST_RETRY_JITTER (100ms)
        wait += randint(1, self.MAX_REQUEST_RETRY_JITTER())

        # Is never more than MAX_REQUEST_RETRY_DELAY (1s)
        wait = min(self.MAX_REQUEST_RETRY_DELAY(), wait)

        # Is never less than MIN_REQUEST_RETRY_DELAY (100ms)
        wait = max(self.MIN_REQUEST_RETRY_DELAY(), wait)

        self._metrics["retries"] = attempt
        self._metrics["backoff"].append(wait)  # type: ignore[attr-defined]

        return wait

    def _process_response(self, response: requests.Response) -> Any:
        return self._parse(response).content()

    def _parse(self, response: requests.Response) -> Response:
        if not response.text:
            return EmptyResponse(response.status_code)
        try:
            return JsonResponse(response)
        except ValueError:
            return PlainResponse(response)


class Response:
    def __init__(
        self, status_code: int, content: Any, headers: Mapping[str, str]
    ) -> None:
        self._status_code = status_code
        self._content = content
        self._headers = headers

    def content(self) -> Any:
        if self._is_error():
            if self._status_code == 429:
                reset_at = int(self._headers.get("x-ratelimit-reset", "-1"))
                raise RateLimitError(
                    error_code=self._error_code(),
                    message=self._error_message(),
                    reset_at=reset_at,
                    headers=self._headers,
                )
            if self._error_code() == "mfa_required":
                raise Auth0Error(
                    status_code=self._status_code,
                    error_code=self._error_code(),
                    message=self._error_message(),
                    content=self._content,
                    headers=self._headers
                )

            raise Auth0Error(
                status_code=self._status_code,
                error_code=self._error_code(),
                message=self._error_message(),
                headers=self._headers
            )
        else:
            return self._content

    def _is_error(self) -> bool:
        return self._status_code is None or self._status_code >= 400

    # Adding these methods to force implementation in subclasses because they are references in this parent class
    def _error_code(self):
        raise NotImplementedError

    def _error_message(self):
        raise NotImplementedError


class JsonResponse(Response):
    def __init__(self, response: requests.Response | RequestsResponse) -> None:
        content = loads(response.text)
        super().__init__(response.status_code, content, response.headers)

    def _error_code(self) -> str:
        if "errorCode" in self._content:
            return self._content.get("errorCode")
        elif "error" in self._content:
            return self._content.get("error")
        elif "code" in self._content:
            return self._content.get("code")
        else:
            return UNKNOWN_ERROR

    def _error_message(self) -> str:
        if "error_description" in self._content:
            return self._content.get("error_description")
        message = self._content.get("message", "")
        if message is not None and message != "":
            return message
        return self._content.get("error", "")


class PlainResponse(Response):
    def __init__(self, response: requests.Response | RequestsResponse) -> None:
        super().__init__(response.status_code, response.text, response.headers)

    def _error_code(self) -> str:
        return UNKNOWN_ERROR

    def _error_message(self) -> str:
        return self._content


class EmptyResponse(Response):
    def __init__(self, status_code: int) -> None:
        super().__init__(status_code, "", {})

    def _error_code(self) -> str:
        return UNKNOWN_ERROR

    def _error_message(self) -> str:
        return ""
