# Copyright 2022, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import datetime
import warnings
from typing import Any, Callable

import attrs
import requests

import cads_api_client

from . import config
from .processing import ApiResponse, ApiResponsePaginated, RequestKwargs


@attrs.define
class Collections(ApiResponsePaginated):
    """A class to interact with catalogue collections."""

    @property
    def collection_ids(self) -> list[str]:
        """List of collection IDs."""
        return [collection["id"] for collection in self._json_dict["collections"]]


@attrs.define
class Collection(ApiResponse):
    """A class to interact with a catalogue collection."""

    @property
    def begin_datetime(self) -> datetime.datetime | None:
        """Begin datetime of the collection."""
        if (value := self._json_dict["extent"]["temporal"]["interval"][0][0]) is None:
            return value
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))

    @property
    def end_datetime(self) -> datetime.datetime | None:
        """End datetime of the collection."""
        if (value := self._json_dict["extent"]["temporal"]["interval"][0][1]) is None:
            return value
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))

    @property
    def bbox(self) -> tuple[float, float, float, float]:
        """Bounding box of the collection (W, S, E, N)."""
        return tuple(self._json_dict["extent"]["spatial"]["bbox"][0])

    @property
    def id(self) -> str:
        """Collection ID."""
        return str(self._json_dict["id"])

    @property
    def process(self) -> cads_api_client.Process:
        """Collection process."""
        url = self._get_link_href(rel="retrieve")
        return cads_api_client.Process.from_request("get", url, **self._request_kwargs)

    @property
    def form(self) -> list[dict[str, Any]]:
        """Form JSON."""
        url = f"{self.url}/form.json"
        return ApiResponse.from_request(
            "get", url, log_messages=False, **self._request_kwargs
        )._json_list

    @property
    def constraints(self) -> list[dict[str, Any]]:
        """Constraints JSON."""
        url = f"{self.url}/constraints.json"
        return ApiResponse.from_request(
            "get", url, log_messages=False, **self._request_kwargs
        )._json_list

    def submit(self, **request: Any) -> cads_api_client.Remote:
        warnings.warn(
            "`.submit` has been deprecated, and in the future will raise an error."
            " Please use `.process.submit` from now on.",
            DeprecationWarning,
        )
        return self.process.submit(**request)


@attrs.define(slots=False)
class Catalogue:
    url: str
    headers: dict[str, Any]
    session: requests.Session
    retry_options: dict[str, Any]
    request_options: dict[str, Any]
    download_options: dict[str, Any]
    sleep_max: float
    cleanup: bool
    log_callback: Callable[..., None] | None
    force_exact_url: bool = False

    def __attrs_post_init__(self) -> None:
        if not self.force_exact_url:
            self.url += f"/{config.SUPPORTED_API_VERSION}"

    @property
    def _request_kwargs(self) -> RequestKwargs:
        return RequestKwargs(
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            request_options=self.request_options,
            download_options=self.download_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
            log_callback=self.log_callback,
        )

    def get_collections(self, **params: Any) -> Collections:
        url = f"{self.url}/datasets"
        return Collections.from_request(
            "get", url, params=params, **self._request_kwargs
        )

    def get_collection(self, collection_id: str) -> Collection:
        url = f"{self.url}/collections/{collection_id}"
        return Collection.from_request("get", url, **self._request_kwargs)

    def get_licenses(self, **params: Any) -> dict[str, Any]:
        url = f"{self.url}/vocabularies/licences"
        response = ApiResponse.from_request(
            "get", url, params=params, **self._request_kwargs
        )
        return response._json_dict

    @property
    def messages(self) -> ApiResponse:
        url = f"{self.url}/messages"
        return ApiResponse.from_request(
            "get", url, log_messages=False, **self._request_kwargs
        )
