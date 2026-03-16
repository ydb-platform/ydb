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
from typing import Any, Callable

import attrs
import requests

from ecmwf import datastores
from ecmwf.datastores import config, utils
from ecmwf.datastores.processing import ApiResponse, ApiResponsePaginated, RequestKwargs


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
        return utils.string_to_datetime(value)

    @property
    def end_datetime(self) -> datetime.datetime | None:
        """End datetime of the collection."""
        if (value := self._json_dict["extent"]["temporal"]["interval"][0][1]) is None:
            return value
        return utils.string_to_datetime(value)

    @property
    def published_at(self) -> datetime.datetime:
        """When the collection was first published."""
        return utils.string_to_datetime(self._json_dict["published"])

    @property
    def updated_at(self) -> datetime.datetime:
        """When the collection was last updated."""
        return utils.string_to_datetime(self._json_dict["updated"])

    @property
    def title(self) -> str:
        """Title of the collection."""
        value = self._json_dict["title"]
        assert isinstance(value, str)
        return value

    @property
    def description(self) -> str:
        """Description of the collection."""
        value = self._json_dict["description"]
        assert isinstance(value, str)
        return value

    @property
    def bbox(self) -> tuple[float, float, float, float]:
        """Bounding box of the collection (W, S, E, N)."""
        return tuple(self._json_dict["extent"]["spatial"]["bbox"][0])

    @property
    def id(self) -> str:
        """Collection ID."""
        return str(self._json_dict["id"])

    @property
    def _process(self) -> datastores.Process:
        url = self._get_link_href(rel="retrieve")
        return datastores.Process.from_request("get", url, **self._request_kwargs)

    @property
    def form(self) -> list[dict[str, Any]]:
        url = f"{self.url}/form.json"
        return ApiResponse.from_request(
            "get", url, log_messages=False, **self._request_kwargs
        )._json_list

    @property
    def constraints(self) -> list[dict[str, Any]]:
        url = f"{self.url}/constraints.json"
        return ApiResponse.from_request(
            "get", url, log_messages=False, **self._request_kwargs
        )._json_list

    def submit(self, request: dict[str, Any]) -> datastores.Remote:
        """Submit a request.

        Parameters
        ----------
        request: dict[str,Any]
            Request parameters.

        Returns
        -------
        datastores.Remote
        """
        return self._process.submit(request)

    def apply_constraints(self, request: dict[str, Any]) -> dict[str, Any]:
        """Apply constraints to the parameters in a request.

        Parameters
        ----------
        request: dict[str,Any]
            Request parameters.

        Returns
        -------
        dict[str,Any]
            Dictionary of valid values.
        """
        return self._process.apply_constraints(request)

    def estimate_costs(self, request: dict[str, Any]) -> dict[str, Any]:
        return self._process.estimate_costs(request)


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

    def get_collections(self, search_stats: bool = False, **params: Any) -> Collections:
        url = f"{self.url}/datasets"
        params["search_stats"] = search_stats
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
