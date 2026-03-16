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

import functools
import warnings
from typing import Any, Callable, Literal

import attrs
import multiurl.base
import requests

from ecmwf import datastores
from ecmwf.datastores import config
from ecmwf.datastores.catalogue import Catalogue
from ecmwf.datastores.processing import Processing, RequestKwargs
from ecmwf.datastores.profile import Profile

T_STATUS = Literal["accepted", "running", "successful", "failed", "rejected"]


@attrs.define(slots=False)
class Client:
    """ECMWF Data Stores Service (DSS) API Python client.

    Parameters
    ----------
    url: str or None, default: None
        API URL. If None, infer from ECMWF_DATASTORES_URL or ECMWF_DATASTORES_RC_FILE.
    key: str or None, default: None
        API Key. If None, infer from ECMWF_DATASTORES_KEY or ECMWF_DATASTORES_RC_FILE.
    verify: bool, default: True
        Whether to verify the TLS certificate at the remote end.
    timeout: float or tuple[float,float], default: 60
        How many seconds to wait for the server to send data, as a float, or a (connect, read) tuple.
    progress: bool, default: True
        Whether to display the progress bar during download.
    cleanup: bool, default: False
        Whether to delete requests after completion.
    sleep_max: float, default: 120
        Maximum time to wait (in seconds) while checking for a status change.
    retry_after: float, default: 120
        Time to wait (in seconds) between retries.
    maximum_tries: int, default: 500
        Maximum number of retries.
    session: requests.Session
        Requests session.
    """

    url: str | None = None
    key: str | None = None
    verify: bool = True
    timeout: float | tuple[float, float] = 60
    progress: bool = True
    cleanup: bool = False
    sleep_max: float = 120
    retry_after: float = 120
    maximum_tries: int = 500
    session: requests.Session = attrs.field(factory=requests.Session)
    _log_callback: Callable[..., None] | None = None

    def __attrs_post_init__(self) -> None:
        if self.url is None:
            self.url = str(config.get_config("url"))

        if self.key is None:
            try:
                self.key = str(config.get_config("key"))
            except (KeyError, FileNotFoundError):
                warnings.warn("The API key is missing", UserWarning)

        try:
            self._catalogue_api.messages.log_messages()
        except Exception as exc:
            warnings.warn(str(exc), UserWarning)

    def _get_headers(self, key_is_mandatory: bool = True) -> dict[str, str]:
        headers = {"User-Agent": f"ecmwf-datastores-client/{datastores.__version__}"}
        if self.key is not None:
            headers["PRIVATE-TOKEN"] = self.key
        elif key_is_mandatory:
            raise ValueError("The API key is needed to access this resource")
        return headers

    @property
    def _retry_options(self) -> dict[str, Any]:
        return {
            "maximum_tries": self.maximum_tries,
            "retry_after": self.retry_after,
        }

    @property
    def _download_options(self) -> dict[str, Any]:
        progress_bar = (
            multiurl.base.progress_bar if self.progress else multiurl.base.NoBar
        )
        return {
            "progress_bar": progress_bar,
        }

    @property
    def _request_options(self) -> dict[str, Any]:
        return {
            "timeout": self.timeout,
            "verify": self.verify,
        }

    def _get_request_kwargs(self, key_is_mandatory: bool = True) -> RequestKwargs:
        return RequestKwargs(
            headers=self._get_headers(key_is_mandatory=key_is_mandatory),
            session=self.session,
            retry_options=self._retry_options,
            request_options=self._request_options,
            download_options=self._download_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
            log_callback=self._log_callback,
        )

    @functools.cached_property
    def _catalogue_api(self) -> Catalogue:
        return Catalogue(
            f"{self.url}/catalogue",
            **self._get_request_kwargs(key_is_mandatory=False),
        )

    @functools.cached_property
    def _retrieve_api(self) -> Processing:
        return Processing(f"{self.url}/retrieve", **self._get_request_kwargs())

    @functools.cached_property
    def _profile_api(self) -> Profile:
        return Profile(f"{self.url}/profiles", **self._get_request_kwargs())

    def accept_licence(self, licence_id: str, revision: int) -> dict[str, Any]:
        return self._profile_api.accept_licence(licence_id, revision=revision)

    def apply_constraints(
        self, collection_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        """Apply constraints to the parameters in a request.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).
        request: dict[str,Any]
            Request parameters.

        Returns
        -------
        dict[str,Any]
            Dictionary of valid values.
        """
        return self.get_process(collection_id).apply_constraints(request)

    def check_authentication(self) -> dict[str, Any]:
        """Verify authentication.

        Returns
        -------
        dict[str,Any]
            Content of the response.

        Raises
        ------
        requests.HTTPError
            If the authentication fails.
        """
        return self._profile_api.check_authentication()

    def delete(self, *request_ids: str) -> dict[str, Any]:
        """Delete requests.

        Parameters
        ----------
        *request_ids: str
            Request IDs.

        Returns
        -------
        dict[str,Any]
            Content of the response.
        """
        return self._retrieve_api.delete(*request_ids)

    def download_results(self, request_id: str, target: str | None = None) -> str:
        """Download the results of a request.

        Parameters
        ----------
        request_id: str
            Request ID.
        target: str or None
            Target path. If None, download to the working directory.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        return self.get_remote(request_id).download(target)

    def estimate_costs(self, collection_id: str, request: Any) -> dict[str, Any]:
        return self.get_process(collection_id).estimate_costs(request)

    def get_accepted_licences(
        self,
        scope: Literal[None, "all", "dataset", "portal"] = None,
    ) -> list[dict[str, Any]]:
        params = {k: v for k, v in zip(["scope"], [scope]) if v is not None}
        licences: list[dict[str, Any]]
        licences = self._profile_api.accepted_licences(**params).get("licences", [])
        return licences

    def get_collection(self, collection_id: str) -> datastores.Collection:
        """Retrieve a catalogue collection.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).

        Returns
        -------
        datastores.Collection
        """
        return self._catalogue_api.get_collection(collection_id)

    def get_collections(
        self,
        limit: int | None = None,
        sortby: Literal[None, "id", "relevance", "title", "update"] = None,
        query: str | None = None,
        keywords: list[str] | None = None,
    ) -> datastores.Collections:
        """Retrieve catalogue collections.

        Parameters
        ----------
        limit: int | None
            Number of collections per page.
        sortby: {None, 'id', 'relevance', 'title', 'update'}
            Field to sort results by.
        query: str or None
            Full-text search query.
        keywords: list[str] or None
            Filter by keywords.

        Returns
        -------
        datastores.Collections
        """
        params: dict[str, Any] = {
            k: v
            for k, v in zip(
                ["limit", "sortby", "q", "kw"], [limit, sortby, query, keywords]
            )
            if v is not None
        }
        return self._catalogue_api.get_collections(**params)

    def get_jobs(
        self,
        limit: int | None = None,
        sortby: Literal[None, "created", "-created"] = None,
        status: None | T_STATUS | list[T_STATUS] = None,
    ) -> datastores.Jobs:
        """Retrieve submitted jobs.

        Parameters
        ----------
        limit: int or None
            Number of jobs per page.
        sortby: {None, 'created', '-created'}
            Field to sort results by.
        status: None or {'accepted', 'running', 'successful', 'failed', 'rejected'} or list
            Status of the results.

        Returns
        -------
        datastores.Jobs
        """
        params = {
            k: v
            for k, v in zip(["limit", "sortby", "status"], [limit, sortby, status])
            if v is not None
        }
        return self._retrieve_api.get_jobs(**params)

    def get_licences(
        self,
        scope: Literal[None, "all", "dataset", "portal"] = None,
    ) -> list[dict[str, Any]]:
        params = {k: v for k, v in zip(["scope"], [scope]) if v is not None}
        licences: list[dict[str, Any]]
        licences = self._catalogue_api.get_licenses(**params).get("licences", [])
        return licences

    def get_process(self, collection_id: str) -> datastores.Process:
        return self._retrieve_api.get_process(collection_id)

    def get_processes(
        self,
        limit: int | None = None,
        sortby: Literal[None, "id", "-id"] = None,
    ) -> datastores.Processes:
        params = {
            k: v for k, v in zip(["limit", "sortby"], [limit, sortby]) if v is not None
        }
        return self._retrieve_api.get_processes(**params)

    def get_remote(self, request_id: str) -> datastores.Remote:
        """
        Retrieve the remote object of a request.

        Parameters
        ----------
        request_id: str
            Request ID.

        Returns
        -------
        datastores.Remote
        """
        return self._retrieve_api.get_job(request_id).get_remote()

    def get_results(self, request_id: str) -> datastores.Results:
        """
        Retrieve the results of a request.

        Parameters
        ----------
        request_id: str
            Request ID.

        Returns
        -------
        datastores.Results
        """
        return self.get_remote(request_id).get_results()

    def retrieve(
        self,
        collection_id: str,
        request: dict[str, Any],
        target: str | None = None,
    ) -> str:
        """Submit a request and retrieve the results.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).
        request: dict[str,Any]
            Request parameters.
        target: str or None
            Target path. If None, download to the working directory.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        return self.submit(collection_id, request).download(target)

    def star_collection(self, collection_id: str) -> list[str]:
        return self._profile_api.star_collection(collection_id)

    def submit(self, collection_id: str, request: dict[str, Any]) -> datastores.Remote:
        """Submit a request.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).
        request: dict[str,Any]
            Request parameters.

        Returns
        -------
        datastores.Remote
        """
        return self._retrieve_api.submit(collection_id, request)

    def submit_and_wait_on_results(
        self, collection_id: str, request: dict[str, Any]
    ) -> datastores.Results:
        """Submit a request and wait for the results to be ready.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).
        request: dict[str,Any]
            Request parameters.

        Returns
        -------
        datastores.Results
        """
        return self._retrieve_api.submit(collection_id, request).get_results()

    def unstar_collection(self, collection_id: str) -> None:
        return self._profile_api.unstar_collection(collection_id)
