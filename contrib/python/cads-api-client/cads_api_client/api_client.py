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

import cads_api_client

from . import __version__, catalogue, config, processing, profile


@attrs.define(slots=False)
class ApiClient:
    """A client to interact with the CADS API.

    Parameters
    ----------
    url: str or None, default: None
        API URL. If None, infer from CADS_API_URL or CADS_API_RC.
    key: str or None, default: None
        API Key. If None, infer from CADS_API_KEY or CADS_API_RC.
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
        headers = {"User-Agent": f"cads-api-client/{__version__}"}
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

    def _get_request_kwargs(
        self, key_is_mandatory: bool = True
    ) -> processing.RequestKwargs:
        return processing.RequestKwargs(
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
    def _catalogue_api(self) -> catalogue.Catalogue:
        return catalogue.Catalogue(
            f"{self.url}/catalogue",
            **self._get_request_kwargs(key_is_mandatory=False),
        )

    @functools.cached_property
    def _retrieve_api(self) -> processing.Processing:
        return processing.Processing(
            f"{self.url}/retrieve", **self._get_request_kwargs()
        )

    @functools.cached_property
    def _profile_api(self) -> profile.Profile:
        return profile.Profile(f"{self.url}/profiles", **self._get_request_kwargs())

    def accept_licence(self, licence_id: str, revision: int) -> dict[str, Any]:
        """Accept a licence.

        Parameters
        ----------
        licence_id: str
            Licence ID.
        revision: int
            Licence revision number.

        Returns
        -------
        dict[str,Any]
            Content of the response.
        """
        return self._profile_api.accept_licence(licence_id, revision=revision)

    def apply_constraints(self, collection_id: str, **request: Any) -> dict[str, Any]:
        """Apply constraints to the parameters in a request.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).
        **request: Any
            Request parameters.

        Returns
        -------
        dict[str,Any]
            Dictionary of valid values.
        """
        return self.get_process(collection_id).apply_constraints(**request)

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

    def download_results(self, request_uid: str, target: str | None = None) -> str:
        """Download the results of a request.

        Parameters
        ----------
        request_uid: str
            Request UID.
        target: str or None
            Target path. If None, download to the working directory.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        return self.get_remote(request_uid).download(target)

    def estimate_costs(self, collection_id: str, **request: Any) -> dict[str, Any]:
        """Estimate costs of the parameters in a request.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).
        **request: Any
            Request parameters.

        Returns
        -------
        dict[str,Any]
            Dictionary of estimated costs.
        """
        return self.get_process(collection_id).estimate_costs(**request)

    def get_accepted_licences(
        self,
        scope: Literal[None, "all", "dataset", "portal"] = None,
    ) -> list[dict[str, Any]]:
        """Retrieve accepted licences.

        Parameters
        ----------
        scope: {None, 'all', 'dataset', 'portal'}
            Licence scope.

        Returns
        -------
        list[dict[str,Any]]
            List of dictionaries with license information.
        """
        params = {k: v for k, v in zip(["scope"], [scope]) if v is not None}
        licences: list[dict[str, Any]]
        licences = self._profile_api.accepted_licences(**params).get("licences", [])
        return licences

    def get_collection(self, collection_id: str) -> cads_api_client.Collection:
        """Retrieve a catalogue collection.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).

        Returns
        -------
        cads_api_client.Collection
        """
        return self._catalogue_api.get_collection(collection_id)

    def get_collections(
        self,
        limit: int | None = None,
        sortby: Literal[None, "id", "relevance", "title", "update"] = None,
        query: str | None = None,
        keywords: list[str] | None = None,
    ) -> cads_api_client.Collections:
        """Retrieve catalogue collections.

        Parameters
        ----------
        limit: int | None
            Number of processes per page.
        sortby: {None, 'id', 'relevance', 'title', 'update'}
            Field to sort results by.
        query: str or None
            Full-text search query.
        keywords: list[str] or None
            Filter by keywords.

        Returns
        -------
        cads_api_client.Collections
        """
        params = {
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
        status: Literal[None, "accepted", "running", "successful", "failed"] = None,
    ) -> cads_api_client.Jobs:
        """Retrieve submitted jobs.

        Parameters
        ----------
        limit: int or None
            Number of processes per page.
        sortby: {None, 'created', '-created'}
            Field to sort results by.
        status: {None, 'accepted', 'running', 'successful', 'failed'}
            Status of the results.

        Returns
        -------
        cads_api_client.Jobs
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
        """Retrieve licences.

        Parameters
        ----------
        scope: {None, 'all', 'dataset', 'portal'}
            Licence scope.

        Returns
        -------
        list[dict[str,Any]]
            List of dictionaries with license information.
        """
        params = {k: v for k, v in zip(["scope"], [scope]) if v is not None}
        licences: list[dict[str, Any]]
        licences = self._catalogue_api.get_licenses(**params).get("licences", [])
        return licences

    def get_process(self, collection_id: str) -> cads_api_client.Process:
        """
        Retrieve a process.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).

        Returns
        -------
        cads_api_client.Process
        """
        return self._retrieve_api.get_process(collection_id)

    def get_processes(
        self,
        limit: int | None = None,
        sortby: Literal[None, "id", "-id"] = None,
    ) -> cads_api_client.Processes:
        params = {
            k: v for k, v in zip(["limit", "sortby"], [limit, sortby]) if v is not None
        }
        return self._retrieve_api.get_processes(**params)

    def get_remote(self, request_uid: str) -> cads_api_client.Remote:
        """
        Retrieve the remote object of a request.

        Parameters
        ----------
        request_uid: str
            Request UID.

        Returns
        -------
        cads_api_client.Remote
        """
        return self._retrieve_api.get_job(request_uid).make_remote()

    def get_results(self, request_uid: str) -> cads_api_client.Results:
        """
        Retrieve the results of a request.

        Parameters
        ----------
        request_uid: str
            Request UID.

        Returns
        -------
        cads_api_client.Results
        """
        return self.get_remote(request_uid).make_results()

    def retrieve(
        self,
        collection_id: str,
        target: str | None = None,
        **request: Any,
    ) -> str:
        """Submit a request and retrieve the results.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).
        target: str or None
            Target path. If None, download to the working directory.
        **request: Any
            Request parameters.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        return self.submit(collection_id, **request).download(target)

    def submit(self, collection_id: str, **request: Any) -> cads_api_client.Remote:
        """Submit a request.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).
        **request: Any
            Request parameters.

        Returns
        -------
        cads_api_client.Remote
        """
        return self._retrieve_api.submit(collection_id, **request)

    def submit_and_wait_on_results(
        self, collection_id: str, **request: Any
    ) -> cads_api_client.Results:
        """Submit a request and wait for the results to be ready.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"projections-cmip6"``).
        **request: Any
            Request parameters.

        Returns
        -------
        cads_api_client.Results
        """
        return self._retrieve_api.submit(collection_id, **request).make_results()
