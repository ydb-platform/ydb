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
import functools
import logging
import os
import time
import urllib.parse
import warnings
from typing import Any, Callable, Type, TypedDict, TypeVar

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

import attrs
import multiurl
import requests

import cads_api_client

from . import config

T_ApiResponse = TypeVar("T_ApiResponse", bound="ApiResponse")

LOGGER = logging.getLogger(__name__)

LEVEL_NAMES_MAPPING = {
    "CRITICAL": 50,
    "FATAL": 50,
    "ERROR": 40,
    "WARNING": 30,
    "WARN": 30,
    "INFO": 20,
    "DEBUG": 10,
    "NOTSET": 0,
}


class RequestKwargs(TypedDict):
    headers: dict[str, str]
    session: requests.Session
    retry_options: dict[str, Any]
    request_options: dict[str, Any]
    download_options: dict[str, Any]
    sleep_max: float
    cleanup: bool
    log_callback: Callable[..., None] | None


class ProcessingFailedError(RuntimeError):
    pass


class DownloadError(RuntimeError):
    pass


class LinkError(Exception):
    pass


def error_json_to_message(error_json: dict[str, Any]) -> str:
    error_messages = [
        str(error_json[key])
        for key in ("title", "traceback", "detail")
        if key in error_json
    ]
    return "\n".join(error_messages)


def cads_raise_for_status(response: requests.Response) -> None:
    if 400 <= response.status_code < 500:
        try:
            error_json = response.json()
        except Exception:
            pass
        else:
            message = "\n".join(
                [
                    f"{response.status_code} Client Error: {response.reason} for url: {response.url}",
                    error_json_to_message(error_json),
                ]
            )
            raise requests.HTTPError(message, response=response)
    response.raise_for_status()


def get_level_and_message(message: str) -> tuple[int, str]:
    level = 20
    for severity in LEVEL_NAMES_MAPPING:
        if message.startswith(severity):
            level = LEVEL_NAMES_MAPPING[severity]
            message = message.replace(severity, "", 1).lstrip(":").lstrip()
            break
    return level, message


def log(*args: Any, callback: Callable[..., None] | None = None, **kwargs: Any) -> None:
    if callback is None:
        LOGGER.log(*args, **kwargs)
    else:
        callback(*args, **kwargs)


@attrs.define(slots=False)
class ApiResponse:
    response: requests.Response
    headers: dict[str, str]
    session: requests.Session
    retry_options: dict[str, Any]
    request_options: dict[str, Any]
    download_options: dict[str, Any]
    sleep_max: float
    cleanup: bool
    log_callback: Callable[..., None] | None

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

    @classmethod
    def from_request(
        cls: Type[T_ApiResponse],
        method: str,
        url: str,
        headers: dict[str, str],
        session: requests.Session | None,
        retry_options: dict[str, Any],
        request_options: dict[str, Any],
        download_options: dict[str, Any],
        sleep_max: float,
        cleanup: bool,
        log_callback: Callable[..., None] | None,
        log_messages: bool = True,
        **kwargs: Any,
    ) -> T_ApiResponse:
        if session is None:
            session = requests.Session()
        robust_request = multiurl.robust(session.request, **retry_options)

        inputs = kwargs.get("json", {}).get("inputs", {})
        log(
            logging.DEBUG,
            f"{method.upper()} {url} {inputs or ''}".strip(),
            callback=log_callback,
        )
        response = robust_request(
            method, url, headers=headers, **request_options, **kwargs
        )
        log(logging.DEBUG, f"REPLY {response.text}", callback=log_callback)

        cads_raise_for_status(response)

        self = cls(
            response,
            headers=headers,
            session=session,
            retry_options=retry_options,
            request_options=request_options,
            download_options=download_options,
            sleep_max=sleep_max,
            cleanup=cleanup,
            log_callback=log_callback,
        )
        if log_messages:
            self.log_messages()
        return self

    @property
    def url(self) -> str:
        """URL."""
        return str(self.response.request.url)

    @property
    def json(self) -> Any:
        """Content of the response."""
        return self.response.json()

    @property
    def _json_dict(self) -> dict[str, Any]:
        assert isinstance(content := self.json, dict)
        return content

    @property
    def _json_list(self) -> list[dict[str, Any]]:
        assert isinstance(content := self.json, list)
        return content

    def log_messages(self) -> None:
        if message_str := self._json_dict.get("message"):
            level, message_str = get_level_and_message(message_str)
            self.log(level, message_str)

        messages = self._json_dict.get("messages", [])
        dataset_messages = (
            self._json_dict.get("metadata", {})
            .get("datasetMetadata", {})
            .get("messages", [])
        )
        for message_dict in messages + dataset_messages:
            if not (content := message_dict.get("content")):
                continue
            if date := message_dict.get("date"):
                content = f"[{date}] {content}"
            severity = message_dict.get("severity", "notset").upper()
            level = LEVEL_NAMES_MAPPING.get(severity, 20)
            self.log(level, content)

    def _get_links(self, rel: str | None = None) -> list[dict[str, str]]:
        links = []
        for link in self._json_dict.get("links", []):
            if rel is not None and link.get("rel") == rel:
                links.append(link)
        return links

    def _get_link_href(self, rel: str | None = None) -> str:
        links = self._get_links(rel)
        if len(links) != 1:
            raise LinkError(f"link not found or not unique {rel=}")
        return links[0]["href"]

    def _from_rel_href(self, rel: str) -> Self | None:
        rels = self._get_links(rel=rel)
        if len(rels) > 1:
            raise LinkError(f"link not unique {rel=}")

        if len(rels) == 1:
            out = self.from_request("get", rels[0]["href"], **self._request_kwargs)
        else:
            out = None
        return out

    def log(self, *args: Any, **kwargs: Any) -> None:
        log(*args, callback=self.log_callback, **kwargs)

    def info(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.INFO, *args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.WARNING, *args, **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.ERROR, *args, **kwargs)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.DEBUG, *args, **kwargs)


@attrs.define
class ApiResponsePaginated(ApiResponse):
    @property
    def next(self) -> Self | None:
        """Next page."""
        return self._from_rel_href(rel="next")

    @property
    def prev(self) -> Self | None:
        """Previous page."""
        return self._from_rel_href(rel="prev")


@attrs.define
class Processes(ApiResponsePaginated):
    @property
    def collection_ids(self) -> list[str]:
        """Available collection IDs."""
        return [proc["id"] for proc in self._json_dict["processes"]]

    @property
    def process_ids(self) -> list[str]:
        warnings.warn(
            "`.process_ids` has been deprecated, and in the future will raise an error."
            " Please use `.collection_ids` from now on.",
            DeprecationWarning,
        )
        return self.collection_ids


@attrs.define
class Process(ApiResponse):
    """A class to interact with a process."""

    @property
    def id(self) -> str:
        """Process ID."""
        process_id: str = self._json_dict["id"]
        return process_id

    def submit(self, **request: Any) -> cads_api_client.Remote:
        """Submit a request.

        Parameters
        ----------
        **request: Any
            Request parameters.

        Returns
        -------
        cads_api_client.Remote
        """
        job = Job.from_request(
            "post",
            f"{self.url}/execution",
            json={"inputs": request},
            **self._request_kwargs,
        )
        return job.make_remote()

    def apply_constraints(self, **request: Any) -> dict[str, Any]:
        """Apply constraints to the parameters in a request.

        Parameters
        ----------
        **request: Any
            Request parameters.

        Returns
        -------
        dict[str,Any]
            Dictionary of valid values.
        """
        response = ApiResponse.from_request(
            "post",
            f"{self.url}/constraints",
            json={"inputs": request},
            **self._request_kwargs,
        )
        return response._json_dict

    def estimate_costs(self, **request: Any) -> dict[str, Any]:
        """Estimate costs of the parameters in a request.

        Parameters
        ----------
        **request: Any
            Request parameters.

        Returns
        -------
        dict[str,Any]
            Dictionary of estimated costs.
        """
        response = ApiResponse.from_request(
            "post",
            f"{self.url}/costing",
            json={"inputs": request},
            **self._request_kwargs,
        )
        return response._json_dict


@attrs.define(slots=False)
class Remote:
    """A class to interact with a submitted job."""

    url: str
    headers: dict[str, str]
    session: requests.Session
    retry_options: dict[str, Any]
    request_options: dict[str, Any]
    download_options: dict[str, Any]
    sleep_max: float
    cleanup: bool
    log_callback: Callable[..., None] | None

    def __attrs_post_init__(self) -> None:
        self.log_start_time = None
        self.last_status = None
        self.info(f"Request ID is {self.request_uid}")

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

    def _log_metadata(self, metadata: dict[str, Any]) -> None:
        logs = metadata.get("log", [])
        for self.log_start_time, message in sorted(logs):
            level, message = get_level_and_message(message)
            self.log(level, message)

    def _get_api_response(self, method: str, **kwargs: Any) -> ApiResponse:
        return ApiResponse.from_request(
            method, self.url, **self._request_kwargs, **kwargs
        )

    @property
    def request_uid(self) -> str:
        """Request UID."""
        return self.url.rpartition("/")[2]

    @property
    def json(self) -> dict[str, Any]:
        """Content of the response."""
        params = {"log": True, "request": True}
        if self.log_start_time:
            params["logStartTime"] = self.log_start_time
        return self._get_api_response("get", params=params)._json_dict

    @property
    def collection_id(self) -> str:
        """Collection ID."""
        return str(self.json["processID"])

    @property
    def request(self) -> dict[str, Any]:
        """Request parameters."""
        return dict(self.json["metadata"]["request"]["ids"])

    @property
    def status(self) -> str:
        """Request status."""
        reply = self.json
        self._log_metadata(reply.get("metadata", {}))

        status = reply["status"]
        if self.last_status != status:
            self.info(f"status has been updated to {status}")
        self.last_status = status
        return str(status)

    @property
    def creation_datetime(self) -> datetime.datetime:
        """Creation datetime of the job."""
        return datetime.datetime.fromisoformat(self.json["created"])

    @property
    def start_datetime(self) -> datetime.datetime | None:
        """Start datetime of the job. If None, job has not started."""
        value = self.json.get("started")
        return value if value is None else datetime.datetime.fromisoformat(value)

    @property
    def end_datetime(self) -> datetime.datetime | None:
        """End datetime of the job. If None, job has not finished."""
        value = self.json.get("finished")
        return value if value is None else datetime.datetime.fromisoformat(value)

    def _wait_on_results(self) -> None:
        sleep = 1.0
        while not self.results_ready:
            self.debug(f"results not ready, waiting for {sleep} seconds")
            time.sleep(sleep)
            sleep = min(sleep * 1.5, self.sleep_max)

    @property
    def results_ready(self) -> bool:
        """Check if results are ready."""
        status = self.status
        if status == "successful":
            return True
        if status in ("accepted", "running"):
            return False
        if status == "failed":
            results = self.make_results(wait=False)
            raise ProcessingFailedError(error_json_to_message(results._json_dict))
        if status in ("dismissed", "deleted"):
            raise ProcessingFailedError(f"API state {status!r}")
        raise ProcessingFailedError(f"Unknown API state {status!r}")

    def make_results(self, wait: bool = True) -> Results:
        if wait:
            self._wait_on_results()
        response = self._get_api_response("get")
        try:
            results_url = response._get_link_href(rel="results")
        except LinkError:
            results_url = f"{self.url}/results"
        results = Results.from_request("get", results_url, **self._request_kwargs)
        return results

    def download(self, target: str | None = None) -> str:
        """Download the results.

        Parameters
        ----------
        target: str or None
            Target path. If None, download to the working directory.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        results = self.make_results()
        return results.download(target)

    def delete(self) -> dict[str, Any]:
        """Delete job.

        Returns
        -------
        dict[str,Any]
            Content of the response.
        """
        response = self._get_api_response("delete")
        self.cleanup = False
        return response._json_dict

    def _warn(self) -> None:
        message = (
            ".update and .reply are available for backward compatibility."
            " You can now use .download directly without needing to check whether the request is completed."
        )
        warnings.warn(message, DeprecationWarning)

    def update(self, request_id: str | None = None) -> None:
        self._warn()
        if request_id:
            assert request_id == self.request_uid
        try:
            del self.reply
        except AttributeError:
            pass
        self.reply

    @functools.cached_property
    def reply(self) -> dict[str, Any]:
        self._warn()

        reply = dict(self.json)
        reply.setdefault("state", reply["status"])

        if reply["state"] == "successful":
            reply["state"] = "completed"
        elif reply["state"] == "queued":
            reply["state"] = "accepted"
        elif reply["state"] == "failed":
            reply.setdefault("error", {})
            try:
                self.make_results()
            except Exception as exc:
                reply["error"].setdefault("message", str(exc))

        reply.setdefault("request_id", self.request_uid)
        return reply

    def log(self, *args: Any, **kwargs: Any) -> None:
        log(*args, callback=self.log_callback, **kwargs)

    def info(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.INFO, *args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.WARNING, *args, **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.ERROR, *args, **kwargs)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.DEBUG, *args, **kwargs)

    def __del__(self) -> None:
        if self.cleanup:
            try:
                self.delete()
            except Exception as exc:
                warnings.warn(str(exc), UserWarning)


@attrs.define
class Job(ApiResponse):
    def make_remote(self) -> Remote:
        if self.response.request.method == "POST":
            url = self._get_link_href(rel="monitor")
        else:
            url = self._get_link_href(rel="self")
        return Remote(url, **self._request_kwargs)


@attrs.define
class Jobs(ApiResponsePaginated):
    """A class to interact with submitted jobs."""

    @property
    def request_uids(self) -> list[str]:
        """List of request UIDs."""
        return [job["jobID"] for job in self._json_dict["jobs"]]

    @property
    def job_ids(self) -> list[str]:
        warnings.warn(
            "`.job_ids` has been deprecated, and in the future will raise an error."
            " Please use `.request_uids` from now on.",
            DeprecationWarning,
        )
        return self.request_uids


@attrs.define
class Results(ApiResponse):
    """A class to interact with the results of a job."""

    def _check_size(self, target: str) -> None:
        if (target_size := os.path.getsize(target)) != (size := self.content_length):
            raise DownloadError(
                f"Download failed: downloaded {target_size} byte(s) out of {size}"
            )

    @property
    def asset(self) -> dict[str, Any]:
        """Asset dictionary."""
        return dict(self._json_dict["asset"]["value"])

    def _download(self, url: str, target: str) -> requests.Response:
        download_options = {"stream": True, "resume_transfers": True}
        download_options.update(self.download_options)
        multiurl.download(
            url,
            target=target,
            **self.retry_options,
            **self.request_options,
            **download_options,
        )
        return requests.Response()  # mutliurl robust needs a response

    def download(
        self,
        target: str | None = None,
    ) -> str:
        """Download the results.

        Parameters
        ----------
        target: str or None
            Target path. If None, download to the working directory.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        url = self.location
        if target is None:
            parts = urllib.parse.urlparse(url)
            target = parts.path.strip("/").split("/")[-1]

        if os.path.exists(target):
            os.remove(target)

        robust_download = multiurl.robust(self._download, **self.retry_options)
        robust_download(url, target)
        self._check_size(target)
        return target

    @property
    def location(self) -> str:
        """File location."""
        result_href = self.asset["href"]
        return urllib.parse.urljoin(self.response.url, result_href)

    @property
    def content_length(self) -> int:
        """File size in Bytes."""
        return int(self.asset["file:size"])

    @property
    def content_type(self) -> str:
        """File MIME type."""
        return str(self.asset["type"])


@attrs.define(slots=False)
class Processing:
    url: str
    headers: dict[str, str]
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

    def get_processes(self, **params: Any) -> Processes:
        url = f"{self.url}/processes"
        return Processes.from_request("get", url, params=params, **self._request_kwargs)

    def get_process(self, process_id: str) -> Process:
        url = f"{self.url}/processes/{process_id}"
        return Process.from_request("get", url, **self._request_kwargs)

    def get_jobs(self, **params: Any) -> Jobs:
        url = f"{self.url}/jobs"
        return Jobs.from_request("get", url, params=params, **self._request_kwargs)

    def get_job(self, job_id: str) -> Job:
        url = f"{self.url}/jobs/{job_id}"
        return Job.from_request("get", url, **self._request_kwargs)

    def submit(self, collection_id: str, **request: Any) -> Remote:
        return self.get_process(collection_id).submit(**request)
