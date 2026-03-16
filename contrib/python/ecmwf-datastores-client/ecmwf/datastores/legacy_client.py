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

import collections
import logging
import typing
import warnings
from types import TracebackType
from typing import Any, Callable, TypeVar, overload

import cdsapi.api
import multiurl
import requests

from ecmwf import datastores

LOGGER = logging.getLogger(__name__)
F = TypeVar("F", bound=Callable[..., Any])


class LoggingContext:
    def __init__(self, logger: logging.Logger, quiet: bool, debug: bool) -> None:
        self.old_level = logger.level
        if quiet:
            logger.setLevel(logging.WARNING)
        else:
            logger.setLevel(logging.DEBUG if debug else logging.INFO)

        self.new_handlers = []
        if not logger.handlers:
            formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            self.new_handlers.append(handler)

        self.logger = logger

    def __enter__(self) -> logging.Logger:
        return self.logger

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.logger.setLevel(self.old_level)
        for handler in self.new_handlers:
            self.logger.removeHandler(handler)


class LegacyClient(cdsapi.api.Client):  # type: ignore[misc]
    def __init__(
        self,
        url: str | None = None,
        key: str | None = None,
        quiet: bool = False,
        debug: bool = False,
        verify: bool | int | None = None,
        timeout: int = 60,
        progress: bool = True,
        full_stack: None = None,
        delete: bool = False,
        retry_max: int = 500,
        sleep_max: float = 120,
        wait_until_complete: bool = True,
        info_callback: Callable[..., None] | None = None,
        warning_callback: Callable[..., None] | None = None,
        error_callback: Callable[..., None] | None = None,
        debug_callback: Callable[..., None] | None = None,
        metadata: None = None,
        forget: None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.issue_deprecated_kwargs_warning(
            full_stack=full_stack, metadata=metadata, forget=forget
        )

        self.url, self.key, verify = cdsapi.api.get_url_key_verify(url, key, verify)
        self.verify = bool(verify)
        self.quiet = quiet
        self._debug = debug
        self.timeout = timeout
        self.progress = progress
        self.delete = delete
        self.retry_max = retry_max
        self.sleep_max = sleep_max
        self.wait_until_complete = wait_until_complete
        self.info_callback = info_callback
        self.warning_callback = warning_callback
        self.error_callback = error_callback
        self.debug_callback = debug_callback
        self.session = requests.Session() if session is None else session

        self.client = datastores.Client(
            url=self.url,
            key=self.key,
            verify=self.verify,
            timeout=self.timeout,
            progress=self.progress,
            cleanup=self.delete,
            sleep_max=self.sleep_max,
            retry_after=self.sleep_max,
            maximum_tries=self.retry_max,
            session=self.session,
            log_callback=self.log,
        )
        self.debug(
            "CDSAPI %s",
            {
                "url": self.url,
                "key": self.key,
                "quiet": self.quiet,
                "verify": self.verify,
                "timeout": self.timeout,
                "progress": self.progress,
                "sleep_max": self.sleep_max,
                "retry_max": self.retry_max,
                "delete": self.delete,
                "datastores_version": datastores.__version__,
            },
        )

    @classmethod
    def issue_deprecated_kwargs_warning(self, **kwargs: Any) -> None:
        if kwargs := {k: v for k, v in kwargs.items() if v is not None}:
            warnings.warn(
                f"The following parameters are deprecated: {kwargs}."
                " Set them to None to silence this warning.",
                UserWarning,
            )

    @classmethod
    def raise_toolbox_error(self) -> None:
        raise NotImplementedError(
            "Legacy CDS Toolbox is now discontinued."
            " Watch for announcements/updates on new CDS improved capabilities on our Forum (https://forum.ecmwf.int/)."
        )

    @overload
    def retrieve(self, name: str, request: dict[str, Any], target: str) -> str: ...

    @overload
    def retrieve(
        self, name: str, request: dict[str, Any], target: None = ...
    ) -> datastores.Results: ...

    def retrieve(
        self, name: str, request: dict[str, Any], target: str | None = None
    ) -> str | datastores.Remote | datastores.Results:
        submitted: datastores.Remote | datastores.Results
        if self.wait_until_complete:
            submitted = self.client.submit_and_wait_on_results(
                collection_id=name,
                request=request,
            )
        else:
            submitted = self.client.submit(
                collection_id=name,
                request=request,
            )

        return submitted if target is None else submitted.download(target)

    def log(self, level: int, *args: Any, **kwargs: Any) -> None:
        with LoggingContext(
            logger=LOGGER, quiet=self.quiet, debug=self._debug
        ) as logger:
            if level == logging.INFO and self.info_callback is not None:
                self.info_callback(*args, **kwargs)
            elif level == logging.WARNING and self.warning_callback is not None:
                self.warning_callback(*args, **kwargs)
            elif level == logging.ERROR and self.error_callback is not None:
                self.error_callback(*args, **kwargs)
            elif level == logging.DEBUG and self.debug_callback is not None:
                self.debug_callback(*args, **kwargs)
            else:
                logger.log(level, *args, **kwargs)

    def info(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.INFO, *args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.WARNING, *args, **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.ERROR, *args, **kwargs)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.DEBUG, *args, **kwargs)

    @typing.no_type_check
    def service(self, name, *args, **kwargs):
        self.raise_toolbox_error()

    @typing.no_type_check
    def workflow(self, code, *args, **kwargs):
        self.raise_toolbox_error()

    def status(self, context: Any = None) -> dict[str, list[str]]:
        status = collections.defaultdict(list)
        messages = self.client._catalogue_api.messages._json_dict.get("messages", [])
        for message in messages:
            status[message["severity"]].append(message["content"])
        return dict(status)

    @typing.no_type_check
    def _download(self, results, targets=None):
        if isinstance(
            results, (cdsapi.api.Result, datastores.Remote, datastores.Results)
        ):
            if targets:
                path = targets.pop(0)
            else:
                path = None
            return results.download(path)

        if isinstance(results, (list, tuple)):
            return [self._download(x, targets) for x in results]

        if isinstance(results, dict):
            if "location" in results and "contentLength" in results:
                reply = dict(
                    location=results["location"],
                    content_length=results["contentLength"],
                    content_type=results.get("contentType"),
                )

                if targets:
                    path = targets.pop(0)
                else:
                    path = None

                return cdsapi.api.Result(self, reply).download(path)

            r = {}
            for v in results.values():
                r[v] = self._download(v, targets)
            return r

        return results

    @typing.no_type_check
    def download(self, results, targets=None):
        if targets:
            # Make a copy
            targets = [t for t in targets]
        return self._download(results, targets)

    def remote(self, url: str) -> cdsapi.api.Result:
        r = requests.head(url)
        reply = dict(
            location=url,
            content_length=r.headers["Content-Length"],
            content_type=r.headers["Content-Type"],
        )
        return cdsapi.api.Result(self, reply)

    def robust(self, call: F) -> F:
        robust: F = multiurl.robust(call, **self.client._retry_options)
        return robust
