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

from typing import Any, Callable

import attrs
import requests

from . import config, processing


@attrs.define(slots=False)
class Profile:
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
    def _request_kwargs(self) -> processing.RequestKwargs:
        return processing.RequestKwargs(
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            request_options=self.request_options,
            download_options=self.download_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
            log_callback=self.log_callback,
        )

    def _get_api_response(
        self, method: str, url: str, **kwargs: Any
    ) -> processing.ApiResponse:
        return processing.ApiResponse.from_request(
            method,
            url,
            **self._request_kwargs,
            **kwargs,
        )

    def accept_licence(self, licence_id: str, revision: int) -> dict[str, Any]:
        url = f"{self.url}/account/licences/{licence_id}"
        return self._get_api_response(
            "put", url, json={"revision": revision}
        )._json_dict

    def accepted_licences(self, **params: Any) -> dict[str, Any]:
        url = f"{self.url}/account/licences"
        return self._get_api_response("get", url, params=params)._json_dict

    def check_authentication(self) -> dict[str, Any]:
        url = f"{self.url}/account/verification/pat"
        return self._get_api_response("post", url)._json_dict
