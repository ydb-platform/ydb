# Copyright 2025 Google LLC
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
#

from __future__ import annotations

from abc import ABC, abstractmethod

__all__ = [
    "GeminiNextGenAPIClientAdapter",
    "AsyncGeminiNextGenAPIClientAdapter"
]

class BaseGeminiNextGenAPIClientAdapter(ABC):
    @abstractmethod
    def is_vertex_ai(self) -> bool:
        ...

    @abstractmethod
    def get_project(self) -> str | None:
        ...

    @abstractmethod
    def get_location(self) -> str | None:
        ...


class AsyncGeminiNextGenAPIClientAdapter(BaseGeminiNextGenAPIClientAdapter):
    @abstractmethod
    async def async_get_auth_headers(self) -> dict[str, str] | None:
        ...


class GeminiNextGenAPIClientAdapter(BaseGeminiNextGenAPIClientAdapter):
    @abstractmethod
    def get_auth_headers(self) -> dict[str, str] | None:
        ...
