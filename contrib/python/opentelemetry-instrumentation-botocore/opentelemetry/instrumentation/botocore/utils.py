# Copyright The OpenTelemetry Authors
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

from urllib.parse import urlparse

from opentelemetry.semconv._incubating.attributes import (
    server_attributes as ServerAttributes,
)
from opentelemetry.util.types import AttributeValue


def get_server_attributes(endpoint_url: str) -> dict[str, AttributeValue]:
    """Extract server.* attributes from AWS endpoint URL."""
    parsed = urlparse(endpoint_url)
    attributes = {}
    if parsed.hostname:
        attributes[ServerAttributes.SERVER_ADDRESS] = parsed.hostname
        attributes[ServerAttributes.SERVER_PORT] = parsed.port or 443
    return attributes
