# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

from typing_extensions import deprecated

OPENAI_API_TYPE: Final = "openai.api.type"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

OPENAI_REQUEST_SERVICE_TIER: Final = "openai.request.service_tier"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

OPENAI_RESPONSE_SERVICE_TIER: Final = "openai.response.service_tier"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

OPENAI_RESPONSE_SYSTEM_FINGERPRINT: Final = (
    "openai.response.system_fingerprint"
)
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""


@deprecated(
    "The attribute openai.api.type is deprecated - Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai)"
)
class OpenaiApiTypeValues(Enum):
    CHAT_COMPLETIONS = "chat_completions"
    """The OpenAI [Chat Completions API](https://developers.openai.com/api/reference/chat-completions/overview)."""
    RESPONSES = "responses"
    """The OpenAI [Responses API](https://developers.openai.com/api/reference/responses/overview)."""


@deprecated(
    "The attribute openai.request.service_tier is deprecated - Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai)"
)
class OpenaiRequestServiceTierValues(Enum):
    AUTO = "auto"
    """The system will utilize scale tier credits until they are exhausted."""
    DEFAULT = "default"
    """The system will utilize the default scale tier."""
