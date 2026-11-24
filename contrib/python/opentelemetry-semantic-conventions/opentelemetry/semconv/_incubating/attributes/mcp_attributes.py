# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

from typing_extensions import deprecated

MCP_METHOD_NAME: Final = "mcp.method.name"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

MCP_PROTOCOL_VERSION: Final = "mcp.protocol.version"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

MCP_RESOURCE_URI: Final = "mcp.resource.uri"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""

MCP_SESSION_ID: Final = "mcp.session.id"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""


@deprecated(
    "The attribute mcp.method.name is deprecated - Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai)"
)
class McpMethodNameValues(Enum):
    NOTIFICATIONS_CANCELLED = "notifications/cancelled"
    """Notification cancelling a previously-issued request."""
    INITIALIZE = "initialize"
    """Request to initialize the MCP client."""
    NOTIFICATIONS_INITIALIZED = "notifications/initialized"
    """Notification indicating that the MCP client has been initialized."""
    NOTIFICATIONS_PROGRESS = "notifications/progress"
    """Notification indicating the progress for a long-running operation."""
    PING = "ping"
    """Request to check that the other party is still alive."""
    RESOURCES_LIST = "resources/list"
    """Request to list resources available on server."""
    RESOURCES_TEMPLATES_LIST = "resources/templates/list"
    """Request to list resource templates available on server."""
    RESOURCES_READ = "resources/read"
    """Request to read a resource."""
    NOTIFICATIONS_RESOURCES_LIST_CHANGED = (
        "notifications/resources/list_changed"
    )
    """Notification indicating that the list of resources has changed."""
    RESOURCES_SUBSCRIBE = "resources/subscribe"
    """Request to subscribe to a resource."""
    RESOURCES_UNSUBSCRIBE = "resources/unsubscribe"
    """Request to unsubscribe from resource updates."""
    NOTIFICATIONS_RESOURCES_UPDATED = "notifications/resources/updated"
    """Notification indicating that a resource has been updated."""
    PROMPTS_LIST = "prompts/list"
    """Request to list prompts available on server."""
    PROMPTS_GET = "prompts/get"
    """Request to get a prompt."""
    NOTIFICATIONS_PROMPTS_LIST_CHANGED = "notifications/prompts/list_changed"
    """Notification indicating that the list of prompts has changed."""
    TOOLS_LIST = "tools/list"
    """Request to list tools available on server."""
    TOOLS_CALL = "tools/call"
    """Request to call a tool."""
    NOTIFICATIONS_TOOLS_LIST_CHANGED = "notifications/tools/list_changed"
    """Notification indicating that the list of tools has changed."""
    LOGGING_SET_LEVEL = "logging/setLevel"
    """Request to set the logging level."""
    NOTIFICATIONS_MESSAGE = "notifications/message"
    """Notification indicating that a message has been received."""
    SAMPLING_CREATE_MESSAGE = "sampling/createMessage"
    """Request to create a sampling message."""
    COMPLETION_COMPLETE = "completion/complete"
    """Request to complete a prompt."""
    ROOTS_LIST = "roots/list"
    """Request to list roots available on server."""
    NOTIFICATIONS_ROOTS_LIST_CHANGED = "notifications/roots/list_changed"
    """Notification indicating that the list of roots has changed."""
    ELICITATION_CREATE = "elicitation/create"
    """Request from the server to elicit additional information from the user via the client."""
