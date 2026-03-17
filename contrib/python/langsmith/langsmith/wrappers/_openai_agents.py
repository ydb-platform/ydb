"""Tombstone module for backward compatibility.

This module has been moved to langsmith.integrations.openai_agents.
Imports from this location are deprecated but will continue to work.
"""

import warnings

from langsmith.integrations.openai_agents_sdk import OpenAIAgentsTracingProcessor

warnings.warn(
    "langsmith.wrappers._openai_agents is deprecated and has been moved to "
    "langsmith.integrations.openai_agents_sdk. Please update your imports.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["OpenAIAgentsTracingProcessor"]
