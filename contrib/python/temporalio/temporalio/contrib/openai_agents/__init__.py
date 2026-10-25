"""Support for using the OpenAI Agents SDK as part of Temporal workflows.

This module provides compatibility between the
`OpenAI Agents SDK <https://github.com/openai/openai-agents-python>`_ and Temporal workflows.
"""

from temporalio.contrib.openai_agents._errors import AgentsWorkflowError
from temporalio.contrib.openai_agents._mcp import (
    StatefulMCPServerProvider,
    StatelessMCPServerProvider,
)
from temporalio.contrib.openai_agents._model_parameters import ModelActivityParameters
from temporalio.contrib.openai_agents._temporal_openai_agents import (
    OpenAIAgentsPlugin,
    OpenAIPayloadConverter,
)
from temporalio.contrib.openai_agents._temporal_worker_env_ref import (
    AllowAllWorkerEnvVars,
    temporal_worker_env_ref,
)
from temporalio.contrib.openai_agents.sandbox._sandbox_client_provider import (
    SandboxClientProvider,
)
from temporalio.contrib.openai_agents.sandbox._temporal_worker_env_value import (
    TemporalWorkerEnvValue,
)

from . import testing, workflow

__all__ = [
    "AgentsWorkflowError",
    "AllowAllWorkerEnvVars",
    "ModelActivityParameters",
    "OpenAIAgentsPlugin",
    "OpenAIPayloadConverter",
    "SandboxClientProvider",
    "StatelessMCPServerProvider",
    "StatefulMCPServerProvider",
    "TemporalWorkerEnvValue",
    "temporal_worker_env_ref",
    "testing",
    "workflow",
]
