"""Error types for the OpenAI Agents SDK Temporal integration."""

from temporalio.exceptions import TemporalError


class AgentsWorkflowError(TemporalError):
    """Error that terminates the calling workflow or update.

    Raised when the agents SDK raises an error which should terminate, or when
    the plugin rejects an unsupported configuration.
    """
