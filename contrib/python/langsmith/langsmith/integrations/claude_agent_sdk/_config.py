"""Configuration management for Claude Agent SDK tracing."""

from typing import Any, Optional

# Global configuration for tracing
_tracing_config: dict[str, Any] = {
    "name": None,
    "project_name": None,
    "metadata": None,
    "tags": None,
}


def set_tracing_config(
    name: Optional[str] = None,
    project_name: Optional[str] = None,
    metadata: Optional[dict] = None,
    tags: Optional[list[str]] = None,
) -> None:
    """Set the global tracing configuration for Claude Agent SDK.

    Args:
        name: Name of the root trace.
        project_name: LangSmith project to trace to.
        metadata: Metadata to associate with all traces.
        tags: Tags to associate with all traces.
    """
    global _tracing_config
    _tracing_config = {
        "name": name,
        "project_name": project_name,
        "metadata": metadata,
        "tags": tags,
    }


def get_tracing_config() -> dict[str, Any]:
    """Get the current tracing configuration."""
    return _tracing_config.copy()
