"""Shared context (ContextVars and global defaults) that configure tracing."""

import contextvars
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

if TYPE_CHECKING:
    from langsmith.client import Client
    from langsmith.run_trees import RunTree
else:
    Client = Any  # type: ignore[assignment]
    RunTree = Any  # type: ignore[assignment]

_PROJECT_NAME = contextvars.ContextVar[Optional[str]]("_PROJECT_NAME", default=None)
_TAGS = contextvars.ContextVar[Optional[list[str]]]("_TAGS", default=None)
_METADATA = contextvars.ContextVar[Optional[dict[str, Any]]]("_METADATA", default=None)

_TRACING_ENABLED = contextvars.ContextVar[Optional[Union[bool, Literal["local"]]]](
    "_TRACING_ENABLED", default=None
)
_CLIENT = contextvars.ContextVar[Optional["Client"]]("_CLIENT", default=None)
_PARENT_RUN_TREE = contextvars.ContextVar[Optional["RunTree"]](
    "_PARENT_RUN_TREE", default=None
)
# Not thread-local, so you can set this process-wide (before asyncio.run, etc.)
_GLOBAL_PROJECT_NAME: Optional[str] = None
_GLOBAL_TAGS: Optional[list[str]] = None
_GLOBAL_METADATA: Optional[dict[str, Any]] = None
_GLOBAL_TRACING_ENABLED: Optional[Union[bool, Literal["local"]]] = None
_GLOBAL_CLIENT: Optional["Client"] = None
