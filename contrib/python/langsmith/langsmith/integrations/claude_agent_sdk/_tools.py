"""Thread-local storage utilities for Claude Agent SDK tracing.

This module provides thread-local storage for the parent run tree,
which is used by hooks to maintain trace context when async context
propagation is broken.
"""

import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)

# Thread-local store for passing the parent run tree into hooks.
# Claude's async event loop by default breaks tracing.
# contextvars start empty within new anyio threads. The parent run tree is threaded
# via thread-local as a fallback when context propagation isn't available.
_thread_local = threading.local()


def set_parent_run_tree(run_tree: Any) -> None:
    """Set the parent run tree in thread-local storage."""
    _thread_local.parent_run_tree = run_tree


def clear_parent_run_tree() -> None:
    """Clear the parent run tree from thread-local storage."""
    if hasattr(_thread_local, "parent_run_tree"):
        delattr(_thread_local, "parent_run_tree")


def get_parent_run_tree() -> Any:
    """Get the parent run tree from thread-local storage."""
    return getattr(_thread_local, "parent_run_tree", None)
