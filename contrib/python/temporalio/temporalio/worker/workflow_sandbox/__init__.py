"""Sandbox for Temporal workflows.

.. warning::
    This API for this module is considered unstable and may change in future.

This module contains the sandbox for helping ensure workflow determinism. It
does this in two ways: global state isolation and restrictions on making
non-deterministic calls. See the Python SDK documentation for how to use,
customize, and work around sandbox issues.
"""

# This module is the result of many trial-and-error efforts to isolate state and
# restrict imports.
#
# Approaches to isolating state:
#
# * Using exec() with existing modules copied and use importlib.reload
#   * Problem: Reload shares globals
# * Using exec() with custom importer that copies over some modules
#   * The current implementation
# * Using Py_NewInterpreter from
#   https://docs.python.org/3/c-api/init.html#sub-interpreter-support
#   * Not yet tried
#   * Will have to investigate whether we can pass through modules
#
# Approaches to import/call restrictions:
#
# * Using sys.addaudithook
#   * Problem: No callback for every function
# * Using sys.settrace
#   * Problem: Too expensive
# * Using sys.setprofile
#   * Problem: Only affects calls, not variable access
# * Custom importer to proxy out bad things
#   * The current implementation
#     * Could not use sys.meta_path approach because we need to pass through
#       modules, not just set their creator/executor
#   * Similar wrapper implementation to
#     https://github.com/pallets/werkzeug/blob/main/src/werkzeug/local.py
#   * TODO(cretz): Investigate whether https://github.com/GrahamDumpleton/wrapt
#     would be cleaner
#
# TODO(cretz): TODOs:
#
# * Try subinterpreter via Rust
# * Rework SandboxMatcher to be more robust and easier to build
# * Protobuf issues w/ shared static state:
#   * https://github.com/protocolbuffers/protobuf/issues/10143
#   * Waiting on https://github.com/protocolbuffers/protobuf/issues/10075 to be
#     released
# * Investigate why we can't restrict the "io" library due to BufferedIOBase
#   extension issue in shutil/compression
# * ABC issue w/ wrapped subclasses:
#   * https://bugs.python.org/issue44847
#   * https://github.com/GrahamDumpleton/wrapt/issues/130

from ._restrictions import (
    RestrictedWorkflowAccessError,
    SandboxMatcher,
    SandboxRestrictions,
)
from ._runner import SandboxedWorkflowRunner

__all__ = [
    "RestrictedWorkflowAccessError",
    "SandboxedWorkflowRunner",
    "SandboxMatcher",
    "SandboxRestrictions",
]
