from __future__ import annotations

import dataclasses
import sys
from types import FrameType, TracebackType

import temporalio.workflow
from temporalio.worker.workflow_sandbox._runner import SandboxedWorkflowRunner

from ._workflow_instance import WorkflowRunner

__all__ = [
    "_install_workflow_breakpoint_hook",
    "_relax_sandbox_for_debugger",
    "_temporal_workflow_breakpoint_hook",
]

_ORIGINAL_BREAKPOINTHOOK = sys.breakpointhook


def _build_workflow_pdb_class() -> type:
    """Build a Pdb subclass that suspends sandbox restrictions during the REPL.

    pdb's cmdloop touches ``readline.get_completer`` and other
    sandbox-restricted internals each time it interacts with the user; we
    bracket each interaction with ``_sandbox_unrestricted.value = True`` and
    restore the previous value afterwards. Outside the REPL the sandbox
    stays intact.

    ``pdb`` is imported lazily because it's a debug-only dependency that
    pulls in ``cmd``/``bdb``/``linecache``; no reason to pay that cost at
    worker import time.
    """
    import pdb

    from temporalio.workflow._sandbox import _sandbox_unrestricted

    class _WorkflowPdb(pdb.Pdb):
        # The `interaction` signature differs across Python versions: 3.10-3.12
        # typeshed names the second parameter `traceback: TracebackType | None`,
        # while 3.13+ renames it `tb_or_exc` and widens the type to include
        # `BaseException`. No single signature satisfies both stubs, so we
        # suppress the override check.
        def interaction(  # type: ignore[override]
            self,
            frame: FrameType | None,
            tb_or_exc: TracebackType | BaseException | None,
        ) -> None:
            prev = getattr(_sandbox_unrestricted, "value", False)
            _sandbox_unrestricted.value = True
            try:
                super().interaction(frame, tb_or_exc)  # type: ignore[arg-type]
            finally:
                _sandbox_unrestricted.value = prev

        # Override `q`/`quit`/`exit`/EOF (Ctrl-D) to behave like `continue`.
        # Default pdb raises `BdbQuit`, which propagates as an uncaught
        # exception out of workflow.run, fails the workflow task, and
        # triggers a server retry storm during teardown. For a debug
        # session the user almost always wants "stop debugging and let the
        # workflow finish" — that's `continue`. Users who truly want to
        # abort can Ctrl-C the outer shell.
        def do_quit(self, arg: str) -> bool | None:
            self.message(
                "[Temporal] 'q'/Ctrl-D continues the workflow. "
                "Ctrl-C the outer shell to abort."
            )
            return self.do_continue(arg)

        do_q = do_exit = do_quit
        do_EOF = do_quit

    return _WorkflowPdb


def _temporal_workflow_breakpoint_hook(*args: object, **kwargs: object) -> object:
    """``sys.breakpointhook`` that handles ``breakpoint()`` inside workflows.

    Only installed when ``debug_mode`` is enabled on the Worker. From inside
    a workflow activation: drops the user into a custom Pdb at the workflow's
    own frame, with sandbox restrictions suspended during the REPL. From
    anywhere else (test code, helpers, etc.): delegates to whatever hook was
    previously installed.
    """
    if not temporalio.workflow.in_workflow():
        # Not inside a workflow activation — let pytest's wrapper, ipdb, or
        # whatever else is configured handle it.
        return _ORIGINAL_BREAKPOINTHOOK(*args, **kwargs)
    # Inside a workflow: drop the user into pdb at the caller's frame (the
    # workflow's `run` method, where breakpoint() was actually written) rather
    # than landing inside this hook. Bypassing the configured breakpoint hook
    # also avoids pytest's pdb wrapper, which assumes a test-code context and
    # touches sandbox-restricted internals during its terminal-writer setup.
    # `sandbox_unrestricted()` lifts member checks for the duration of the
    # REPL so pdb's own initialization (readline, etc.) isn't blocked.
    # `skip` tells pdb not to stop in our hook frame or the contextlib
    # plumbing — without it pdb's first step lands at the `with` teardown
    # instead of the user's next workflow line.
    caller_frame = sys._getframe(1)
    with temporalio.workflow.unsafe.sandbox_unrestricted():
        pdb_cls = _build_workflow_pdb_class()
        pdb_cls(
            skip=[
                "temporalio.worker._debugger",
                "temporalio.workflow._sandbox",
                "contextlib",
            ]
        ).set_trace(caller_frame)
    return None


def _install_workflow_breakpoint_hook() -> None:
    """Set ``sys.breakpointhook`` to the workflow hook if it isn't already."""
    if sys.breakpointhook is not _temporal_workflow_breakpoint_hook:
        sys.breakpointhook = _temporal_workflow_breakpoint_hook


def _relax_sandbox_for_debugger(workflow_runner: WorkflowRunner) -> WorkflowRunner:
    """Allow ``breakpoint()`` past the sandbox so it can reach the worker hook.

    The sandbox flags ``breakpoint`` as non-deterministic by default; without
    this relaxation the call raises before our breakpoint hook can run.
    Once inside the hook, the hook itself enters ``sandbox_unrestricted()``
    for the duration of the debugger session, so pdb's internals (readline,
    os.environ, etc.) aren't blocked either — without permanently dropping
    sandbox checks for the rest of workflow execution.
    """
    if not isinstance(workflow_runner, SandboxedWorkflowRunner):
        return workflow_runner

    restrictions = workflow_runner.restrictions
    invalid = restrictions.invalid_module_members
    builtins_matcher = invalid.children.get("__builtins__")
    if builtins_matcher is None:
        return workflow_runner

    # `breakpoint` may sit either in `children` (as a leaf matcher with a
    # custom error message) or in `use` (the legacy flat form). Strip from
    # whichever shape is present.
    has_child = "breakpoint" in builtins_matcher.children
    has_use = "breakpoint" in builtins_matcher.use
    if not (has_child or has_use):
        return workflow_runner

    new_children = {
        k: v for k, v in builtins_matcher.children.items() if k != "breakpoint"
    }
    new_use = set(builtins_matcher.use) - {"breakpoint"}
    new_builtins = dataclasses.replace(
        builtins_matcher, children=new_children, use=new_use
    )
    new_invalid = dataclasses.replace(
        invalid, children={**invalid.children, "__builtins__": new_builtins}
    )
    new_restrictions = dataclasses.replace(
        restrictions, invalid_module_members=new_invalid
    )
    return dataclasses.replace(workflow_runner, restrictions=new_restrictions)
