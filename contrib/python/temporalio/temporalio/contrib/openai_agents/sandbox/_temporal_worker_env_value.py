"""Sandbox environment value resolved from the Temporal Worker's environment."""

from __future__ import annotations

import os
from collections.abc import Collection, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Literal

from agents.sandbox.manifest import EnvValue

from temporalio import workflow
from temporalio.contrib.openai_agents._temporal_worker_env_ref import (
    AllowAllWorkerEnvVars,
    _is_resolvable,
    _snapshot_resolvable_env_vars,
)
from temporalio.exceptions import ApplicationError

_resolvable_worker_env_vars: ContextVar[frozenset[str] | AllowAllWorkerEnvVars] = (
    ContextVar("temporal_resolvable_worker_env_vars")
)


@contextmanager
def _resolvable_worker_env_vars_scope(  # type:ignore[reportUnusedFunction]
    names: Collection[str] | AllowAllWorkerEnvVars,
) -> Iterator[None]:
    token = _resolvable_worker_env_vars.set(_snapshot_resolvable_env_vars(names))
    try:
        yield
    finally:
        _resolvable_worker_env_vars.reset(token)


class TemporalWorkerEnvValue(EnvValue):
    """A sandbox environment variable whose value is read on the Temporal Worker.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    Put one in a sandbox manifest's ``Environment`` in place of the value
    itself. Only the name travels in the manifest, and the worker reads the
    value when the sandbox environment is needed. Every worker that runs sandbox
    activities must set the variable and name it in
    ``OpenAIAgentsPlugin(resolvable_worker_env_vars=[...])``.
    """

    type: Literal["temporal.worker_env_value"] = "temporal.worker_env_value"  # type: ignore[assignment]

    name: str
    """Name of the environment variable to read on the worker."""

    async def resolve(self) -> str:
        """Return the value read from the worker's environment.

        Raises:
            ApplicationError: If the variable is not resolvable on this worker,
                is unset or empty, or if called from workflow code.
        """
        if workflow.in_workflow():
            raise ApplicationError(
                "TemporalWorkerEnvValue.resolve() must run in an activity: it reads the "
                "process environment, which is non-deterministic on replay and would "
                "pull the value into workflow state.",
                type="TemporalWorkerEnvValueUnresolved",
                non_retryable=True,
            )
        resolvable = _resolvable_worker_env_vars.get(frozenset())
        if not _is_resolvable(resolvable, self.name):
            raise ApplicationError(
                f"TemporalWorkerEnvValue environment variable {self.name!r} is not "
                "resolvable on this worker. Name it in "
                "OpenAIAgentsPlugin(resolvable_worker_env_vars=[...]).",
                type="TemporalWorkerEnvValueUnresolved",
                non_retryable=True,
            )
        value = os.environ.get(self.name)
        if not value:
            raise ApplicationError(
                f"TemporalWorkerEnvValue environment variable {self.name!r} is not set, "
                "or is empty, in the worker process environment.",
                type="TemporalWorkerEnvValueUnresolved",
                non_retryable=True,
            )
        return value
