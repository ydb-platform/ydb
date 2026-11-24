"""Temporal-aware sandbox client that dispatches lifecycle operations as activities."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from agents.sandbox import Manifest
from agents.sandbox.session.sandbox_client import (
    BaseSandboxClient,
    BaseSandboxClientOptions,
)
from agents.sandbox.session.sandbox_session import SandboxSession
from agents.sandbox.session.sandbox_session_state import SandboxSessionState
from agents.sandbox.snapshot import SnapshotBase, SnapshotSpec, SnapshotSpecUnion
from pydantic.type_adapter import TypeAdapter

from temporalio import workflow
from temporalio.contrib.openai_agents._errors import AgentsWorkflowError
from temporalio.contrib.openai_agents.sandbox._temporal_activity_models import (
    CreateSessionArgs,
    ResumeSessionArgs,
    SessionResult,
    StopArgs,
)
from temporalio.contrib.openai_agents.sandbox._temporal_sandbox_session import (
    TemporalSandboxSession,
)
from temporalio.workflow import ActivityConfig


class TemporalSandboxClient(BaseSandboxClient[BaseSandboxClientOptions]):
    """Stateless client that dispatches all lifecycle operations as Temporal activities.

    No inner client is needed -- session creation, resumption, and deletion are
    all handled by activities whose names are prefixed with the provider
    ``name`` (e.g. ``"daytona-sandbox_create_session"``).  The real
    ``BaseSandboxClient`` lives inside :class:`SandboxClientProvider` on the worker.

    Users should never need to instantiate this directly -- use
    :func:`temporalio.contrib.openai_agents.workflow.temporal_sandbox_client`
    instead.

    Args:
        name: The name of the :class:`SandboxClientProvider` registered on the
            worker.  Used as an activity-name prefix so that the correct
            sandbox backend is targeted.
        config: Optional activity configuration for controlling timeouts,
            retries, etc.  Defaults to a 5-minute ``start_to_close_timeout``.
    """

    def __init__(
        self,
        name: str,
        config: ActivityConfig | None = None,
    ) -> None:
        """Initialize the client."""
        self._name = name
        self._config: ActivityConfig = config or ActivityConfig(
            start_to_close_timeout=timedelta(minutes=5),
        )
        self.backend_id = name

    async def create(
        self,
        *,
        snapshot: SnapshotSpec | SnapshotBase | None = None,
        manifest: Manifest | None = None,
        options: BaseSandboxClientOptions,
    ) -> SandboxSession:
        """Create a new sandbox session via activity."""
        _reject_host_path_grants(manifest)
        result: SessionResult = await workflow.execute_activity(
            f"{self._name}-sandbox_client_create",
            arg=CreateSessionArgs(
                snapshot_spec=TypeAdapter(SnapshotSpecUnion).validate_python(snapshot)
                if isinstance(snapshot, SnapshotSpec)
                else snapshot,
                manifest=manifest,
                client_options=options,
            ),
            result_type=SessionResult,
            **self._config,
        )
        return self._wrap_session(
            TemporalSandboxSession(
                name=self._name,
                config=self._config,
                state=result.state,
                supports_pty_flag=result.supports_pty,
            ),
            # Real instrumentation runs in the activity in the real client session.
            instrumentation=None,
        )

    async def resume(self, state: SandboxSessionState) -> SandboxSession:
        """Resume an existing sandbox session via activity."""
        _reject_host_path_grants(state.manifest)
        result: SessionResult = await workflow.execute_activity(
            f"{self._name}-sandbox_client_resume",
            arg=ResumeSessionArgs(state=state),
            result_type=SessionResult,
            **self._config,
        )
        return self._wrap_session(
            TemporalSandboxSession(
                name=self._name,
                config=self._config,
                state=result.state,
                supports_pty_flag=result.supports_pty,
            ),
            # Real instrumentation runs in the activity in the real client session.
            instrumentation=None,
        )

    async def delete(self, session: TemporalSandboxSession) -> TemporalSandboxSession:  # type: ignore[override]
        """Delete a sandbox session via activity."""
        await workflow.execute_activity(
            f"{self._name}-sandbox_client_delete",
            arg=StopArgs(state=session.state),
            **self._config,
        )
        return session

    def deserialize_session_state(self, payload: dict[str, Any]) -> SandboxSessionState:
        """Deserialize a session state from a dict."""
        return SandboxSessionState.parse(payload)


def _reject_host_path_grants(manifest: Manifest | None) -> None:
    if manifest is None:
        return
    # Sandbox-side paths only: this message reaches the workflow failure event.
    bound = [g.path for g in manifest.extra_path_grants if g.host_path is not None]
    if bound:
        raise AgentsWorkflowError(
            "Sandbox path grants with a host_path are not supported by the Temporal OpenAI "
            f"Agents plugin (found: {', '.join(bound)}). A grant's host_path is written "
            "into the activity argument in plaintext. Remove host_path from these grants."
        )
