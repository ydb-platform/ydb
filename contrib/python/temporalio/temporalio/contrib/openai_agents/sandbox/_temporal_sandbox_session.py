"""Temporal-aware sandbox session that routes all I/O through Temporal activities."""

from __future__ import annotations

import io
from pathlib import Path

from agents.sandbox.session.base_sandbox_session import BaseSandboxSession
from agents.sandbox.session.pty_types import PtyExecUpdate
from agents.sandbox.session.sandbox_session_state import SandboxSessionState
from agents.sandbox.types import ExecResult, User

from temporalio import workflow
from temporalio.contrib.openai_agents.sandbox._temporal_activity_models import (
    ExecArgs,
    HydrateWorkspaceArgs,
    PersistWorkspaceArgs,
    PersistWorkspaceResult,
    PtyExecStartArgs,
    PtyExecUpdateResult,
    PtyWriteStdinArgs,
    ReadArgs,
    ReadResult,
    RunningArgs,
    RunningResult,
    StartArgs,
    StopArgs,
    WriteArgs,
)
from temporalio.contrib.openai_agents.sandbox._temporal_activity_models import (
    ExecResult as ExecResultModel,
)
from temporalio.workflow import ActivityConfig


class TemporalSandboxSession(BaseSandboxSession):
    """A BaseSandboxSession that routes all I/O through Temporal activities.

    This class is fully stateless with respect to the physical sandbox -- it
    holds only the serializable ``SandboxSessionState`` and a ``supports_pty``
    flag (both provided by the worker-side ``SessionResult``).

    Activity names are prefixed with the provider ``name`` so that dispatches
    reach the correct sandbox backend's activities on the worker.

    Each activity receives a single Pydantic model instance. Because the Temporal
    client is configured with ``pydantic_data_converter``, all fields are
    serialized and deserialized automatically.
    """

    def __init__(
        self,
        name: str,
        config: ActivityConfig,
        state: SandboxSessionState,
        supports_pty_flag: bool = True,
    ) -> None:
        """Initialize the session."""
        self._name = name
        self._config = config
        self._state = state
        self._supports_pty = supports_pty_flag

    @property
    def state(self) -> SandboxSessionState:
        """The current session state."""
        return self._state

    @state.setter
    def state(self, value: SandboxSessionState) -> None:  # type: ignore[reportIncompatibleVariableOverride]
        self._state = value

    async def exec(
        self,
        *command: str | Path,
        timeout: float | None = None,
        shell: bool | list[str] = True,
        user: str | User | None = None,
    ) -> ExecResult:
        """Execute a command in the sandbox via activity."""
        result: ExecResultModel = await workflow.execute_activity(
            f"{self._name}-sandbox_session_exec",
            arg=ExecArgs(
                state=self.state,
                command=[str(c) for c in command],
                timeout=timeout,
                shell=shell,
                user=user,
            ),
            result_type=ExecResultModel,
            **self._config,
        )
        return ExecResult(
            stdout=result.stdout, stderr=result.stderr, exit_code=result.exit_code
        )

    async def _exec_internal(
        self,
        *command: str | Path,
        timeout: float | None = None,
    ) -> ExecResult:
        raise NotImplementedError("TemporalSandboxSession overrides exec() directly")

    async def read(self, path: Path, *, user: str | User | None = None) -> io.IOBase:
        """Read a file from the sandbox via activity."""
        result: ReadResult = await workflow.execute_activity(
            f"{self._name}-sandbox_session_read",
            arg=ReadArgs(state=self.state, path=str(path)),
            result_type=ReadResult,
            **self._config,
        )
        return io.BytesIO(result.data)

    async def write(
        self, path: Path, data: io.IOBase, *, user: str | User | None = None
    ) -> None:
        """Write a file to the sandbox via activity."""
        await workflow.execute_activity(
            f"{self._name}-sandbox_session_write",
            arg=WriteArgs(state=self.state, path=str(path), data=data.read()),
            **self._config,
        )

    async def running(self) -> bool:
        """Check if the sandbox is running via activity."""
        result: RunningResult = await workflow.execute_activity(
            f"{self._name}-sandbox_session_running",
            arg=RunningArgs(state=self.state),
            result_type=RunningResult,
            **self._config,
        )
        return result.is_running

    async def shutdown(self) -> None:
        """Shut down the sandbox via activity."""
        await workflow.execute_activity(
            f"{self._name}-sandbox_session_shutdown",
            arg=StopArgs(state=self.state),
            **self._config,
        )

    async def persist_workspace(self) -> io.IOBase:
        """Persist the workspace via activity."""
        result: PersistWorkspaceResult = await workflow.execute_activity(
            f"{self._name}-sandbox_session_persist_workspace",
            arg=PersistWorkspaceArgs(state=self.state),
            result_type=PersistWorkspaceResult,
            **self._config,
        )
        return io.BytesIO(result.data)

    async def hydrate_workspace(self, data: io.IOBase) -> None:
        """Hydrate the workspace via activity."""
        await workflow.execute_activity(
            f"{self._name}-sandbox_session_hydrate_workspace",
            arg=HydrateWorkspaceArgs(state=self.state, data=data.read()),
            **self._config,
        )

    def supports_pty(self) -> bool:
        """Whether this session supports PTY operations."""
        return self._supports_pty

    async def pty_exec_start(
        self,
        *command: str | Path,
        timeout: float | None = None,
        shell: bool | list[str] = True,
        user: str | User | None = None,
        tty: bool = False,
        yield_time_s: float | None = None,
        max_output_tokens: int | None = None,
    ) -> PtyExecUpdate:
        """Start a PTY exec via activity."""
        result: PtyExecUpdateResult = await workflow.execute_activity(
            f"{self._name}-sandbox_session_pty_exec_start",
            arg=PtyExecStartArgs(
                state=self.state,
                command=[str(c) for c in command],
                timeout=timeout,
                shell=shell,
                user=user,
                tty=tty,
                yield_time_s=yield_time_s,
                max_output_tokens=max_output_tokens,
            ),
            result_type=PtyExecUpdateResult,
            **self._config,
        )
        return PtyExecUpdate(
            process_id=result.process_id,
            output=result.output,
            exit_code=result.exit_code,
            original_token_count=result.original_token_count,
        )

    async def pty_write_stdin(
        self,
        *,
        session_id: int,
        chars: str,
        yield_time_s: float | None = None,
        max_output_tokens: int | None = None,
    ) -> PtyExecUpdate:
        """Write to PTY stdin via activity."""
        result: PtyExecUpdateResult = await workflow.execute_activity(
            f"{self._name}-sandbox_session_pty_write_stdin",
            arg=PtyWriteStdinArgs(
                state=self.state,
                session_id=session_id,
                chars=chars,
                yield_time_s=yield_time_s,
                max_output_tokens=max_output_tokens,
            ),
            result_type=PtyExecUpdateResult,
            **self._config,
        )
        return PtyExecUpdate(
            process_id=result.process_id,
            output=result.output,
            exit_code=result.exit_code,
            original_token_count=result.original_token_count,
        )

    async def start(self) -> None:
        """Start the sandbox session via activity."""
        await workflow.execute_activity(
            f"{self._name}-sandbox_session_start",
            arg=StartArgs(state=self.state),
            **self._config,
        )

    async def stop(self) -> None:
        """Stop the sandbox session via activity."""
        await workflow.execute_activity(
            f"{self._name}-sandbox_session_stop",
            arg=StopArgs(state=self.state),
            **self._config,
        )
