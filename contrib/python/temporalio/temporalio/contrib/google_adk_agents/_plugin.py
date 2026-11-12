from __future__ import annotations

import dataclasses
import inspect
import time
import uuid
import warnings
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from types import FrameType
from typing import Any

import opentelemetry.metrics
import opentelemetry.trace

from temporalio import workflow
from temporalio.contrib.google_adk_agents._mcp import (
    TemporalMcpToolSetProvider,
    TemporalStatefulMcpToolSetProvider,
)
from temporalio.contrib.google_adk_agents._model import (
    invoke_model,
    invoke_model_streaming,
)
from temporalio.contrib.pydantic import (
    PydanticPayloadConverter,
    ToJsonOptions,
)
from temporalio.converter import DataConverter, DefaultPayloadConverter
from temporalio.plugin import SimplePlugin
from temporalio.worker import (
    ReplayerConfig,
    WorkerConfig,
    WorkflowRunner,
)
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner


def _stacklevel_outside_temporalio() -> int:
    # Attribute provider warnings to the nearest frame outside temporalio,
    # e.g. the user's Worker(...)/Replayer(...) call or a user plugin that
    # delegates here, however many plugin frames sit in between.
    level = 1
    own_frame: FrameType | None = inspect.currentframe()
    frame = own_frame.f_back if own_frame is not None else None
    while frame is not None:
        module = frame.f_globals.get("__name__", "")
        if module != "temporalio" and not module.startswith("temporalio."):
            return level
        frame = frame.f_back
        level += 1
    return 1


def _warn_if_global_otel_providers_not_replay_safe() -> None:
    # ADK records metrics, spans, and log events through the process-global
    # OpenTelemetry providers from code that runs workflow-side, so a
    # non-replay-safe global provider re-emits that telemetry on every
    # workflow replay. Warn only on providers positively identified as
    # replay-unsafe: an OpenTelemetry SDK provider used directly as the
    # global. Anything else stays silent -- unset (proxy) and no-op providers
    # drop recordings, and unknown provider types (e.g. a custom provider
    # delegating to a replay-safe one) cannot be classified, where a false
    # positive is worse than a missed warning. The SDK logger provider is not
    # checked because its class is only importable from the underscore
    # namespace opentelemetry.sdk._logs while opentelemetry-python has not
    # promoted the logs SDK to a public namespace.
    try:
        from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
        from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
    except ImportError:
        # Without the opentelemetry-sdk package installed no SDK provider can
        # exist, so there is nothing replay-unsafe to warn about.
        return
    stacklevel = _stacklevel_outside_temporalio()
    if isinstance(opentelemetry.metrics.get_meter_provider(), SdkMeterProvider):
        warnings.warn(
            "The global OpenTelemetry MeterProvider is not replay-safe: Google ADK "
            "records metrics from workflow code, so every workflow replay will "
            "re-record them. Wrap your provider in "
            "temporalio.contrib.opentelemetry.ReplaySafeMeterProvider and make it "
            "the first and only global provider set: "
            "opentelemetry.metrics.set_meter_provider(ReplaySafeMeterProvider(provider))",
            UserWarning,
            stacklevel=stacklevel,
        )
    if isinstance(opentelemetry.trace.get_tracer_provider(), SdkTracerProvider):
        warnings.warn(
            "The global OpenTelemetry TracerProvider is not replay-safe: Google ADK "
            "creates spans from workflow code, so every workflow replay will "
            "re-emit them. Install a replay-safe provider: "
            "opentelemetry.trace.set_tracer_provider("
            "temporalio.contrib.opentelemetry.create_tracer_provider())",
            UserWarning,
            stacklevel=stacklevel,
        )


def setup_deterministic_runtime():
    """Configures ADK runtime for Temporal determinism.

    .. warning::
        This function is experimental and may change in future versions.
        Use with caution in production environments.

    This should be called at the start of a Temporal Workflow before any ADK components
    (like SessionService) are used, if they rely on runtime.get_time() or runtime.new_uuid().
    """
    try:
        import google.adk.platform.time
        import google.adk.platform.uuid

        # Define safer, context-aware providers
        def _deterministic_time_provider() -> float:
            if workflow.in_workflow():
                return workflow.now().timestamp()
            return time.time()

        def _deterministic_id_provider() -> str:
            if workflow.in_workflow():
                return str(workflow.uuid4())
            return str(uuid.uuid4())

        google.adk.platform.time.set_time_provider(_deterministic_time_provider)
        google.adk.platform.uuid.set_id_provider(_deterministic_id_provider)
    except ImportError:
        pass
    except Exception as e:
        print(f"Warning: Failed to set deterministic runtime providers: {e}")


class GoogleAdkPlugin(SimplePlugin):
    """A Temporal Worker Plugin configured for ADK.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    This plugin configures:
    - Pydantic Payload Converter (required for ADK objects).
    - Sandbox Passthrough for google.adk and google.genai modules.

    At worker and replayer configuration time it also warns when the global
    OpenTelemetry meter or tracer provider is not replay-safe, since ADK
    telemetry recorded from workflow code would duplicate on replay.
    """

    def __init__(
        self,
        toolset_providers: list[
            TemporalMcpToolSetProvider | TemporalStatefulMcpToolSetProvider
        ]
        | None = None,
    ):
        """Initializes the Temporal ADK Plugin.

        Args:
            toolset_providers: Optional list of stateless
                (:class:`TemporalMcpToolSetProvider`) or stateful
                (:class:`TemporalStatefulMcpToolSetProvider`) toolset providers
                for MCP integration.
        """

        @asynccontextmanager
        async def run_context() -> AsyncIterator[None]:
            setup_deterministic_runtime()
            yield

        def workflow_runner(runner: WorkflowRunner | None) -> WorkflowRunner:
            if not runner:
                raise ValueError("No WorkflowRunner provided to the ADK plugin.")

            # If in sandbox, add additional passthrough
            if isinstance(runner, SandboxedWorkflowRunner):
                return dataclasses.replace(
                    runner,
                    restrictions=runner.restrictions.with_passthrough_modules(
                        "google.adk", "google.genai", "mcp"
                    ),
                )
            return runner

        # Annotate as Sequence[Callable[..., Any]] because invoke_model
        # and invoke_model_streaming have different signatures, so the
        # inferred list type would not satisfy SimplePlugin's parameter.
        new_activities: list[Callable[..., Any]] = [
            invoke_model,
            invoke_model_streaming,
        ]
        if toolset_providers is not None:
            for toolset_provider in toolset_providers:
                new_activities.extend(toolset_provider._get_activities())

        super().__init__(
            name="google.AdkPlugin",
            data_converter=self._configure_data_converter,
            activities=new_activities,
            run_context=lambda: run_context(),
            workflow_runner=workflow_runner,
        )

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """See base class. Also warns when the global OpenTelemetry meter or
        tracer provider is not replay-safe, since ADK telemetry would
        duplicate on replay.
        """
        _warn_if_global_otel_providers_not_replay_safe()
        return super().configure_worker(config)

    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        """See base class. Also warns when the global OpenTelemetry meter or
        tracer provider is not replay-safe, since every replayed workflow
        would re-emit ADK telemetry.
        """
        _warn_if_global_otel_providers_not_replay_safe()
        return super().configure_replayer(config)

    def _configure_data_converter(
        self, converter: DataConverter | None
    ) -> DataConverter:
        if converter is None:
            return DataConverter(payload_converter_class=_AdkPayloadConverter)
        elif converter.payload_converter_class is DefaultPayloadConverter:
            return dataclasses.replace(
                converter, payload_converter_class=_AdkPayloadConverter
            )
        return converter


class _AdkPayloadConverter(PydanticPayloadConverter):
    """PayloadConverter for Google ADK that strips unset None fields."""

    def __init__(self) -> None:
        super().__init__(ToJsonOptions(exclude_unset=True))
