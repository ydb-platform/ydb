import dataclasses

from temporalio.contrib.opentelemetry import OpenTelemetryInterceptor
from temporalio.plugin import SimplePlugin
from temporalio.worker import WorkflowRunner
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner


class OpenTelemetryPlugin(SimplePlugin):
    """OpenTelemetry plugin for Temporal SDK.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    This plugin integrates OpenTelemetry tracing with the Temporal SDK, providing
    automatic span creation for workflows, activities, and other Temporal operations.
    It uses the new OpenTelemetryInterceptor implementation.

    Unlike the prior TracingInterceptor, this allows for accurate duration spans and parenting inside a workflow
    using opentelemetry.trace.get_tracer() directly.

    Your tracer provider should be created with `create_tracer_provider` for it to be used within a Temporal worker.
    """

    def __init__(self, *, add_temporal_spans: bool = False):
        """Initialize the OpenTelemetry plugin.

        Args:
            add_temporal_spans: Whether to add additional Temporal-specific spans
                for operations like StartWorkflow, RunWorkflow, etc.
        """
        interceptors = [OpenTelemetryInterceptor(add_temporal_spans)]

        def workflow_runner(runner: WorkflowRunner | None) -> WorkflowRunner:
            if not runner:
                raise ValueError(
                    "No WorkflowRunner provided to the OpenTelemetry plugin."
                )

            # If in sandbox, add additional passthrough
            if isinstance(runner, SandboxedWorkflowRunner):
                return dataclasses.replace(
                    runner,
                    restrictions=runner.restrictions.with_passthrough_modules(
                        "opentelemetry"
                    ),
                )
            return runner

        super().__init__(
            "OpenTelemetryPlugin",
            interceptors=interceptors,
            workflow_runner=workflow_runner,
        )
