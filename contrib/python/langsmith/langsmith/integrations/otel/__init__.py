"""OpenTelemetry integration for LangSmith."""

import logging
from typing import Optional, cast

from langsmith import utils as ls_utils

from .processor import OtelExporter, OtelSpanProcessor

logger = logging.getLogger(__name__)

__all__ = ["configure", "OtelSpanProcessor", "OtelExporter"]


def configure(
    api_key: Optional[str] = None,
    project_name: Optional[str] = None,
    SpanProcessor: Optional[type] = None,
) -> bool:
    """Configure OpenTelemetry with LangSmith as the TracerProvider.

    Initializes OpenTelemetry with LangSmith as the primary and only TracerProvider.

    Usage:
        >>> from langsmith.integrations.otel import configure
        >>> configure(  # doctest: +SKIP
        ...     api_key="your-api-key", project_name="your-project"
        ... )

        Using environment variables:
        >>> # Set LANGSMITH_API_KEY and LANGSMITH_PROJECT
        >>> configure()  # Will use env vars  # doctest: +SKIP

    WARNING: This function is only for when LangSmith is your ONLY OpenTelemetry source.
    It sets the global TracerProvider, which can only be done once per application.

    This function will fail if OpenTelemetry is already initialized with another
    TracerProvider (you cannot override an existing TracerProvider).

    If you already have OpenTelemetry set up with other tools, use OtelSpanProcessor
    directly to add LangSmith to your existing setup:

    Example for adding LangSmith to existing OTEL setup:
        from opentelemetry import trace
        from langsmith.integrations.otel.processor import OtelSpanProcessor

        # Use your existing provider (already initialized)
        provider = trace.get_tracer_provider()

        # Add LangSmith processor to existing provider
        langsmith_processor = OtelSpanProcessor(
            api_key="your-api-key",
            project="your-project"
        )
        provider.add_span_processor(langsmith_processor)

    Args:
        api_key: LangSmith API key. Defaults to LANGSMITH_API_KEY env var.
        project_name: Project name. Defaults to LANGSMITH_PROJECT env var.
        SpanProcessor: Span processor class to use. Defaults to BatchSpanProcessor.

    Returns:
        True if configuration succeeded, False if TracerProvider already exists.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.trace import NoOpTracer, ProxyTracer, ProxyTracerProvider

        existing_provider = cast(TracerProvider, trace.get_tracer_provider())
        tracer = existing_provider.get_tracer(__name__)

        # Check if OpenTelemetry is in its default uninitialized state
        # (ProxyTracerProvider with NoOpTracer means no real TracerProvider was set)
        if (
            isinstance(existing_provider, ProxyTracerProvider)
            and hasattr(tracer, "_tracer")
            and isinstance(
                cast(
                    ProxyTracer,  # type: ignore[attr-defined, name-defined]
                    tracer,
                )._tracer,
                NoOpTracer,
            )
        ):
            # Safe to set TracerProvider since none exists yet
            provider = TracerProvider()
            trace.set_tracer_provider(provider)
        else:
            logger.warning(
                "OpenTelemetry TracerProvider is already set. "
                "Cannot override existing TracerProvider. Use OtelSpanProcessor "
                "directly to add LangSmith to your existing provider instead."
            )
            return False

        api_key = api_key or ls_utils.get_api_key(None)
        if not api_key:
            return False

        project_name = project_name or ls_utils.get_tracer_project()

        processor = OtelSpanProcessor(
            api_key=api_key, project=project_name, SpanProcessor=SpanProcessor
        )
        provider.add_span_processor(processor)  # type: ignore
        return True
    except Exception as e:
        logger.warning("Failed to initialize Otel for LangSmith:", e)
        return False
