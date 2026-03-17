"""OpenTelemetry span processor and exporter for LangSmith."""

import logging
import warnings
from typing import Optional
from urllib.parse import urljoin

from langsmith import utils as ls_utils

try:
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    OTEL_AVAILABLE = True
except ImportError:
    warnings.warn(
        "OpenTelemetry packages are not installed. "
        "Install optional OpenTelemetry dependencies with: "
        "pip install langsmith[otel]",
        UserWarning,
        stacklevel=2,
    )

    class OTLPSpanExporter:  # type: ignore[no-redef]
        """Mock otlp span exporter class."""

        def __init__(self, *args, **kwargs):
            """Mock init method."""
            raise ImportError(
                "OpenTelemetry packages are not installed. "
                "Install optional OpenTelemetry dependencies with: "
                "pip install langsmith[otel]"
            )

    class BatchSpanProcessor:  # type: ignore[no-redef]
        """Mock batch span processor class."""

        def __init__(self, *args, **kwargs):
            """Mock init method."""
            raise ImportError(
                "OpenTelemetry packages are not installed. "
                "Install optional OpenTelemetry dependencies with: "
                "pip install langsmith[otel]"
            )

    class trace:
        """Mock trace class."""

        @staticmethod
        def get_tracer_provider():
            """Mock get tracer provider method."""
            raise ImportError(
                "OpenTelemetry packages are not installed. "
                "Install optional OpenTelemetry dependencies with: "
                "pip install langsmith[otel]"
            )

    OTEL_AVAILABLE = False


class OtelExporter(OTLPSpanExporter):
    """A subclass of OTLPSpanExporter configured for LangSmith.

    Environment Variables:
    - LANGSMITH_API_KEY: Your LangSmith API key.
    - LANGSMITH_ENDPOINT: Base URL for LangSmith API (defaults to https://api.smith.langchain.com).
    - LANGSMITH_PROJECT: Project identifier.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        project: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        **kwargs,
    ):
        """Initialize the OtelExporter.

        Args:
            url: OTLP endpoint URL. Defaults to {LANGSMITH_ENDPOINT}/otel/v1/traces.
            api_key: LangSmith API key. Defaults to LANGSMITH_API_KEY env var.
            parent: Parent identifier (e.g., "project_name:test").
                Defaults to LANGSMITH_PARENT env var.
            headers: Additional headers to include in requests.
            **kwargs: Additional arguments passed to OTLPSpanExporter.
        """
        base_url = ls_utils.get_api_url(None)
        # Ensure base_url ends with / for proper joining
        if not base_url.endswith("/"):
            base_url += "/"
        endpoint = url or urljoin(base_url, "otel/v1/traces")
        api_key = api_key or ls_utils.get_api_key(None)
        project = project or ls_utils.get_tracer_project()
        headers = headers or {}

        if not api_key:
            raise ValueError(
                "API key is required. Provide it via api_key parameter or "
                "LANGSMITH_API_KEY environment variable."
            )

        if not project:
            project = "default"
            logging.info(
                "No project specified, using default. "
                "Configure with LANGSMITH_PROJECT environment variable or "
                "project parameter."
            )

        exporter_headers = {
            "x-api-key": api_key,
            **headers,
        }

        if project:
            exporter_headers["Langsmith-Project"] = project

        self.project = project

        super().__init__(endpoint=endpoint, headers=exporter_headers, **kwargs)


class OtelSpanProcessor:
    """A span processor for adding LangSmith to OpenTelemetry setups.

    This class combines the OtelExporter and BatchSpanProcessor
    into a single processor that can be added to any TracerProvider.

    Use this when:
    1. You already have OpenTelemetry initialized with other tools
    2. You want to add LangSmith alongside existing OTEL exporters

    Examples:
        # Fresh OpenTelemetry setup (LangSmith only):
        from langsmith.integrations.otel import configure
        configure(api_key="your-key", project="your-project")

        # Add LangSmith to existing OpenTelemetry setup:
        from opentelemetry import trace
        from langsmith.integrations.otel.processor import OtelSpanProcessor

        # Get your existing TracerProvider (already set by other tools)
        provider = trace.get_tracer_provider()

        # Add LangSmith processor alongside existing processors
        langsmith_processor = OtelSpanProcessor(
            project="your-project",
        )
        provider.add_span_processor(langsmith_processor)
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        project: Optional[str] = None,
        url: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        SpanProcessor: Optional[type] = None,
    ):
        """Initialize the OtelSpanProcessor.

        Args:
            api_key: LangSmith API key. Defaults to LANGSMITH_API_KEY env var.
            project: Project identifier. Defaults to LANGSMITH_PROJECT env var.
            url: Base URL for LangSmith API. Defaults to LANGSMITH_ENDPOINT env var
                or https://api.smith.langchain.com.
            headers: Additional headers to include in requests.
            SpanProcessor: Optional span processor class.
                Defaults to BatchSpanProcessor.
        """
        # Create the exporter
        # Convert url to the full endpoint URL that OtelExporter expects
        exporter_url = None
        if url:
            exporter_url = f"{url.rstrip('/')}/otel/v1/traces"

        self._exporter = OtelExporter(
            url=exporter_url, api_key=api_key, project=project, headers=headers
        )

        # Create the processor chain
        if not OTEL_AVAILABLE:
            raise ImportError(
                "OpenTelemetry packages are not installed. "
                "Install optional OpenTelemetry dependencies with: "
                "pip install langsmith[otel]"
            )

        if SpanProcessor is None:
            SpanProcessor = BatchSpanProcessor

        self._processor = SpanProcessor(self._exporter)

    def on_start(self, span, parent_context=None):
        """Forward span start events to the inner processor."""
        self._processor.on_start(span, parent_context)

    def on_end(self, span):
        """Forward span end events to the inner processor."""
        self._processor.on_end(span)

    def shutdown(self):
        """Shutdown processor."""
        self._processor.shutdown()

    def force_flush(self, timeout_millis=30000):
        """Force flush the inner processor."""
        return self._processor.force_flush(timeout_millis)

    @property
    def exporter(self):
        """The underlying OtelExporter."""
        return self._exporter

    @property
    def processor(self):
        """The underlying span processor."""
        return self._processor
