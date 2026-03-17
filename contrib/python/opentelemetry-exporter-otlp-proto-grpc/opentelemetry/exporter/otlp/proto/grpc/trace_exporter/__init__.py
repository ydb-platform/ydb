# Copyright The OpenTelemetry Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""OTLP Span Exporter"""

import logging
from os import environ
from typing import Dict, Optional, Sequence, Tuple, Union
from typing import Sequence as TypingSequence

from grpc import ChannelCredentials, Compression
from opentelemetry.exporter.otlp.proto.common.trace_encoder import (
    encode_spans,
)
from opentelemetry.exporter.otlp.proto.grpc.exporter import (  # noqa: F401
    OTLPExporterMixin,
    _get_credentials,
    environ_to_compression,
    get_resource_data,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import (
    TraceServiceStub,
)
from opentelemetry.proto.common.v1.common_pb2 import (  # noqa: F401
    InstrumentationScope,
)
from opentelemetry.proto.trace.v1.trace_pb2 import (  # noqa: F401
    ResourceSpans,
    ScopeSpans,
    Status,
)
from opentelemetry.proto.trace.v1.trace_pb2 import (  # noqa: F401
    Span as CollectorSpan,
)
from opentelemetry.sdk.environment_variables import (
    _OTEL_PYTHON_EXPORTER_OTLP_GRPC_TRACES_CREDENTIAL_PROVIDER,
    OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE,
    OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE,
    OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY,
    OTEL_EXPORTER_OTLP_TRACES_COMPRESSION,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    OTEL_EXPORTER_OTLP_TRACES_HEADERS,
    OTEL_EXPORTER_OTLP_TRACES_INSECURE,
    OTEL_EXPORTER_OTLP_TRACES_TIMEOUT,
)
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

logger = logging.getLogger(__name__)


# pylint: disable=no-member
class OTLPSpanExporter(
    SpanExporter,
    OTLPExporterMixin[
        Sequence[ReadableSpan],
        ExportTraceServiceRequest,
        SpanExportResult,
        TraceServiceStub,
    ],
):
    # pylint: disable=unsubscriptable-object
    """OTLP span exporter

    Args:
        endpoint: OpenTelemetry Collector receiver endpoint
        insecure: Connection type
        credentials: Credentials object for server authentication
        headers: Headers to send when exporting
        timeout: Backend request timeout in seconds
        compression: gRPC compression method to use
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        insecure: Optional[bool] = None,
        credentials: Optional[ChannelCredentials] = None,
        headers: Optional[
            Union[TypingSequence[Tuple[str, str]], Dict[str, str], str]
        ] = None,
        timeout: Optional[float] = None,
        compression: Optional[Compression] = None,
        channel_options: Optional[Tuple[Tuple[str, str]]] = None,
    ):
        insecure_spans = environ.get(OTEL_EXPORTER_OTLP_TRACES_INSECURE)
        if insecure is None and insecure_spans is not None:
            insecure = insecure_spans.lower() == "true"

        if (
            not insecure
            and environ.get(OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE) is not None
        ):
            credentials = _get_credentials(
                credentials,
                _OTEL_PYTHON_EXPORTER_OTLP_GRPC_TRACES_CREDENTIAL_PROVIDER,
                OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE,
                OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY,
                OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE,
            )

        environ_timeout = environ.get(OTEL_EXPORTER_OTLP_TRACES_TIMEOUT)
        environ_timeout = (
            float(environ_timeout) if environ_timeout is not None else None
        )

        compression = (
            environ_to_compression(OTEL_EXPORTER_OTLP_TRACES_COMPRESSION)
            if compression is None
            else compression
        )

        OTLPExporterMixin.__init__(
            self,
            stub=TraceServiceStub,
            result=SpanExportResult,
            endpoint=endpoint
            or environ.get(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT),
            insecure=insecure,
            credentials=credentials,
            headers=headers or environ.get(OTEL_EXPORTER_OTLP_TRACES_HEADERS),
            timeout=timeout or environ_timeout,
            compression=compression,
            channel_options=channel_options,
        )

    def _translate_data(
        self, data: Sequence[ReadableSpan]
    ) -> ExportTraceServiceRequest:
        return encode_spans(data)

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return self._export(spans)

    def shutdown(self, timeout_millis: float = 30_000, **kwargs) -> None:
        OTLPExporterMixin.shutdown(self, timeout_millis=timeout_millis)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Nothing is buffered in this exporter, so this method does nothing."""
        return True

    @property
    def _exporting(self):
        return "traces"
