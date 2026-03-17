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

from os import environ
from typing import Dict, Literal, Optional, Sequence, Tuple, Union
from typing import Sequence as TypingSequence

from grpc import ChannelCredentials, Compression
from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
from opentelemetry.exporter.otlp.proto.grpc.exporter import (
    OTLPExporterMixin,
    _get_credentials,
    environ_to_compression,
)
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)
from opentelemetry.proto.collector.logs.v1.logs_service_pb2_grpc import (
    LogsServiceStub,
)
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs.export import (
    LogRecordExporter,
    LogRecordExportResult,
)
from opentelemetry.sdk.environment_variables import (
    _OTEL_PYTHON_EXPORTER_OTLP_GRPC_LOGS_CREDENTIAL_PROVIDER,
    OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE,
    OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE,
    OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY,
    OTEL_EXPORTER_OTLP_LOGS_COMPRESSION,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
    OTEL_EXPORTER_OTLP_LOGS_HEADERS,
    OTEL_EXPORTER_OTLP_LOGS_INSECURE,
    OTEL_EXPORTER_OTLP_LOGS_TIMEOUT,
)


class OTLPLogExporter(
    LogRecordExporter,
    OTLPExporterMixin[
        Sequence[ReadableLogRecord],
        ExportLogsServiceRequest,
        LogRecordExportResult,
        LogsServiceStub,
    ],
):
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
        insecure_logs = environ.get(OTEL_EXPORTER_OTLP_LOGS_INSECURE)
        if insecure is None and insecure_logs is not None:
            insecure = insecure_logs.lower() == "true"

        if (
            not insecure
            and environ.get(OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE) is not None
        ):
            credentials = _get_credentials(
                credentials,
                _OTEL_PYTHON_EXPORTER_OTLP_GRPC_LOGS_CREDENTIAL_PROVIDER,
                OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE,
                OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY,
                OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE,
            )

        environ_timeout = environ.get(OTEL_EXPORTER_OTLP_LOGS_TIMEOUT)
        environ_timeout = (
            float(environ_timeout) if environ_timeout is not None else None
        )

        compression = (
            environ_to_compression(OTEL_EXPORTER_OTLP_LOGS_COMPRESSION)
            if compression is None
            else compression
        )

        OTLPExporterMixin.__init__(
            self,
            endpoint=endpoint or environ.get(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT),
            insecure=insecure,
            credentials=credentials,
            headers=headers or environ.get(OTEL_EXPORTER_OTLP_LOGS_HEADERS),
            timeout=timeout or environ_timeout,
            compression=compression,
            stub=LogsServiceStub,
            result=LogRecordExportResult,
            channel_options=channel_options,
        )

    def _translate_data(
        self, data: Sequence[ReadableLogRecord]
    ) -> ExportLogsServiceRequest:
        return encode_logs(data)

    def export(  # type: ignore [reportIncompatibleMethodOverride]
        self,
        batch: Sequence[ReadableLogRecord],
    ) -> Literal[LogRecordExportResult.SUCCESS, LogRecordExportResult.FAILURE]:
        return OTLPExporterMixin._export(self, batch)

    def shutdown(self, timeout_millis: float = 30_000, **kwargs) -> None:
        OTLPExporterMixin.shutdown(self, timeout_millis=timeout_millis)

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        """Nothing is buffered in this exporter, so this method does nothing."""
        return True

    @property
    def _exporting(self) -> str:
        return "logs"
