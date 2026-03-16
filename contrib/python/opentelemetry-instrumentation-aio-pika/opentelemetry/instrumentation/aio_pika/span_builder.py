# Copyright The OpenTelemetry Authors
#
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
from typing import Optional

from aio_pika.abc import AbstractChannel, AbstractMessage

from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION,
    MESSAGING_SYSTEM,
)
from opentelemetry.semconv._incubating.attributes.net_attributes import (
    NET_PEER_NAME,
    NET_PEER_PORT,
)
from opentelemetry.semconv.trace import (
    MessagingOperationValues,
    SpanAttributes,
)
from opentelemetry.trace import Span, SpanKind, Tracer

_DEFAULT_ATTRIBUTES = {MESSAGING_SYSTEM: "rabbitmq"}


class SpanBuilder:
    def __init__(self, tracer: Tracer):
        self._tracer = tracer
        self._attributes = _DEFAULT_ATTRIBUTES.copy()
        self._operation: MessagingOperationValues = None
        self._kind: SpanKind = None
        self._destination: str = None

    def set_as_producer(self):
        self._kind = SpanKind.PRODUCER

    def set_as_consumer(self):
        self._kind = SpanKind.CONSUMER

    def set_operation(self, operation: MessagingOperationValues):
        self._operation = operation

    def set_destination(self, destination: str):
        self._destination = destination
        # TODO: Update this implementation once the semantic conventions for messaging stabilize
        # See: https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/messaging.md
        self._attributes[SpanAttributes.MESSAGING_DESTINATION] = destination

    def set_channel(self, channel: AbstractChannel):
        if hasattr(channel, "_connection"):
            # aio_rmq 9.1 and above removed the connection attribute from the abstract listings
            connection = channel._connection
        else:
            # aio_rmq 9.0.5 and below
            connection = channel.connection
        if hasattr(connection, "connection"):
            # aio_rmq 7
            url = connection.connection.url
        else:
            # aio_rmq 8
            url = connection.url
        self._attributes.update(
            {
                NET_PEER_NAME: url.host,
                NET_PEER_PORT: url.port or 5672,
            }
        )

    def set_message(self, message: AbstractMessage):
        properties = message.properties
        if properties.message_id:
            self._attributes[MESSAGING_MESSAGE_ID] = properties.message_id
        if properties.correlation_id:
            self._attributes[SpanAttributes.MESSAGING_CONVERSATION_ID] = (
                properties.correlation_id
            )

    def build(self) -> Optional[Span]:
        if not is_instrumentation_enabled():
            return None
        if self._operation:
            self._attributes[MESSAGING_OPERATION] = self._operation.value
        else:
            # TODO: Update this implementation once the semantic conventions for messaging stabilize
            # See: https://github.com/open-telemetry/semantic-conventions/blob/main/docs/registry/attributes/messaging.md
            self._attributes[SpanAttributes.MESSAGING_TEMP_DESTINATION] = True
        span = self._tracer.start_span(
            self._generate_span_name(),
            kind=self._kind,
            attributes=self._attributes,
        )
        return span

    def _generate_span_name(self) -> str:
        operation_value = self._operation.value if self._operation else "send"
        return f"{self._destination} {operation_value}"
