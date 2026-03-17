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
import asyncio
from typing import Type
from unittest import TestCase, mock, skipIf
from unittest.mock import MagicMock

from aio_pika import Exchange, RobustExchange

from opentelemetry.instrumentation.aio_pika.publish_decorator import (
    PublishDecorator,
)
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_MESSAGE_ID,
    MESSAGING_SYSTEM,
)
from opentelemetry.semconv._incubating.attributes.net_attributes import (
    NET_PEER_NAME,
    NET_PEER_PORT,
)
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind, get_tracer

from .consts import (
    AIOPIKA_VERSION_INFO,
    CHANNEL_7,
    CHANNEL_8,
    CONNECTION_7,
    CONNECTION_8,
    CORRELATION_ID,
    EXCHANGE_NAME,
    MESSAGE,
    MESSAGE_ID,
    MESSAGING_SYSTEM_VALUE,
    ROUTING_KEY,
    SERVER_HOST,
    SERVER_PORT,
)


@skipIf(AIOPIKA_VERSION_INFO >= (8, 0), "Only for aio_pika 7")
class TestInstrumentedExchangeAioRmq7(TestCase):
    EXPECTED_ATTRIBUTES = {
        MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE,
        SpanAttributes.MESSAGING_DESTINATION: f"{EXCHANGE_NAME},{ROUTING_KEY}",
        NET_PEER_NAME: SERVER_HOST,
        NET_PEER_PORT: SERVER_PORT,
        MESSAGING_MESSAGE_ID: MESSAGE_ID,
        SpanAttributes.MESSAGING_CONVERSATION_ID: CORRELATION_ID,
        SpanAttributes.MESSAGING_TEMP_DESTINATION: True,
    }

    def setUp(self):
        self.tracer = get_tracer(__name__)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def test_get_publish_span(self):
        exchange = Exchange(CONNECTION_7, CHANNEL_7, EXCHANGE_NAME)
        tracer = mock.MagicMock()
        PublishDecorator(tracer, exchange)._get_publish_span(
            MESSAGE, ROUTING_KEY
        )
        tracer.start_span.assert_called_once_with(
            f"{EXCHANGE_NAME},{ROUTING_KEY} send",
            kind=SpanKind.PRODUCER,
            attributes=self.EXPECTED_ATTRIBUTES,
        )

    def _test_publish(self, exchange_type: Type[Exchange]):
        exchange = exchange_type(CONNECTION_7, CHANNEL_7, EXCHANGE_NAME)
        with mock.patch.object(
            PublishDecorator, "_get_publish_span"
        ) as mock_get_publish_span:
            with mock.patch.object(Exchange, "publish") as mock_publish:
                decorated_publish = PublishDecorator(
                    self.tracer, exchange
                ).decorate(mock_publish)
                self.loop.run_until_complete(
                    decorated_publish(MESSAGE, ROUTING_KEY)
                )
        mock_publish.assert_called_once()
        mock_get_publish_span.assert_called_once()

    def test_publish(self):
        self._test_publish(Exchange)

    def test_robust_publish(self):
        self._test_publish(RobustExchange)

    def _test_publish_works_with_not_recording_span(self, exchange_type):
        exchange = exchange_type(CONNECTION_7, CHANNEL_7, EXCHANGE_NAME)
        with mock.patch.object(
            PublishDecorator, "_get_publish_span"
        ) as mock_get_publish_span:
            mocked_not_recording_span = MagicMock()
            mocked_not_recording_span.is_recording.return_value = False
            mock_get_publish_span.return_value = mocked_not_recording_span
            with mock.patch.object(Exchange, "publish") as mock_publish:
                with mock.patch(
                    "opentelemetry.instrumentation.aio_pika.publish_decorator.propagate.inject"
                ) as mock_inject:
                    decorated_publish = PublishDecorator(
                        self.tracer, exchange
                    ).decorate(mock_publish)
                    self.loop.run_until_complete(
                        decorated_publish(MESSAGE, ROUTING_KEY)
                    )
        mock_publish.assert_called_once()
        mock_get_publish_span.assert_called_once()
        mock_inject.assert_called_once()

    def test_publish_works_with_not_recording_span(self):
        self._test_publish_works_with_not_recording_span(Exchange)

    def test_publish_works_with_not_recording_span_robust(self):
        self._test_publish_works_with_not_recording_span(RobustExchange)


@skipIf(AIOPIKA_VERSION_INFO <= (8, 0), "Only for aio_pika 8")
class TestInstrumentedExchangeAioRmq8(TestCase):
    EXPECTED_ATTRIBUTES = {
        MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE,
        SpanAttributes.MESSAGING_DESTINATION: f"{EXCHANGE_NAME},{ROUTING_KEY}",
        NET_PEER_NAME: SERVER_HOST,
        NET_PEER_PORT: SERVER_PORT,
        MESSAGING_MESSAGE_ID: MESSAGE_ID,
        SpanAttributes.MESSAGING_CONVERSATION_ID: CORRELATION_ID,
        SpanAttributes.MESSAGING_TEMP_DESTINATION: True,
    }

    def setUp(self):
        self.tracer = get_tracer(__name__)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def test_get_publish_span(self):
        exchange = Exchange(CHANNEL_8, EXCHANGE_NAME)
        tracer = mock.MagicMock()
        PublishDecorator(tracer, exchange)._get_publish_span(
            MESSAGE, ROUTING_KEY
        )
        tracer.start_span.assert_called_once_with(
            f"{EXCHANGE_NAME},{ROUTING_KEY} send",
            kind=SpanKind.PRODUCER,
            attributes=self.EXPECTED_ATTRIBUTES,
        )

    def _test_publish(self, exchange_type: Type[Exchange]):
        exchange = exchange_type(CONNECTION_8, CHANNEL_8, EXCHANGE_NAME)
        with mock.patch.object(
            PublishDecorator, "_get_publish_span"
        ) as mock_get_publish_span:
            with mock.patch.object(Exchange, "publish") as mock_publish:
                decorated_publish = PublishDecorator(
                    self.tracer, exchange
                ).decorate(mock_publish)
                self.loop.run_until_complete(
                    decorated_publish(MESSAGE, ROUTING_KEY)
                )
        mock_publish.assert_called_once()
        mock_get_publish_span.assert_called_once()

    def test_publish(self):
        self._test_publish(Exchange)

    def test_robust_publish(self):
        self._test_publish(RobustExchange)

    def _test_publish_works_with_not_recording_span(self, exchange_type):
        exchange = exchange_type(CONNECTION_7, CHANNEL_7, EXCHANGE_NAME)
        with mock.patch.object(
            PublishDecorator, "_get_publish_span"
        ) as mock_get_publish_span:
            mocked_not_recording_span = MagicMock()
            mocked_not_recording_span.is_recording.return_value = False
            mock_get_publish_span.return_value = mocked_not_recording_span
            with mock.patch.object(Exchange, "publish") as mock_publish:
                with mock.patch(
                    "opentelemetry.instrumentation.aio_pika.publish_decorator.propagate.inject"
                ) as mock_inject:
                    decorated_publish = PublishDecorator(
                        self.tracer, exchange
                    ).decorate(mock_publish)
                    self.loop.run_until_complete(
                        decorated_publish(MESSAGE, ROUTING_KEY)
                    )
        mock_publish.assert_called_once()
        mock_get_publish_span.assert_called_once()
        mock_inject.assert_called_once()

    def test_publish_works_with_not_recording_span(self):
        self._test_publish_works_with_not_recording_span(Exchange)

    def test_publish_works_with_not_recording_span_robust(self):
        self._test_publish_works_with_not_recording_span(RobustExchange)
