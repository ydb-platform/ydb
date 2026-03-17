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
from unittest import TestCase, mock, skipIf

from aio_pika import Queue

from opentelemetry.instrumentation.aio_pika.callback_decorator import (
    CallbackDecorator,
)
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION,
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
    CORRELATION_ID,
    EXCHANGE_NAME,
    MESSAGE,
    MESSAGE_ID,
    MESSAGING_SYSTEM_VALUE,
    QUEUE_NAME,
    SERVER_HOST,
    SERVER_PORT,
)


@skipIf(AIOPIKA_VERSION_INFO >= (8, 0), "Only for aio_pika 7")
class TestInstrumentedQueueAioRmq7(TestCase):
    EXPECTED_ATTRIBUTES = {
        MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE,
        SpanAttributes.MESSAGING_DESTINATION: EXCHANGE_NAME,
        NET_PEER_NAME: SERVER_HOST,
        NET_PEER_PORT: SERVER_PORT,
        MESSAGING_MESSAGE_ID: MESSAGE_ID,
        SpanAttributes.MESSAGING_CONVERSATION_ID: CORRELATION_ID,
        MESSAGING_OPERATION: "receive",
    }

    def setUp(self):
        self.tracer = get_tracer(__name__)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def test_get_callback_span(self):
        queue = Queue(CHANNEL_7, QUEUE_NAME, False, False, False, None)
        tracer = mock.MagicMock()
        CallbackDecorator(tracer, queue)._get_span(MESSAGE)
        tracer.start_span.assert_called_once_with(
            f"{EXCHANGE_NAME} receive",
            kind=SpanKind.CONSUMER,
            attributes=self.EXPECTED_ATTRIBUTES,
        )

    def test_decorate_callback(self):
        queue = Queue(CHANNEL_7, QUEUE_NAME, False, False, False, None)
        callback = mock.MagicMock(return_value=asyncio.sleep(0))
        with mock.patch.object(
            CallbackDecorator, "_get_span"
        ) as mocked_get_callback_span:
            callback_decorator = CallbackDecorator(self.tracer, queue)
            decorated_callback = callback_decorator.decorate(callback)
            self.loop.run_until_complete(decorated_callback(MESSAGE))
        mocked_get_callback_span.assert_called_once()
        callback.assert_called_once_with(MESSAGE)


@skipIf(AIOPIKA_VERSION_INFO <= (8, 0), "Only for aio_pika 8")
class TestInstrumentedQueueAioRmq8(TestCase):
    EXPECTED_ATTRIBUTES = {
        MESSAGING_SYSTEM: MESSAGING_SYSTEM_VALUE,
        SpanAttributes.MESSAGING_DESTINATION: EXCHANGE_NAME,
        NET_PEER_NAME: SERVER_HOST,
        NET_PEER_PORT: SERVER_PORT,
        MESSAGING_MESSAGE_ID: MESSAGE_ID,
        SpanAttributes.MESSAGING_CONVERSATION_ID: CORRELATION_ID,
        MESSAGING_OPERATION: "receive",
    }

    def setUp(self):
        self.tracer = get_tracer(__name__)
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def test_get_callback_span(self):
        queue = Queue(CHANNEL_8, QUEUE_NAME, False, False, False, None)
        tracer = mock.MagicMock()
        CallbackDecorator(tracer, queue)._get_span(MESSAGE)
        tracer.start_span.assert_called_once_with(
            f"{EXCHANGE_NAME} receive",
            kind=SpanKind.CONSUMER,
            attributes=self.EXPECTED_ATTRIBUTES,
        )

    def test_decorate_callback(self):
        queue = Queue(CHANNEL_8, QUEUE_NAME, False, False, False, None)
        callback = mock.MagicMock(return_value=asyncio.sleep(0))
        with mock.patch.object(
            CallbackDecorator, "_get_span"
        ) as mocked_get_callback_span:
            callback_decorator = CallbackDecorator(self.tracer, queue)
            decorated_callback = callback_decorator.decorate(callback)
            self.loop.run_until_complete(decorated_callback(MESSAGE))
        mocked_get_callback_span.assert_called_once()
        callback.assert_called_once_with(MESSAGE)
