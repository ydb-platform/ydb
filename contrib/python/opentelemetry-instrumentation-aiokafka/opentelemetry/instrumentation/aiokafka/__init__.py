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

"""
Instrument aiokafka to report instrumentation-kafka produced and consumed messages

Usage
-----

.. code:: python

    import asyncio
    from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

    # Instrument kafka
    AIOKafkaInstrumentor().instrument()

    # report a span of type producer with the default settings
    async def produce():
        producer = AIOKafkaProducer(bootstrap_servers=['localhost:9092'])
        await producer.start()
        try:
            await producer.send_and_wait('my-topic', b'raw_bytes')
        finally:
            await producer.stop()

    # report a span of type consumer with the default settings
    async def consume():
        consumer = AIOKafkaConsumer('my-topic', group_id='my-group', bootstrap_servers=['localhost:9092'])
        await consumer.start()
        try:
            async for message in consumer:
                # process message
                print(message)
        finally:
            await consumer.stop()

    asyncio.run(produce())
    asyncio.run(consume())

The _instrument() method accepts the following keyword args:
tracer_provider (TracerProvider) - an optional tracer provider
async_produce_hook (Callable) - a function with extra user-defined logic to be performed before sending the message
this function signature is:
def async_produce_hook(span: Span, args, kwargs)
async_consume_hook (Callable) - a function with extra user-defined logic to be performed after consuming a message
this function signature is:
def async_consume_hook(span: Span, record: kafka.record.ABCRecord, args, kwargs)
for example:

.. code:: python

    import asyncio
    from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

    async def async_produce_hook(span, args, kwargs):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_async_response_hook", "some-value")

    async def async_consume_hook(span, record, args, kwargs):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_consume_hook", "some-value")

    # instrument kafka with produce and consume hooks
    AIOKafkaInstrumentor().instrument(async_produce_hook=async_produce_hook, async_consume_hook=async_consume_hook)

    # Using kafka as normal now will automatically generate spans,
    # including user custom attributes added from the hooks
    async def produce():
        producer = AIOKafkaProducer(bootstrap_servers=['localhost:9092'])
        await producer.start()
        try:
            await producer.send_and_wait('my-topic', b'raw_bytes')
        finally:
            await producer.stop()

    asyncio.run(produce())

API
___
"""

from __future__ import annotations

from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Collection

import aiokafka
from wrapt import (
    wrap_function_wrapper,  # type: ignore[reportUnknownVariableType]
)

from opentelemetry import trace
from opentelemetry.instrumentation.aiokafka.package import _instruments
from opentelemetry.instrumentation.aiokafka.utils import (
    _wrap_getmany,
    _wrap_getone,
    _wrap_send,
)
from opentelemetry.instrumentation.aiokafka.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.schemas import Schemas

if TYPE_CHECKING:
    from typing import TypedDict

    from typing_extensions import Unpack

    from .utils import ConsumeHookT, ProduceHookT

    class InstrumentKwargs(TypedDict, total=False):
        tracer_provider: trace.TracerProvider
        async_produce_hook: ProduceHookT
        async_consume_hook: ConsumeHookT

    class UninstrumentKwargs(TypedDict, total=False):
        pass


class AIOKafkaInstrumentor(BaseInstrumentor):
    """An instrumentor for kafka module
    See `BaseInstrumentor`
    """

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Unpack[InstrumentKwargs]):
        """Instruments the kafka module

        Args:
            **kwargs: Optional arguments
                ``tracer_provider``: a TracerProvider, defaults to global.
                ``async_produce_hook``: a callable to be executed just before producing a message
                ``async_consume_hook``: a callable to be executed just after consuming a message
        """
        tracer_provider = kwargs.get("tracer_provider")

        async_produce_hook = kwargs.get("async_produce_hook")
        if not iscoroutinefunction(async_produce_hook):
            async_produce_hook = None

        async_consume_hook = kwargs.get("async_consume_hook")
        if not iscoroutinefunction(async_consume_hook):
            async_consume_hook = None

        tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider=tracer_provider,
            schema_url=Schemas.V1_27_0.value,
        )

        wrap_function_wrapper(
            aiokafka.AIOKafkaProducer,
            "send",
            _wrap_send(tracer, async_produce_hook),
        )
        wrap_function_wrapper(
            aiokafka.AIOKafkaConsumer,
            "getone",
            _wrap_getone(tracer, async_consume_hook),
        )
        wrap_function_wrapper(
            aiokafka.AIOKafkaConsumer,
            "getmany",
            _wrap_getmany(tracer, async_consume_hook),
        )

    def _uninstrument(self, **kwargs: Unpack[UninstrumentKwargs]):
        unwrap(aiokafka.AIOKafkaProducer, "send")
        unwrap(aiokafka.AIOKafkaConsumer, "getone")
        unwrap(aiokafka.AIOKafkaConsumer, "getmany")
