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
Instrument `celery`_ to trace Celery applications.

.. _celery: https://pypi.org/project/celery/

Usage
-----

* Start broker backend

.. code::

    docker run -p 5672:5672 rabbitmq


* Run instrumented task

.. code:: python

    from opentelemetry.instrumentation.celery import CeleryInstrumentor

    from celery import Celery
    from celery.signals import worker_process_init

    @worker_process_init.connect(weak=False)
    def init_celery_tracing(*args, **kwargs):
        CeleryInstrumentor().instrument()

    app = Celery("tasks", broker="amqp://localhost")

    @app.task
    def add(x, y):
        return x + y

    add.delay(42, 50)

Setting up tracing
------------------

When tracing a celery worker process, tracing and instrumentation both must be initialized after the celery worker
process is initialized. This is required for any tracing components that might use threading to work correctly
such as the BatchSpanProcessor. Celery provides a signal called ``worker_process_init`` that can be used to
accomplish this as shown in the example above.

API
---
"""

import logging
from timeit import default_timer
from typing import Collection, Iterable

from billiard import VERSION
from billiard.einfo import ExceptionInfo
from celery import signals  # pylint: disable=no-name-in-module

from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.instrumentation.celery import utils
from opentelemetry.instrumentation.celery.package import _instruments
from opentelemetry.instrumentation.celery.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.metrics import get_meter
from opentelemetry.propagate import extract, inject
from opentelemetry.propagators.textmap import Getter
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.status import Status, StatusCode

if VERSION >= (4, 0, 1):
    from billiard.einfo import ExceptionWithTraceback
else:
    ExceptionWithTraceback = None

logger = logging.getLogger(__name__)

# Task operations
_TASK_TAG_KEY = "celery.action"
_TASK_APPLY_ASYNC = "apply_async"
_TASK_RUN = "run"

_TASK_RETRY_REASON_KEY = "celery.retry.reason"
_TASK_REVOKED_REASON_KEY = "celery.revoked.reason"
_TASK_REVOKED_TERMINATED_SIGNAL_KEY = "celery.terminated.signal"
_TASK_NAME_KEY = "celery.task_name"


class CeleryGetter(Getter):
    def get(self, carrier, key):
        value = getattr(carrier, key, None)
        if value is None:
            return None
        if isinstance(value, str) or not isinstance(value, Iterable):
            value = (value,)
        return value

    def keys(self, carrier):
        return []


celery_getter = CeleryGetter()


class CeleryInstrumentor(BaseInstrumentor):
    metrics = None
    task_id_to_start_time = {}

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        tracer_provider = kwargs.get("tracer_provider")

        # pylint: disable=attribute-defined-outside-init
        self._tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        meter_provider = kwargs.get("meter_provider")
        meter = get_meter(
            __name__,
            __version__,
            meter_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        self.create_celery_metrics(meter)

        signals.task_prerun.connect(self._trace_prerun, weak=False)
        signals.task_postrun.connect(self._trace_postrun, weak=False)
        signals.before_task_publish.connect(
            self._trace_before_publish, weak=False
        )
        signals.after_task_publish.connect(
            self._trace_after_publish, weak=False
        )
        signals.task_failure.connect(self._trace_failure, weak=False)
        signals.task_retry.connect(self._trace_retry, weak=False)

    def _uninstrument(self, **kwargs):
        signals.task_prerun.disconnect(self._trace_prerun)
        signals.task_postrun.disconnect(self._trace_postrun)
        signals.before_task_publish.disconnect(self._trace_before_publish)
        signals.after_task_publish.disconnect(self._trace_after_publish)
        signals.task_failure.disconnect(self._trace_failure)
        signals.task_retry.disconnect(self._trace_retry)

    def _trace_prerun(self, *args, **kwargs):
        task = utils.retrieve_task(kwargs)
        task_id = utils.retrieve_task_id(kwargs)

        if task is None or task_id is None:
            return

        self.update_task_duration_time(task_id)
        request = task.request
        tracectx = extract(request, getter=celery_getter) or None
        token = context_api.attach(tracectx) if tracectx is not None else None

        logger.debug("prerun signal start task_id=%s", task_id)

        operation_name = f"{_TASK_RUN}/{task.name}"
        span = self._tracer.start_span(
            operation_name, context=tracectx, kind=trace.SpanKind.CONSUMER
        )

        activation = trace.use_span(span, end_on_exit=True)
        activation.__enter__()  # pylint: disable=E1101
        utils.attach_context(task, task_id, span, activation, token)

    def _trace_postrun(self, *args, **kwargs):
        task = utils.retrieve_task(kwargs)
        task_id = utils.retrieve_task_id(kwargs)

        if task is None or task_id is None:
            return

        logger.debug("postrun signal task_id=%s", task_id)

        # retrieve and finish the Span
        ctx = utils.retrieve_context(task, task_id)

        if ctx is None:
            logger.warning("no existing span found for task_id=%s", task_id)
            return

        span, activation, token = ctx

        # request context tags
        if span.is_recording():
            span.set_attribute(_TASK_TAG_KEY, _TASK_RUN)
            utils.set_attributes_from_context(span, kwargs)
            utils.set_attributes_from_context(span, task.request)
            span.set_attribute(_TASK_NAME_KEY, task.name)

        activation.__exit__(None, None, None)
        utils.detach_context(task, task_id)
        self.update_task_duration_time(task_id)
        labels = {"task": task.name, "worker": task.request.hostname}
        self._record_histograms(task_id, labels)
        # if the process sending the task is not instrumented
        # there's no incoming context and no token to detach
        if token is not None:
            context_api.detach(token)

    def _trace_before_publish(self, *args, **kwargs):
        task = utils.retrieve_task_from_sender(kwargs)
        task_id = utils.retrieve_task_id_from_message(kwargs)

        if task_id is None:
            return

        if task is None:
            # task is an anonymous task send using send_task or using canvas workflow
            # Signatures() to send to a task not in the current processes dependency
            # tree
            task_name = kwargs.get("sender", "unknown")
        else:
            task_name = task.name
        operation_name = f"{_TASK_APPLY_ASYNC}/{task_name}"
        span = self._tracer.start_span(
            operation_name, kind=trace.SpanKind.PRODUCER
        )

        # apply some attributes here because most of the data is not available
        if span.is_recording():
            span.set_attribute(_TASK_TAG_KEY, _TASK_APPLY_ASYNC)
            span.set_attribute(SpanAttributes.MESSAGING_MESSAGE_ID, task_id)
            span.set_attribute(_TASK_NAME_KEY, task_name)
            utils.set_attributes_from_context(span, kwargs)

        activation = trace.use_span(span, end_on_exit=True)
        activation.__enter__()  # pylint: disable=E1101

        utils.attach_context(
            task, task_id, span, activation, None, is_publish=True
        )

        headers = kwargs.get("headers")
        if headers:
            inject(headers)

    @staticmethod
    def _trace_after_publish(*args, **kwargs):
        task = utils.retrieve_task_from_sender(kwargs)
        task_id = utils.retrieve_task_id_from_message(kwargs)

        if task is None or task_id is None:
            return

        # retrieve and finish the Span
        ctx = utils.retrieve_context(task, task_id, is_publish=True)

        if ctx is None:
            logger.warning("no existing span found for task_id=%s", task_id)
            return

        _, activation, _ = ctx

        activation.__exit__(None, None, None)  # pylint: disable=E1101
        utils.detach_context(task, task_id, is_publish=True)

    @staticmethod
    def _trace_failure(*args, **kwargs):
        task = utils.retrieve_task_from_sender(kwargs)
        task_id = utils.retrieve_task_id(kwargs)

        if task is None or task_id is None:
            return

        ctx = utils.retrieve_context(task, task_id)

        if ctx is None:
            return

        span, _, _ = ctx

        if not span.is_recording():
            return

        status_kwargs = {"status_code": StatusCode.ERROR}

        ex = kwargs.get("einfo")

        if (
            hasattr(task, "throws")
            and ex is not None
            and isinstance(ex.exception, task.throws)
        ):
            return

        if ex is not None:
            # Unwrap the actual exception wrapped by billiard's
            # `ExceptionInfo` and `ExceptionWithTraceback`.
            if isinstance(ex, ExceptionInfo) and ex.exception is not None:
                ex = ex.exception

            if (
                ExceptionWithTraceback is not None
                and isinstance(ex, ExceptionWithTraceback)
                and ex.exc is not None
            ):
                ex = ex.exc

            status_kwargs["description"] = str(ex)
            span.record_exception(ex)
        span.set_status(Status(**status_kwargs))

    @staticmethod
    def _trace_retry(*args, **kwargs):
        task = utils.retrieve_task_from_sender(kwargs)
        task_id = utils.retrieve_task_id_from_request(kwargs)
        reason = utils.retrieve_reason(kwargs)

        if task is None or task_id is None or reason is None:
            return

        ctx = utils.retrieve_context(task, task_id)

        if ctx is None:
            return

        span, _, _ = ctx

        if not span.is_recording():
            return

        # Add retry reason metadata to span
        # Use `str(reason)` instead of `reason.message` in case we get
        # something that isn't an `Exception`
        span.set_attribute(_TASK_RETRY_REASON_KEY, str(reason))

    def update_task_duration_time(self, task_id):
        cur_time = default_timer()
        task_duration_time_until_now = (
            cur_time - self.task_id_to_start_time[task_id]
            if task_id in self.task_id_to_start_time
            else cur_time
        )
        self.task_id_to_start_time[task_id] = task_duration_time_until_now

    def _record_histograms(self, task_id, metric_attributes):
        if task_id is None:
            return

        self.metrics["flower.task.runtime.seconds"].record(
            self.task_id_to_start_time.get(task_id),
            attributes=metric_attributes,
        )

    def create_celery_metrics(self, meter) -> None:
        self.metrics = {
            "flower.task.runtime.seconds": meter.create_histogram(
                name="flower.task.runtime.seconds",
                unit="seconds",
                description="The time it took to run the task.",
            )
        }
