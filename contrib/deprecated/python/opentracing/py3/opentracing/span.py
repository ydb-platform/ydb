# Copyright The OpenTracing Authors
# Copyright Uber Technologies, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from . import logs

from opentracing.ext import tags


class SpanContext(object):
    """SpanContext represents :class:`Span` state that must propagate to
    descendant :class:`Span`\\ s and across process boundaries.

    SpanContext is logically divided into two pieces: the user-level "Baggage"
    (see :meth:`Span.set_baggage_item` and :meth:`Span.get_baggage_item`) that
    propagates across :class:`Span` boundaries and any
    tracer-implementation-specific fields that are needed to identify or
    otherwise contextualize the associated :class:`Span` (e.g., a ``(trace_id,
    span_id, sampled)`` tuple).
    """

    EMPTY_BAGGAGE = {}  # TODO would be nice to make this immutable

    @property
    def baggage(self):
        """
        Return baggage associated with this :class:`SpanContext`.
        If no baggage has been added to the :class:`Span`, returns an empty
        dict.

        The caller must not modify the returned dictionary.

        See also: :meth:`Span.set_baggage_item()` /
        :meth:`Span.get_baggage_item()`

        :rtype: dict
        :return: baggage associated with this :class:`SpanContext` or ``{}``.
        """
        return SpanContext.EMPTY_BAGGAGE


class Span(object):
    """
    Span represents a unit of work executed on behalf of a trace. Examples of
    spans include a remote procedure call, or a in-process method call to a
    sub-component. Every span in a trace may have zero or more causal parents,
    and these relationships transitively form a DAG. It is common for spans to
    have at most one parent, and thus most traces are merely tree structures.

    Span implements a context manager API that allows the following usage::

        with tracer.start_span(operation_name='go_fishing') as span:
            call_some_service()

    In the context manager syntax it's not necessary to call
    :meth:`Span.finish()`
    """

    def __init__(self, tracer, context):
        self._tracer = tracer
        self._context = context

    @property
    def context(self):
        """Provides access to the :class:`SpanContext` associated with this
        :class:`Span`.

        The :class:`SpanContext` contains state that propagates from
        :class:`Span` to :class:`Span` in a larger trace.

        :rtype: SpanContext
        :return: the :class:`SpanContext` associated with this :class:`Span`.
        """
        return self._context

    @property
    def tracer(self):
        """Provides access to the :class:`Tracer` that created this
        :class:`Span`.

        :rtype: Tracer
        :return: the :class:`Tracer` that created this :class:`Span`.
        """
        return self._tracer

    def set_operation_name(self, operation_name):
        """Changes the operation name.

        :param operation_name: the new operation name
        :type operation_name: str

        :rtype: Span
        :return: the :class:`Span` itself, for call chaining.
        """
        return self

    def finish(self, finish_time=None):
        """Indicates that the work represented by this :class:`Span` has completed or
        terminated.

        With the exception of the :attr:`Span.context` property, the semantics
        of all other :class:`Span` methods are undefined after
        :meth:`Span.finish()` has been invoked.

        :param finish_time: an explicit :class:`Span` finish timestamp as a
            unix timestamp per :meth:`time.time()`
        :type finish_time: float
        """
        pass

    def set_tag(self, key, value):
        """Attaches a key/value pair to the :class:`Span`.

        The value must be a string, a bool, or a numeric type.

        If the user calls set_tag multiple times for the same key,
        the behavior of the :class:`Tracer` is undefined, i.e. it is
        implementation specific whether the :class:`Tracer` will retain the
        first value, or the last value, or pick one randomly, or even keep all
        of them.

        :param key: key or name of the tag. Must be a string.
        :type key: str

        :param value: value of the tag.
        :type value: string or bool or int or float

        :rtype: Span
        :return: the :class:`Span` itself, for call chaining.
        """
        return self

    def log_kv(self, key_values, timestamp=None):
        """Adds a log record to the :class:`Span`.

        For example::

            span.log_kv({
                "event": "time to first byte",
                "packet.size": packet.size()})

            span.log_kv({"event": "two minutes ago"}, time.time() - 120)

        :param key_values: A dict of string keys and values of any type
        :type key_values: dict

        :param timestamp: A unix timestamp per :meth:`time.time()`; current
            time if ``None``
        :type timestamp: float

        :rtype: Span
        :return: the :class:`Span` itself, for call chaining.
        """
        return self

    def set_baggage_item(self, key, value):
        """Stores a Baggage item in the :class:`Span` as a key/value pair.

        Enables powerful distributed context propagation functionality where
        arbitrary application data can be carried along the full path of
        request execution throughout the system.

        Note 1: Baggage is only propagated to the future (recursive) children
        of this :class:`Span`.

        Note 2: Baggage is sent in-band with every subsequent local and remote
        calls, so this feature must be used with care.

        :param key: Baggage item key
        :type key: str

        :param value: Baggage item value
        :type value: str

        :rtype: Span
        :return: itself, for chaining the calls.
        """
        return self

    def get_baggage_item(self, key):
        """Retrieves value of the baggage item with the given key.

        :param key: key of the baggage item
        :type key: str

        :rtype: str
        :return: value of the baggage item with given key, or ``None``.
        """
        return None

    def __enter__(self):
        """Invoked when :class:`Span` is used as a context manager.

        :rtype: Span
        :return: the :class:`Span` itself
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ends context manager and calls finish() on the :class:`Span`.

        If exception has occurred during execution, it is automatically logged
        and added as a tag. :attr:`~operation.ext.tags.ERROR` will also be set
        to `True`.
        """
        Span._on_error(self, exc_type, exc_val, exc_tb)
        self.finish()

    @staticmethod
    def _on_error(span, exc_type, exc_val, exc_tb):
        if not span or not exc_val:
            return

        span.set_tag(tags.ERROR, True)
        span.log_kv({
            logs.EVENT: tags.ERROR,
            logs.MESSAGE: str(exc_val),
            logs.ERROR_OBJECT: exc_val,
            logs.ERROR_KIND: exc_type,
            logs.STACK: exc_tb,
        })

    def log_event(self, event, payload=None):
        """DEPRECATED"""
        if payload is None:
            return self.log_kv({logs.EVENT: event})
        else:
            return self.log_kv({logs.EVENT: event, 'payload': payload})

    def log(self, **kwargs):
        """DEPRECATED"""
        key_values = {}
        if logs.EVENT in kwargs:
            key_values[logs.EVENT] = kwargs[logs.EVENT]
        if 'payload' in kwargs:
            key_values['payload'] = kwargs['payload']
        timestamp = None
        if 'timestamp' in kwargs:
            timestamp = kwargs['timestamp']
        return self.log_kv(key_values, timestamp)
