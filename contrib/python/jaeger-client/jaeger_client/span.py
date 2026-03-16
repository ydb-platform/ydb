# Copyright (c) 2016-2018 Uber Technologies, Inc.
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


import threading
import time
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, List

import opentracing
from opentracing.ext import tags as ext_tags
from .tracer import Reference
from . import codecs, thrift
from .constants import SAMPLED_FLAG, DEBUG_FLAG
from .span_context import SpanContext
import jaeger_client.thrift_gen.jaeger.ttypes as ttypes

if TYPE_CHECKING:
    from .tracer import Tracer

logger = logging.getLogger('jaeger_tracing')


class Span(opentracing.Span):
    """Implements opentracing.Span."""

    __slots__ = ['_tracer', '_context',
                 'operation_name', 'start_time', 'end_time',
                 'logs', 'tags', 'finished', 'update_lock']

    def __init__(
        self,
        context: SpanContext,
        tracer: 'Tracer',
        operation_name: str,
        tags: Optional[Dict[str, Any]] = None,
        start_time: Optional[float] = None,
        references: Optional[List[Reference]] = None
    ) -> None:
        super(Span, self).__init__(context=context, tracer=tracer)
        self.operation_name = operation_name
        self.start_time = start_time or time.time()
        self.end_time: Optional[float] = None
        self.finished = False
        self.update_lock = threading.Lock()
        self.references = references
        # we store tags and logs as Thrift objects to avoid extra allocations
        self.tags: List[ttypes.Tag] = []
        self.logs: List[ttypes.Log] = []
        if tags:
            for k, v in tags.items():
                self.set_tag(k, v)

    def set_operation_name(self, operation_name: str) -> 'Span':
        """
        Set or change the operation name.

        :param operation_name: the new operation name
        :return: Returns the Span itself, for call chaining.
        """
        with self.update_lock:
            self.operation_name = operation_name
        return self

    def finish(self, finish_time: Optional[float] = None) -> None:
        """Indicate that the work represented by this span has been completed
        or terminated, and is ready to be sent to the Reporter.

        If any tags / logs need to be added to the span, it should be done
        before calling finish(), otherwise they may be ignored.

        :param finish_time: an explicit Span finish timestamp as a unix
            timestamp per time.time()
        """
        if not self.is_sampled():
            return

        with self.update_lock:
            if self.finished:
                logger.warning('Span has already been finished; will not be reported again.')
                return
            self.finished = True
            self.end_time = finish_time or time.time()

        self.tracer.report_span(self)

    def set_tag(self, key: str, value: Any) -> 'Span':
        """
        :param key:
        :param value:
        """
        with self.update_lock:
            if key == ext_tags.SAMPLING_PRIORITY and not self._set_sampling_priority(value):
                return self
            if self.is_sampled():
                tag = thrift.make_tag(
                    key=key,
                    value=value,
                    max_length=self.tracer.max_tag_value_length,
                    max_traceback_length=self._tracer.max_traceback_length,
                )
                self.tags.append(tag)
        return self

    def _set_sampling_priority(self, value):
        """
        N.B. Caller must be holding update_lock.
        """

        # Ignore debug spans trying to re-enable debug.
        if self.is_debug() and value:
            return False

        try:
            value_num = int(value)
        except ValueError:
            return False
        if value_num == 0:
            self.context.flags &= ~(SAMPLED_FLAG | DEBUG_FLAG)
            return False
        if self.tracer.is_debug_allowed(self.operation_name):
            self.context.flags |= SAMPLED_FLAG | DEBUG_FLAG
            return True
        return False

    def log_kv(self, key_values: Dict[str, Any], timestamp: Optional[float] = None) -> 'Span':
        if self.is_sampled():
            timestamp = timestamp if timestamp else time.time()
            # TODO handle exception logging, 'python.exception.type' etc.
            log = thrift.make_log(
                timestamp=timestamp if timestamp else time.time(),
                fields=key_values,
                max_length=self._tracer.max_tag_value_length,
                max_traceback_length=self._tracer.max_traceback_length,
            )
            with self.update_lock:
                self.logs.append(log)
        return self

    def set_baggage_item(self, key: str, value: Optional[str]) -> 'Span':
        prev_value = self.get_baggage_item(key=key)
        new_context = self.context.with_baggage_item(key=key, value=value)
        with self.update_lock:
            self._context = new_context
        if self.is_sampled():
            logs = {
                'event': 'baggage',
                'key': key,
                'value': value,
            }
            if prev_value:
                # TODO add metric for this
                logs['override'] = 'true'
            self.log_kv(key_values=logs)
        return self

    def get_baggage_item(self, key: str) -> Optional[str]:
        return self.context.baggage.get(key)

    def is_sampled(self) -> bool:
        return self.context.flags & SAMPLED_FLAG == SAMPLED_FLAG

    def is_debug(self) -> bool:
        return self.context.flags & DEBUG_FLAG == DEBUG_FLAG

    def is_rpc(self) -> bool:
        for tag in self.tags:
            if tag.key == ext_tags.SPAN_KIND:
                return tag.vStr == ext_tags.SPAN_KIND_RPC_CLIENT or \
                    tag.vStr == ext_tags.SPAN_KIND_RPC_SERVER
        return False

    def is_rpc_client(self) -> bool:
        for tag in self.tags:
            if tag.key == ext_tags.SPAN_KIND:
                return tag.vStr == ext_tags.SPAN_KIND_RPC_CLIENT
        return False

    @property
    def trace_id(self) -> int:
        return self.context.trace_id

    @property
    def span_id(self) -> int:
        return self.context.span_id

    @property
    def parent_id(self) -> Optional[int]:
        return self.context.parent_id

    @property
    def flags(self) -> int:
        return self.context.flags

    def __repr__(self) -> str:
        c = codecs.span_context_to_string(
            trace_id=self.context.trace_id, span_id=self.context.span_id,
            parent_id=self.context.parent_id, flags=self.context.flags)
        return '%s %s.%s' % (c, self.tracer.service_name, self.operation_name)

    def info(self, message, payload=None):
        """DEPRECATED"""
        if payload:
            self.log(event=message, payload=payload)
        else:
            self.log(event=message)
        return self

    def error(self, message, payload=None):
        """DEPRECATED"""
        self.set_tag('error', True)
        if payload:
            self.log(event=message, payload=payload)
        else:
            self.log(event=message)
        return self
