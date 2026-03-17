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


import socket

import logging
import os
import random
import sys
import time
import opentracing
from typing import Any, Dict, Optional, List, Union

from opentracing import Format, UnsupportedFormatException
from opentracing.ext import tags as ext_tags
from opentracing.scope_managers import ThreadLocalScopeManager, ScopeManager
from opentracing.tracer import Reference

from tornado.concurrent import Future

from . import constants
from .codecs import TextCodec, ZipkinCodec, ZipkinSpanFormat, BinaryCodec, Codec
from .span import Span, SAMPLED_FLAG, DEBUG_FLAG
from .span_context import SpanContext
from .metrics import Metrics, LegacyMetricsFactory, MetricsFactory
from .utils import local_ip
from .sampler import Sampler
from .reporter import BaseReporter
from .throttler import Throttler

logger = logging.getLogger('jaeger_tracing')


class Tracer(opentracing.Tracer):
    """
    N.B. metrics has been deprecated, use metrics_factory instead.
    """
    def __init__(
        self,
        service_name: str,
        reporter: BaseReporter,
        sampler: Sampler,
        metrics: Optional[Metrics] = None,
        metrics_factory: Optional[MetricsFactory] = None,
        trace_id_header: str = constants.TRACE_ID_HEADER,
        generate_128bit_trace_id: bool = False,
        baggage_header_prefix: str = constants.BAGGAGE_HEADER_PREFIX,
        debug_id_header: str = constants.DEBUG_ID_HEADER_KEY,
        one_span_per_rpc: bool = False,
        extra_codecs: Optional[Dict[str, Codec]] = None,
        tags: Optional[Dict[str, Any]] = None,
        max_tag_value_length: int = constants.MAX_TAG_VALUE_LENGTH,
        max_traceback_length: int = constants.MAX_TRACEBACK_LENGTH,
        throttler: Optional[Throttler] = None,
        scope_manager: Optional[ScopeManager] = None,
    ) -> None:
        self.service_name = service_name
        self.reporter = reporter
        self.sampler = sampler
        self.metrics_factory = metrics_factory or LegacyMetricsFactory(metrics or Metrics())
        self.metrics = TracerMetrics(self.metrics_factory)
        self.random = random.Random(time.time() * (os.getpid() or 1))
        self.debug_id_header = debug_id_header
        self.one_span_per_rpc = one_span_per_rpc
        self.max_tag_value_length = max_tag_value_length
        self.max_traceback_length = max_traceback_length
        self.max_trace_id_bits = constants._max_trace_id_bits if generate_128bit_trace_id \
            else constants._max_id_bits
        self.codecs = {
            Format.TEXT_MAP: TextCodec(
                url_encoding=False,
                trace_id_header=trace_id_header,
                baggage_header_prefix=baggage_header_prefix,
                debug_id_header=debug_id_header,
            ),
            Format.HTTP_HEADERS: TextCodec(
                url_encoding=True,
                trace_id_header=trace_id_header,
                baggage_header_prefix=baggage_header_prefix,
                debug_id_header=debug_id_header,
            ),
            Format.BINARY: BinaryCodec(),
            ZipkinSpanFormat: ZipkinCodec(),
        }
        if extra_codecs:
            self.codecs.update(extra_codecs)
        self.tags: dict = {
            constants.JAEGER_VERSION_TAG_KEY: constants.JAEGER_CLIENT_VERSION,
        }
        if tags:
            self.tags.update(tags)

        if self.tags.get(constants.JAEGER_IP_TAG_KEY) is None:
            self.tags[constants.JAEGER_IP_TAG_KEY] = local_ip()

        if self.tags.get(constants.JAEGER_HOSTNAME_TAG_KEY) is None:
            try:
                hostname = socket.gethostname()
                self.tags[constants.JAEGER_HOSTNAME_TAG_KEY] = hostname
            except socket.error:
                logger.exception('Unable to determine host name')

        self.throttler = throttler
        if self.throttler:
            client_id = random.randint(0, sys.maxsize)
            self.throttler.set_client_id(client_id)
            self.tags[constants.CLIENT_UUID_TAG_KEY] = client_id

        self.reporter.set_process(
            service_name=self.service_name,
            tags=self.tags,
            max_length=self.max_tag_value_length,
        )

        super(Tracer, self).__init__(
            scope_manager=scope_manager or ThreadLocalScopeManager()
        )

    def start_span(self,
                   operation_name: Optional[str] = None,
                   child_of: Union[None, Span, SpanContext] = None,
                   references: Union[List[Reference], None, Reference] = None,
                   tags: Union[dict, None] = None,
                   start_time: Optional[float] = None,
                   ignore_active_span: bool = False,
                   ) -> Span:
        """
        Start and return a new Span representing a unit of work.

        :param operation_name: name of the operation represented by the new
            span from the perspective of the current service.
        :param child_of: shortcut for 'child_of' reference
        :param references: (optional) either a single Reference object or a
            list of Reference objects that identify one or more parent
            SpanContexts. (See the opentracing.Reference documentation for detail)
        :param tags: optional dictionary of Span Tags. The caller gives up
            ownership of that dictionary, because the Tracer may use it as-is
            to avoid extra data copying.
        :param start_time: an explicit Span start time as a unix timestamp per
            time.time()
        :param ignore_active_span: an explicit flag that ignores the current
            active :class:`Scope` and creates a root :class:`Span`

        :return: Returns an already-started Span instance.
        """
        parent = child_of

        if self.active_span is not None \
                and not ignore_active_span \
                and not parent:
            parent = self.active_span

        # allow Span to be passed as reference, not just SpanContext
        if isinstance(parent, Span):
            parent = parent.context

        valid_references = None
        if references:
            valid_references = list()
            if not isinstance(references, list):
                references = [references]
            for reference in references:
                if reference.referenced_context is not None:
                    valid_references.append(reference)

        # setting first reference as parent
        if valid_references and (parent is None or not parent.has_trace):
            parent = valid_references[0].referenced_context

        rpc_server = bool(tags and tags.get(ext_tags.SPAN_KIND) == ext_tags.SPAN_KIND_RPC_SERVER)

        if parent is None or not parent.has_trace:
            trace_id = self._random_id(self.max_trace_id_bits)
            span_id = self._random_id(constants._max_id_bits)
            parent_id = None
            flags = 0
            baggage = None
            if parent is None:
                sampled, sampler_tags = \
                    self.sampler.is_sampled(trace_id, operation_name or '')
                if sampled:
                    flags = SAMPLED_FLAG
                    tags = tags or {}
                    for k, v in sampler_tags.items():
                        tags[k] = v
            elif parent.debug_id and self.is_debug_allowed(operation_name):
                flags = SAMPLED_FLAG | DEBUG_FLAG
                tags = tags or {}
                tags[self.debug_id_header] = parent.debug_id
            if parent and parent.baggage:
                baggage = dict(parent.baggage)  # TODO do we need to clone?
        else:
            trace_id = parent.trace_id
            if rpc_server and self.one_span_per_rpc:
                # Zipkin-style one-span-per-RPC
                span_id = parent.span_id
                parent_id = parent.parent_id
            else:
                span_id = self._random_id(constants._max_id_bits)
                parent_id = parent.span_id
            flags = parent.flags
            baggage = dict(parent.baggage)  # TODO do we need to clone?

        span_ctx = SpanContext(trace_id=trace_id, span_id=span_id,
                               parent_id=parent_id, flags=flags,
                               baggage=baggage)
        span = Span(context=span_ctx, tracer=self,
                    operation_name=operation_name or '',
                    tags=tags, start_time=start_time, references=valid_references)

        self._emit_span_metrics(span=span, join=rpc_server)

        return span

    def start_active_span(self,
                          operation_name: Optional[str] = None,
                          child_of: Union[None, Span, SpanContext] = None,
                          references: Union[None, Reference, List[Reference]] = None,
                          tags: Optional[dict] = None,
                          start_time: Optional[float] = None,
                          ignore_active_span: bool = False,
                          finish_on_close: bool = True,
                          ):
        """
        Returns a newly started and activated :class:`Scope`

        :param operation_name: name of the operation represented by the new
            span from the perspective of the current service.
        :param child_of: shortcut for 'child_of' reference
        :param references: (optional) either a single Reference object or a
            list of Reference objects that identify one or more parent
            SpanContexts. (See the Reference documentation for detail)
        :param tags: optional dictionary of Span Tags. The caller gives up
            ownership of that dictionary, because the Tracer may use it as-is
            to avoid extra data copying.
        :param start_time: an explicit Span start time as a unix timestamp per
            time.time()
        :param ignore_active_span: an explicit flag that ignores the current
            active :class:`Scope` and creates a root :class:`Span`
        :param finish_on_close: whether :class:`Span` should automatically be
            finished when :meth:`Scope.close()` is called.

        :return: a :class:`Scope`, already registered via the :class:`ScopeManager`.
        """
        span = self.start_span(
            operation_name=operation_name,
            child_of=child_of,
            references=references,
            tags=tags,
            start_time=start_time,
            ignore_active_span=ignore_active_span,
        )
        return self.scope_manager.activate(span, finish_on_close)

    def inject(self, span_context: Union[Span, SpanContext], format: str, carrier: dict) -> None:
        codec = self.codecs.get(format, None)
        if codec is None:
            raise UnsupportedFormatException(format)
        if isinstance(span_context, Span):
            # be flexible and allow Span as argument, not only SpanContext
            span_context = span_context.context
        if not isinstance(span_context, SpanContext):
            raise ValueError(
                'Expecting Jaeger SpanContext, not %s', type(span_context))
        codec.inject(span_context=span_context, carrier=carrier)

    def extract(self, format: str, carrier: dict) -> SpanContext:
        codec = self.codecs.get(format, None)
        if codec is None:
            raise UnsupportedFormatException(format)
        return codec.extract(carrier)

    def close(self) -> Future:
        """
        Perform a clean shutdown of the tracer, flushing any traces that
        may be buffered in memory.

        :return: Returns a tornado.concurrent.Future that indicates if the
            flush has been completed.
        """
        self.sampler.close()
        return self.reporter.close()

    def _emit_span_metrics(self, span: Span, join: Optional[bool] = False) -> Span:
        if span.is_sampled():
            self.metrics.spans_started_sampled(1)
        else:
            self.metrics.spans_started_not_sampled(1)
        if not span.context.parent_id:
            if span.is_sampled():
                if join:
                    self.metrics.traces_joined_sampled(1)
                else:
                    self.metrics.traces_started_sampled(1)
            else:
                if join:
                    self.metrics.traces_joined_not_sampled(1)
                else:
                    self.metrics.traces_started_not_sampled(1)
        return span

    def report_span(self, span: Span) -> None:
        self.reporter.report_span(span)
        self.metrics.spans_finished(1)

    def random_id(self) -> int:
        """
        DEPRECATED: use _random_id() instead
        """
        return self.random.getrandbits(constants.MAX_ID_BITS)

    def _random_id(self, bitsize: int) -> int:
        return self.random.getrandbits(bitsize)

    def is_debug_allowed(self, *args: Any, **kwargs: Any) -> bool:
        if not self.throttler:
            return True
        return self.throttler.is_allowed(*args, **kwargs)


class TracerMetrics(object):
    """Tracer specific metrics."""

    def __init__(self, metrics_factory: MetricsFactory) -> None:
        self.traces_started_sampled = \
            metrics_factory.create_counter(name='jaeger:traces',
                                           tags={'state': 'started', 'sampled': 'y'})
        self.traces_started_not_sampled = \
            metrics_factory.create_counter(name='jaeger:traces',
                                           tags={'state': 'started', 'sampled': 'n'})
        self.traces_joined_sampled = \
            metrics_factory.create_counter(name='jaeger:traces',
                                           tags={'state': 'joined', 'sampled': 'y'})
        self.traces_joined_not_sampled = \
            metrics_factory.create_counter(name='jaeger:traces',
                                           tags={'state': 'joined', 'sampled': 'n'})
        self.spans_started_sampled = \
            metrics_factory.create_counter(name='jaeger:started_spans', tags={'sampled': 'y'})
        self.spans_started_not_sampled = \
            metrics_factory.create_counter(name='jaeger:started_spans', tags={'sampled': 'n'})
        self.spans_finished = \
            metrics_factory.create_counter(name='jaeger:finished_spans')
