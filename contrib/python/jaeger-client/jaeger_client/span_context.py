# Copyright (c) 2016 Uber Technologies, Inc.
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


import opentracing
from typing import Dict, Optional


class SpanContext(opentracing.SpanContext):
    __slots__ = ['trace_id', 'span_id', 'parent_id', 'flags',
                 '_baggage', '_debug_id']

    """Implements opentracing.SpanContext"""
    def __init__(
        self,
        trace_id: int,
        span_id: int,
        parent_id: Optional[int],
        flags: int,
        baggage: Optional[Dict[str, str]] = None,
        debug_id: Optional[str] = None
    ) -> None:
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_id = parent_id or None
        self.flags = flags
        self._baggage = baggage or opentracing.SpanContext.EMPTY_BAGGAGE
        self._debug_id = debug_id

    @property
    def baggage(self) -> Dict[str, str]:
        return self._baggage or opentracing.SpanContext.EMPTY_BAGGAGE

    def with_baggage_item(self, key: str, value: Optional[str]) -> 'SpanContext':
        baggage = dict(self._baggage)
        if value is not None:
            baggage[key] = value
        else:
            baggage.pop(key, None)
        return SpanContext(
            trace_id=self.trace_id,
            span_id=self.span_id,
            parent_id=self.parent_id,
            flags=self.flags,
            baggage=baggage,
        )

    @property
    def has_trace(self) -> bool:
        return bool(self.trace_id and self.span_id and self.flags is not None)

    @property
    def is_debug_id_container_only(self) -> bool:
        """Deprecated, not used by Jaeger."""
        return not self.trace_id and self._debug_id is not None

    @property
    def debug_id(self) -> Optional[str]:
        return self._debug_id

    @staticmethod
    def with_debug_id(debug_id):
        """Deprecated, not used by Jaeger."""
        ctx = SpanContext(
            trace_id=None, span_id=None, parent_id=None, flags=None
        )
        ctx._debug_id = debug_id
        return ctx
