# Copyright The OpenTracing Authors
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
from opentracing import child_of
from opentracing import Tracer


def test_tracer():
    tracer = Tracer()
    span = tracer.start_span(operation_name='root')
    child = tracer.start_span(operation_name='child',
                              references=child_of(span))
    assert span == child


def test_tracer_active_span():
    tracer = Tracer()
    assert tracer.active_span is tracer.scope_manager.active.span
