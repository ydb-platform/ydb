# Copyright (c) 2017 The OpenTracing Authors.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from __future__ import absolute_import

import mock
import types

from opentracing.scope_manager import ScopeManager
from opentracing.tracer import Tracer
from opentracing.scope import Scope
from opentracing.span import Span, SpanContext
from opentracing import tags
from opentracing import logs


def test_scope_wrapper():
    # ensure `Scope` wraps the `Span` argument
    span = Span(tracer=Tracer(), context=SpanContext())
    scope = Scope(ScopeManager, span)
    assert scope.span == span


def test_scope_context_manager():
    # ensure `Scope` can be used in a Context Manager that
    # calls the `close()` method
    span = Span(tracer=Tracer(), context=SpanContext())
    scope = Scope(ScopeManager(), span)
    with mock.patch.object(scope, 'close') as close:
        with scope:
            pass
        assert close.call_count == 1


def test_scope_error_report():
    tracer = Tracer()
    scope = tracer.start_active_span('foo')
    error_message = 'unexpected_situation'

    with mock.patch.object(scope.span, 'log_kv') as log_kv:
        with mock.patch.object(scope.span, 'set_tag') as set_tag:
            try:
                with scope:
                    raise ValueError(error_message)
            except ValueError:
                pass

            assert set_tag.call_count == 1
            assert set_tag.call_args[0] == (tags.ERROR, True)

            assert log_kv.call_count == 1
            log_kv_args = log_kv.call_args[0][0]
            assert log_kv_args.get(logs.EVENT, None) is tags.ERROR
            assert log_kv_args.get(logs.MESSAGE, None) is error_message
            assert log_kv_args.get(logs.ERROR_KIND, None) is ValueError
            assert isinstance(log_kv_args.get(logs.ERROR_OBJECT, None),
                              ValueError)
            assert isinstance(log_kv_args.get(logs.STACK, None),
                              types.TracebackType)


def test_scope_exit_with_no_span():
    # ensure `Scope.__exit__` doesn't fail with `AttributeError`
    try:
        with Scope(None, None):
            raise ValueError
    except ValueError:
        pass
