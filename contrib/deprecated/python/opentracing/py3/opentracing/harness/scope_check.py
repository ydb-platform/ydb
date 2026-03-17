# Copyright (c) The OpenTracing Authors.
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

from opentracing.span import Span


class ScopeCompatibilityCheckMixin(object):
    """
    A mixin class for validation that a given scope manager implementation
    satisfies the requirements of the OpenTracing API.
    """

    def scope_manager(self):
        raise NotImplementedError('Subclass must implement scope_manager()')

    def run_test(self, test_fn):
        """
        Utility method that can be optionally defined by ScopeManager
        implementers to run the passed test_fn() function
        in a given environment, such as a coroutine or greenlet.
        By default, it simply runs the passed test_fn() function
        in the current thread.
        """

        test_fn()

    def test_missing_active_external(self):
        # test that 'active' does not fail outside the run_test()
        # implementation (greenlet or coroutine).
        scope_manager = self.scope_manager()
        assert scope_manager.active is None

    def test_missing_active(self):
        def fn():
            scope_manager = self.scope_manager()
            assert scope_manager.active is None

        self.run_test(fn)

    def test_activate(self):
        def fn():
            scope_manager = self.scope_manager()
            span = mock.MagicMock(spec=Span)

            scope = scope_manager.activate(span, False)
            assert scope is not None
            assert scope_manager.active is scope

            scope.close()
            assert span.finish.call_count == 0
            assert scope_manager.active is None

        self.run_test(fn)

    def test_activate_external(self):
        # test that activate() does not fail outside the run_test()
        # implementation (greenlet or corotuine).
        scope_manager = self.scope_manager()
        span = mock.MagicMock(spec=Span)

        scope = scope_manager.activate(span, False)
        assert scope is not None
        assert scope_manager.active is scope

        scope.close()
        assert span.finish.call_count == 0
        assert scope_manager.active is None

    def test_activate_finish_on_close(self):
        def fn():
            scope_manager = self.scope_manager()
            span = mock.MagicMock(spec=Span)

            scope = scope_manager.activate(span, True)
            assert scope is not None
            assert scope_manager.active is scope

            scope.close()
            assert span.finish.call_count == 1
            assert scope_manager.active is None

        self.run_test(fn)

    def test_activate_nested(self):
        def fn():
            # when a Scope is closed, the previous one must be re-activated.
            scope_manager = self.scope_manager()
            parent_span = mock.MagicMock(spec=Span)
            child_span = mock.MagicMock(spec=Span)

            with scope_manager.activate(parent_span, True) as parent:
                assert parent is not None
                assert scope_manager.active is parent

                with scope_manager.activate(child_span, True) as child:
                    assert child is not None
                    assert scope_manager.active is child

                assert scope_manager.active is parent

            assert parent_span.finish.call_count == 1
            assert child_span.finish.call_count == 1

            assert scope_manager.active is None

        self.run_test(fn)

    def test_activate_finish_on_close_nested(self):
        def fn():
            # finish_on_close must be correctly handled
            scope_manager = self.scope_manager()
            parent_span = mock.MagicMock(spec=Span)
            child_span = mock.MagicMock(spec=Span)

            parent = scope_manager.activate(parent_span, False)
            with scope_manager.activate(child_span, True):
                pass
            parent.close()

            assert parent_span.finish.call_count == 0
            assert child_span.finish.call_count == 1
            assert scope_manager.active is None

        self.run_test(fn)

    def test_close_wrong_order(self):
        def fn():
            # only the active `Scope` can be closed
            scope_manager = self.scope_manager()
            parent_span = mock.MagicMock(spec=Span)
            child_span = mock.MagicMock(spec=Span)

            parent = scope_manager.activate(parent_span, True)
            child = scope_manager.activate(child_span, True)
            parent.close()

            assert parent_span.finish.call_count == 0
            assert scope_manager.active == child

        self.run_test(fn)
