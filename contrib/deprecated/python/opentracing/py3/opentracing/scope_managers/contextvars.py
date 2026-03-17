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

from contextlib import contextmanager
from contextvars import ContextVar

from opentracing import Scope, ScopeManager


_SCOPE = ContextVar('scope')


class ContextVarsScopeManager(ScopeManager):
    """
    :class:`~opentracing.ScopeManager` implementation for **asyncio**
    that stores the :class:`~opentracing.Scope` using ContextVar.

    The scope manager provides automatic :class:`~opentracing.Span` propagation
    from parent coroutines, tasks and scheduled in event loop callbacks to
    their children.

    .. code-block:: python

        async def child_coroutine():
            # No need manual activation of parent span in child coroutine.
            with tracer.start_active_span('child') as scope:
                ...

        async def parent_coroutine():
            with tracer.start_active_span('parent') as scope:
                ...
                await child_coroutine()
                ...

    """

    def activate(self, span, finish_on_close):
        """
        Make a :class:`~opentracing.Span` instance active.

        :param span: the :class:`~opentracing.Span` that should become active.
        :param finish_on_close: whether *span* should automatically be
            finished when :meth:`Scope.close()` is called.

        :return: a :class:`~opentracing.Scope` instance to control the end
            of the active period for the :class:`~opentracing.Span`.
            It is a programming error to neglect to call :meth:`Scope.close()`
            on the returned instance.
        """

        return self._set_scope(span, finish_on_close)

    @property
    def active(self):
        """
        Return the currently active :class:`~opentracing.Scope` which
        can be used to access the currently active :attr:`Scope.span`.

        :return: the :class:`~opentracing.Scope` that is active,
            or ``None`` if not available.
        """

        return self._get_scope()

    def _set_scope(self, span, finish_on_close):
        return _ContextVarsScope(self, span, finish_on_close)

    def _get_scope(self):
        return _SCOPE.get(None)


class _ContextVarsScope(Scope):
    def __init__(self, manager, span, finish_on_close):
        super(_ContextVarsScope, self).__init__(manager, span)
        self._finish_on_close = finish_on_close
        self._token = _SCOPE.set(self)

    def close(self):
        if self.manager.active is not self:
            return

        _SCOPE.reset(self._token)

        if self._finish_on_close:
            self.span.finish()


@contextmanager
def no_parent_scope():
    """
    Context manager that resets current Scope. Intended to break span
    propagation to children coroutines, tasks or scheduled callbacks.

    .. code-block:: python

        from opentracing.scope_managers.contextvars import no_parent_scope

        def periodic()
            # `periodic` span will be children of root only at the first time.
            with self.tracer.start_active_span('periodic'):
                # Now we break span propagation.
                with no_parent_scope():
                    self.loop.call_soon(periodic)

        with self.tracer.start_active_span('root'):
            self.loop.call_soon(periodic)
    """
    token = _SCOPE.set(None)
    try:
        yield
    finally:
        _SCOPE.reset(token)
