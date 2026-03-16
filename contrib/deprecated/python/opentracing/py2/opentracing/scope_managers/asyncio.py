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

import asyncio

from opentracing import Scope
from opentracing.scope_managers import ThreadLocalScopeManager
from .constants import ACTIVE_ATTR


class AsyncioScopeManager(ThreadLocalScopeManager):
    """
    :class:`~opentracing.ScopeManager` implementation for **asyncio**
    that stores the :class:`~opentracing.Scope` in the current
    :class:`Task` (:meth:`asyncio.current_task()`), falling back to
    thread-local storage if none was being executed.

    Automatic :class:`~opentracing.Span` propagation from
    parent coroutines to their children is not provided, which needs to be
    done manually:

    .. code-block:: python

        async def child_coroutine(span):
            # activate the parent Span, but do not finish it upon
            # deactivation. That will be done by the parent coroutine.
            with tracer.scope_manager.activate(span, finish_on_close=False):
                with tracer.start_active_span('child') as scope:
                    ...

        async def parent_coroutine():
            with tracer.start_active_span('parent') as scope:
                ...
                await child_coroutine(span)
                ...

    """

    def activate(self, span, finish_on_close):
        """
        Make a :class:`~opentracing.Span` instance active.

        :param span: the :class:`~opentracing.Span` that should become active.
        :param finish_on_close: whether *span* should automatically be
            finished when :meth:`Scope.close()` is called.

        If no :class:`Task` is being executed, thread-local
        storage will be used to store the :class:`~opentracing.Scope`.

        :return: a :class:`~opentracing.Scope` instance to control the end
            of the active period for the :class:`~opentracing.Span`.
            It is a programming error to neglect to call :meth:`Scope.close()`
            on the returned instance.
        """

        task = self._get_task()
        if not task:
            return super(AsyncioScopeManager, self).activate(span,
                                                             finish_on_close)

        scope = _AsyncioScope(self, span, finish_on_close)
        self._set_task_scope(scope, task)

        return scope

    @property
    def active(self):
        """
        Return the currently active :class:`~opentracing.Scope` which
        can be used to access the currently active
        :attr:`Scope.span`.

        :return: the :class:`~opentracing.Scope` that is active,
            or ``None`` if not available.
        """

        task = self._get_task()
        if not task:
            return super(AsyncioScopeManager, self).active

        return self._get_task_scope(task)

    def _get_task(self):
        try:
            # Prevent failure when run from a thread
            # without an event loop.
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return None
        if hasattr(asyncio, 'current_task'):
            # Python 3.7+
            return asyncio.current_task(loop=loop)
        else:
            # Python 3.6 and below
            return asyncio.Task.current_task(loop=loop)

    def _set_task_scope(self, scope, task=None):
        if task is None:
            task = self._get_task()

        setattr(task, ACTIVE_ATTR, scope)

    def _get_task_scope(self, task=None):
        if task is None:
            task = self._get_task()

        return getattr(task, ACTIVE_ATTR, None)


class _AsyncioScope(Scope):
    def __init__(self, manager, span, finish_on_close):
        super(_AsyncioScope, self).__init__(manager, span)
        self._finish_on_close = finish_on_close
        self._to_restore = manager.active

    def close(self):
        if self.manager.active is not self:
            return

        self.manager._set_task_scope(self._to_restore)

        if self._finish_on_close:
            self.span.finish()
