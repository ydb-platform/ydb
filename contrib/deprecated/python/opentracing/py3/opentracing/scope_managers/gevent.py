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

import gevent

from opentracing import Scope, ScopeManager
from .constants import ACTIVE_ATTR


class GeventScopeManager(ScopeManager):
    """
    :class:`~opentracing.ScopeManager` implementation for **gevent**
    that stores the :class:`~opentracing.Scope` in the current greenlet
    (:func:`gevent.getcurrent()`).

    Automatic :class:`~opentracing.Span` propagation from parent greenlets to
    their children is not provided, which needs to be
    done manually:

    .. code-block:: python

        def child_greenlet(span):
            # activate the parent Span, but do not finish it upon
            # deactivation. That will be done by the parent greenlet.
            with tracer.scope_manager.activate(span, finish_on_close=False):
                with tracer.start_active_span('child') as scope:
                    ...

        def parent_greenlet():
            with tracer.start_active_span('parent') as scope:
                ...
                gevent.spawn(child_greenlet, span).join()
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

        scope = _GeventScope(self, span, finish_on_close)
        self._set_greenlet_scope(scope)

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

        return self._get_greenlet_scope()

    def _get_greenlet_scope(self, greenlet=None):
        if greenlet is None:
            greenlet = gevent.getcurrent()

        return getattr(greenlet, ACTIVE_ATTR, None)

    def _set_greenlet_scope(self, scope, greenlet=None):
        if greenlet is None:
            greenlet = gevent.getcurrent()

        setattr(greenlet, ACTIVE_ATTR, scope)


class _GeventScope(Scope):
    def __init__(self, manager, span, finish_on_close):
        super(_GeventScope, self).__init__(manager, span)
        self._finish_on_close = finish_on_close
        self._to_restore = manager.active

    def close(self):
        if self.manager.active is not self:
            return

        self.manager._set_greenlet_scope(self._to_restore)

        if self._finish_on_close:
            self.span.finish()
