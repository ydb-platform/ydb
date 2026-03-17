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

import threading

from opentracing import Scope, ScopeManager


class ThreadLocalScopeManager(ScopeManager):
    """
    :class:`~opentracing.ScopeManager` implementation that stores the
    current active :class:`~opentracing.Scope` using thread-local storage.
    """
    def __init__(self):
        self._tls_scope = threading.local()

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
        scope = _ThreadLocalScope(self, span, finish_on_close)
        setattr(self._tls_scope, 'active', scope)
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
        return getattr(self._tls_scope, 'active', None)


class _ThreadLocalScope(Scope):
    def __init__(self, manager, span, finish_on_close):
        super(_ThreadLocalScope, self).__init__(manager, span)
        self._finish_on_close = finish_on_close
        self._to_restore = manager.active

    def close(self):
        if self.manager.active is not self:
            return

        if self._finish_on_close:
            self.span.finish()

        setattr(self._manager._tls_scope, 'active', self._to_restore)
