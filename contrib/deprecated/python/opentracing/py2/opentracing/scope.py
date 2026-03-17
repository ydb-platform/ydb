# Copyright (c) 2017-2019 The OpenTracing Authors.
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

from .span import Span


class Scope(object):
    """A scope formalizes the activation and deactivation of a :class:`Span`,
    usually from a CPU standpoint. Many times a :class:`Span` will be extant
    (in that :meth:`Span.finish()` has not been called) despite being in a
    non-runnable state from a CPU/scheduler standpoint. For instance, a
    :class:`Span` representing the client side of an RPC will be unfinished but
    blocked on IO while the RPC is still outstanding. A scope defines when a
    given :class:`Span` is scheduled and on the path.

    :param manager: the :class:`ScopeManager` that created this :class:`Scope`.
    :type manager: ScopeManager

    :param span: the :class:`Span` used for this :class:`Scope`.
    :type span: Span
    """
    def __init__(self, manager, span):
        """Initializes a scope for *span*."""
        self._manager = manager
        self._span = span

    @property
    def span(self):
        """Returns the :class:`Span` wrapped by this :class:`Scope`.

        :rtype: Span
        """
        return self._span

    @property
    def manager(self):
        """Returns the :class:`ScopeManager` that created this :class:`Scope`.

        :rtype: ScopeManager
        """
        return self._manager

    def close(self):
        """Marks the end of the active period for this :class:`Scope`, updating
        :attr:`ScopeManager.active` in the process.

        NOTE: Calling this method more than once on a single :class:`Scope`
        leads to undefined behavior.
        """
        pass

    def __enter__(self):
        """Allows :class:`Scope` to be used inside a Python Context Manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Calls :meth:`close()` when the execution is outside the Python
        Context Manager.

        If exception has occurred during execution, it is automatically logged
        and added as a tag to the :class:`Span`.
        :attr:`~operation.ext.tags.ERROR` will also be set to `True`.
        """
        Span._on_error(self.span, exc_type, exc_val, exc_tb)
        self.close()
