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
import tornado.stack_context

from opentracing import Scope
from opentracing.scope_managers import ThreadLocalScopeManager


# Implementation based on
# github.com/uber-common/opentracing-python-instrumentation/

class TornadoScopeManager(ThreadLocalScopeManager):
    """
    :class:`~opentracing.ScopeManager` implementation for **Tornado**
    that stores the :class:`~opentracing.Scope` using a custom
    :class:`StackContext`, falling back to thread-local storage if
    none was found.

    Using it under :func:`tracer_stack_context()` will
    also automatically propagate the active :class:`~opentracing.Span`
    from parent coroutines to their children:

    .. code-block:: python

        @tornado.gen.coroutine
        def child_coroutine():
            # No need to pass 'parent' and activate it here,
            # as it is automatically propagated.
            with tracer.start_active_span('child') as scope:
                ...

        @tornado.gen.coroutine
        def parent_coroutine():
            with tracer.start_active_span('parent') as scope:
                ...
                yield child_coroutine()
                ...

        with tracer_stack_context():
            loop.add_callback(parent_coroutine)


    .. note::
        The current version does not support :class:`~opentracing.Span`
        activation in children coroutines when the parent yields over
        **multiple** of them, as the context is effectively shared by all,
        and the active :class:`~opentracing.Span` state is messed up:

        .. code-block:: python

            @tornado.gen.coroutine
            def coroutine(input):
                # No span should be activated here.
                # The parent Span will remain active, though.
                with tracer.start_span('child', child_of=tracer.active_span):
                    ...

            @tornado.gen.coroutine
            def handle_request_wrapper():
                res1 = coroutine('A')
                res2 = coroutine('B')

                yield [res1, res2]
    """

    def activate(self, span, finish_on_close):
        """
        Make a :class:`~opentracing.Span` instance active.

        :param span: the :class:`~opentracing.Span` that should become active.
        :param finish_on_close: whether *span* should automatically be
            finished when :meth:`Scope.close()` is called.

        If no :func:`tracer_stack_context()` is detected, thread-local
        storage will be used to store the :class:`~opentracing.Scope`.
        Observe that in this case the active :class:`~opentracing.Span`
        will not be automatically propagated to the child corotuines.

        :return: a :class:`~opentracing.Scope` instance to control the end
            of the active period for the :class:`~opentracing.Span`.
            It is a programming error to neglect to call :meth:`Scope.close()`
            on the returned instance.
        """

        context = self._get_context()
        if context is None:
            return super(TornadoScopeManager, self).activate(span,
                                                             finish_on_close)

        scope = _TornadoScope(self, span, finish_on_close)
        context.active = scope

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

        context = self._get_context()
        if not context:
            return super(TornadoScopeManager, self).active

        return context.active

    def _get_context(self):
        return _TracerRequestContextManager.current_context()


class _TornadoScope(Scope):
    def __init__(self, manager, span, finish_on_close):
        super(_TornadoScope, self).__init__(manager, span)
        self._finish_on_close = finish_on_close
        self._to_restore = manager.active

    def close(self):
        context = self.manager._get_context()
        if context is None or context.active is not self:
            return

        context.active = self._to_restore

        if self._finish_on_close:
            self.span.finish()


class ThreadSafeStackContext(tornado.stack_context.StackContext):
    """
    Thread safe version of Tornado's StackContext (up to 4.3)
    Copy of implementation by caspersj@, until tornado-extras is open-sourced.
    Tornado's StackContext works as follows:
    - When entering a context, create an instance of StackContext and
      add add this instance to the current "context stack"
    - If execution transfers to another thread (using the wraps helper
      method),  copy the current "context stack" and apply that in the new
      thread when execution starts
    - A context stack can be entered/exited by traversing the stack and
      calling enter/exit on all elements. This is how the `wraps` helper
      method enters/exits in new threads.
    - StackContext has an internal pointer to a context factory (i.e.
      RequestContext), and an internal stack of applied contexts (instances
      of RequestContext) for each instance of StackContext. RequestContext
      instances are entered/exited from the stack as the StackContext
      is entered/exited
    - However, the enter/exit logic and maintenance of this stack of
      RequestContext instances is not thread safe.
    ```
    def __init__(self, context_factory):
        self.context_factory = context_factory
        self.contexts = []
        self.active = True
    def enter(self):
        context = self.context_factory()
        self.contexts.append(context)
        context.__enter__()
    def exit(self, type, value, traceback):
        context = self.contexts.pop()
        context.__exit__(type, value, traceback)
    ```
    Unexpected semantics of Tornado's default StackContext implementation:
    - There exist a race on `self.contexts`, where thread A enters a
      context, thread B enters a context, and thread A exits its context.
      In this case, the exit by thread A pops the instance created by
      thread B and calls exit on this instance.
    - There exists a race between `enter` and `exit` where thread A
      executes the two first statements of enter (create instance and
      add to contexts) and thread B executes exit, calling exit on an
      instance that has been initialized but not yet exited (and
      subsequently this instance will then be entered).
    The ThreadSafeStackContext changes the internal contexts stack to be
    thread local, fixing both of the above issues.
    """

    def __init__(self, *args, **kwargs):
        class LocalContexts(threading.local):
            def __init__(self):
                super(LocalContexts, self).__init__()
                self._contexts = []

            def append(self, item):
                self._contexts.append(item)

            def pop(self):
                return self._contexts.pop()

        super(ThreadSafeStackContext, self).__init__(*args, **kwargs)

        if hasattr(self, 'contexts'):
            # only patch if context exists
            self.contexts = LocalContexts()


class _TracerRequestContext(object):
    __slots__ = ('active', )

    def __init__(self, active=None):
        self.active = active


class _TracerRequestContextManager(object):
    _state = threading.local()
    _state.context = None

    @classmethod
    def current_context(cls):
        return getattr(cls._state, 'context', None)

    def __init__(self, context):
        self._context = context

    def __enter__(self):
        self._prev_context = self.__class__.current_context()
        self.__class__._state.context = self._context
        return self._context

    def __exit__(self, *_):
        self.__class__._state.context = self._prev_context
        self._prev_context = None
        return False


def tracer_stack_context():
    """
    Create a custom Tornado's :class:`StackContext` that allows
    :class:`TornadoScopeManager` to store the active
    :class:`~opentracing.Span` in the thread-local request context.

    Suppose you have a method ``handle_request(request)`` in the
    http server. Instead of calling it directly, use a wrapper:

    .. code-block:: python

        from opentracing.scope_managers.tornado import tracer_stack_context

        @tornado.gen.coroutine
        def handle_request_wrapper(request, actual_handler, *args, **kwargs)

            request_wrapper = TornadoRequestWrapper(request=request)
            span = http_server.before_request(request=request_wrapper)

            with tracer_stack_context():
                with tracer.scope_manager.activate(span, True):
                    return actual_handler(*args, **kwargs)

    :return:
        Return a custom :class:`StackContext` that allows
        :class:`TornadoScopeManager` to activate and propagate
        :class:`~opentracing.Span` instances.
    """
    context = _TracerRequestContext()
    return ThreadSafeStackContext(
            lambda: _TracerRequestContextManager(context)
    )
