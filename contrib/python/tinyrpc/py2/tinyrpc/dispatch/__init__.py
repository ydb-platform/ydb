#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect

import six

from ..exc import *


def public(name=None):
    """Set RPC name on function.

    This function decorator will set the ``_rpc_public_name`` attribute on a
    function, causing it to be picked up if an instance of its parent class is
    registered using
    :py:func:`~tinyrpc.dispatch.RPCDispatcher.register_instance`.

    ``@public`` is a shortcut for ``@public()``.

    :param name: The name to register the function with.
    """
    # called directly with function
    if callable(name):
        f = name
        f._rpc_public_name = f.__name__
        return f

    def _(f):
        f._rpc_public_name = name or f.__name__
        return f

    return _


class RPCDispatcher(object):
    """Stores name-to-method mappings."""

    def __init__(self):
        self.method_map = {}
        self.subdispatchers = {}

    def add_subdispatch(self, dispatcher, prefix=''):
        """Adds a subdispatcher, possibly in its own namespace.

        :param dispatcher: The dispatcher to add as a subdispatcher.
        :param prefix: A prefix. All of the new subdispatchers methods will be
                       available as prefix + their original name.
        """
        self.subdispatchers.setdefault(prefix, []).append(dispatcher)

    def add_method(self, f, name=None):
        """Add a method to the dispatcher.

        :param f: Callable to be added.
        :param name: Name to register it with. If ``None``, ``f.__name__`` will
                     be used.
        """
        assert callable(f), "method argument must be callable"
        # catches a few programming errors that are
        # commonly silently swallowed otherwise
        if not name:
            name = f.__name__

        if name in self.method_map:
            raise RPCError('Name %s already registered')

        self.method_map[name] = f

    def dispatch(self, request, caller=None):
        """Fully handle request.

        The dispatch method determines which method to call, calls it and
        returns a response containing a result.

        No exceptions will be thrown, rather, every exception will be turned
        into a response using :py:func:`~tinyrpc.RPCRequest.error_respond`.

        If a method isn't found, a :py:exc:`~tinyrpc.exc.MethodNotFoundError`
        response will be returned. If any error occurs outside of the requested
        method, a :py:exc:`~tinyrpc.exc.ServerError` without any error
        information will be returend.

        If the method is found and called but throws an exception, the
        exception thrown is used as a response instead. This is the only case
        in which information from the exception is possibly propagated back to
        the client, as the exception is part of the requested method.

        :py:class:`~tinyrpc.RPCBatchRequest` instances are handled by handling
        all its children in order and collecting the results, then returning an
        :py:class:`~tinyrpc.RPCBatchResponse` with the results.

        To allow for custom processing around calling the method (i.e. custom
        error handling), the optional parameter ``caller`` may be provided with
        a callable. When present invoking the method is deferred to this callable.

        :param request: An :py:func:`~tinyrpc.RPCRequest`.
        :param caller: An optional callable used to invoke the method.
        :return: An :py:func:`~tinyrpc.RPCResponse`.
        """
        if hasattr(request, 'create_batch_response'):
            results = [self._dispatch(req, caller) for req in request]

            response = request.create_batch_response()
            if response is not None:
                response.extend(results)

            return response
        else:
            return self._dispatch(request, caller)

    def _dispatch(self, request, caller):
        try:
            method = self.get_method(request.method)
        except MethodNotFoundError as e:
            return request.error_respond(e)
        except Exception:
            # unexpected error, do not let client know what happened
            return request.error_respond(ServerError())

        # we found the method
        try:
            if caller is not None:
                result = caller(method, request.args, request.kwargs)
            else:
                result = method(*request.args, **request.kwargs)
        except Exception as e:
            # an error occured within the method, return it
            return request.error_respond(e)

        # respond with result
        return request.respond(result)

    def get_method(self, name):
        """Retrieve a previously registered method.

        Checks if a method matching ``name`` has been registered.

        If :py:func:`get_method` cannot find a method, every subdispatcher
        with a prefix matching the method name is checked as well.

        Throw a :py:class:`tinyrpc.MethodNotFoundError` if method not found

        :param name: Callable to find.
        :param return: The callable.
        """
        if name in self.method_map:
            return self.method_map[name]

        for prefix, subdispatchers in six.iteritems(self.subdispatchers):
            if name.startswith(prefix):
                for sd in subdispatchers:
                    try:
                        return sd.get_method(name[len(prefix):])
                    except MethodNotFoundError:
                        pass

        raise MethodNotFoundError(name)

    def public(self, name=None):
        """Convenient decorator.

        Allows easy registering of functions to this dispatcher. Example:

        .. code-block:: python

            dispatch = RPCDispatcher()

            @dispatch.public
            def foo(bar):
                # ...

            class Baz(object):
                def not_exposed(self):
                    # ...

                @dispatch.public(name='do_something')
                def visible_method(arg1)
                    # ...

        :param name: Name to register callable with
        """
        if callable(name):
            self.add_method(name)
            return name

        def _(f):
            self.add_method(f, name=name)
            return f

        return _

    def register_instance(self, obj, prefix=''):
        """Create new subdispatcher and register all public object methods on
        it.

        To be used in conjunction with the :py:func:`tinyrpc.dispatch.public`
        decorator (*not* :py:func:`tinyrpc.dispatch.RPCDispatcher.public`).

        :param obj: The object whose public methods should be made available.
        :param prefix: A prefix for the new subdispatcher.
        """
        dispatch = self.__class__()
        for name, f in inspect.getmembers(
                obj, lambda f: callable(f) and hasattr(f, '_rpc_public_name')
        ):
            dispatch.add_method(f, f._rpc_public_name)

        # add to dispatchers
        self.add_subdispatch(dispatch, prefix)
