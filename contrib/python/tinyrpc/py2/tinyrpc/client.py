#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from collections import namedtuple

from .exc import RPCError
from .protocols import RPCErrorResponse

RPCCall = namedtuple('RPCCall', 'method args kwargs')
"""Defines the elements of a RPC call.

RPCCall is used with :py:meth:`~tinyrpc.client.RPCClient.call_all`
to provide the list of requests to be processed. Each request contains the
elements defined in this tuple.
"""

RPCCallTo = namedtuple('RPCCallTo', 'transport method args kwargs')
"""Defines the elements of a RPC call directed to multiple transports.

RPCCallTo is used with :py:meth:`~tinyrpc.client.RPCClient.call_all`
to provide the list of requests to be processed.
"""


class RPCClient(object):
    """Client for making RPC calls to connected servers.

    :param protocol: An :py:class:`~tinyrpc.RPCProtocol` instance.
    :param transport: A :py:class:`~tinyrpc.transports.ClientTransport`
                      instance.
    """

    def __init__(self, protocol, transport):
        self.protocol = protocol
        self.transport = transport

    def _send_and_handle_reply(self, req, one_way, transport=None, no_exception=False):
        tport = self.transport if transport is None else transport

        # sends ...
        reply = tport.send_message(req.serialize().encode('utf-8'))

        if one_way:
            # ... and be done
            return

        # ... or process the reply
        response = self.protocol.parse_reply(reply)

        if not no_exception and isinstance(response, RPCErrorResponse):
            if hasattr(self.protocol, 'raise_error') and callable(
                    self.protocol.raise_error):
                response = self.protocol.raise_error(response)
            else:
                raise RPCError(
                    'Error calling remote procedure: %s' % response.error
                )

        return response

    def call(self, method, args, kwargs, one_way=False):
        """Calls the requested method and returns the result.

        If an error occured, an :py:class:`~tinyrpc.exc.RPCError` instance
        is raised.

        :param method: Name of the method to call.
        :param args: Arguments to pass to the method.
        :param kwargs: Keyword arguments to pass to the method.
        :param one_way: Whether or not a reply is desired.
        """
        req = self.protocol.create_request(method, args, kwargs, one_way)

        rep = self._send_and_handle_reply(req, one_way)

        if one_way:
            return

        return rep.result

    def call_all(self, requests):
        """Calls the methods in the request in parallel.

        When the :py:mod:`gevent` module is already loaded it is assumed to be
        correctly initialized, including monkey patching if necessary.
        In that case the RPC calls defined by ``requests`` is performed in
        parallel otherwise the methods are called sequentially.

        :param requests: A listof either RPCCall or RPCCallTo elements.
                         When RPCCallTo is used each element defines a transport.
                         Otherwise the default transport set when RPCClient is
                         created is used.
        :return: A list with replies matching the order of the requests.
        """
        threads = []

        if 'gevent' in sys.modules:
            # assume that gevent is available and functional, make calls in parallel
            import gevent
            for r in requests:
                req = self.protocol.create_request(r.method, r.args, r.kwargs)
                tr = r.transport.transport if len(r) == 4 else None
                threads.append(gevent.spawn(self._send_and_handle_reply, req, False, tr, True))
            gevent.joinall(threads)
            return [t.value for t in threads]
        else:
            # call serially
            for r in requests:
                req = self.protocol.create_request(r.method, r.args, r.kwargs)
                tr = r.transport.transport if len(r) == 4 else None
                threads.append(self._send_and_handle_reply(req, False, tr, True))
            return threads

    def get_proxy(self, prefix='', one_way=False):
        """Convenience method for creating a proxy.

        :param prefix: Passed on to :py:class:`~tinyrpc.client.RPCProxy`.
        :param one_way: Passed on to :py:class:`~tinyrpc.client.RPCProxy`.
        :return: :py:class:`~tinyrpc.client.RPCProxy` instance."""
        return RPCProxy(self, prefix, one_way)

    def batch_call(self, calls):
        """Experimental, use at your own peril."""
        req = self.protocol.create_batch_request()

        for call_args in calls:
            req.append(self.protocol.create_request(*call_args))

        return self._send_and_handle_reply(req)


class RPCProxy(object):
    """Create a new remote proxy object.

    Proxies allow calling of methods through a simpler interface. See the
    documentation for an example.

    :param client: An :py:class:`~tinyrpc.client.RPCClient` instance.
    :param prefix: Prefix to prepend to every method name.
    :param one_way: Passed to every call of
                    :py:func:`~tinyrpc.client.call`.
    """

    def __init__(self, client, prefix='', one_way=False):
        self.client = client
        self.prefix = prefix
        self.one_way = one_way

    def __getattr__(self, name):
        """Returns a proxy function that, when called, will call a function
        name ``name`` on the client associated with the proxy.
        """
        proxy_func = lambda *args, **kwargs: self.client.call(
                         self.prefix + name,
                         args,
                         kwargs,
                         one_way=self.one_way
                     )
        return proxy_func
