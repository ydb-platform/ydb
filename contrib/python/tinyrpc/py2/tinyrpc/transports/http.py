#!/usr/bin/env python
# -*- coding: utf-8 -*-

import six
import requests
try:
    import geventwebsocket as websocket
    WEBSOCKET = True
except:
    WEBSOCKET = False

from . import ClientTransport


class HttpPostClientTransport(ClientTransport):
    """HTTP POST based client transport.

    Requires :py:mod:`requests`. Submits messages to a server using the body of
    an ``HTTP`` ``POST`` request. Replies are taken from the responses body.

    :param endpoint: The URL to send ``POST`` data to.
    :param post_method: allows to replace `requests.post` with another method,
        e.g. the post method of a `requests.Session()` instance.
    :param kwargs: Additional parameters for :py:func:`requests.post`.
    """
    def __init__(self, endpoint, post_method=None, **kwargs):
        self.endpoint = endpoint
        self.request_kwargs = kwargs
        if post_method is None:
            self.post = requests.post
        else:
            self.post = post_method

    def send_message(self, message, expect_reply=True):
        if not isinstance(message, six.binary_type):
            raise TypeError('bytes expected')

        r = self.post(self.endpoint, data=message, **self.request_kwargs)

        if expect_reply:
            return r.content

if WEBSOCKET:
    class HttpWebSocketClientTransport(ClientTransport):
        """HTTP WebSocket based client transport.

        Requires :py:mod:`websocket-python`. Submits messages to a server using the body of
        an ``HTTP`` ``WebSocket`` message. Replies are taken from the response of the websocket.

        The connection is establish on the ``__init__`` because the protocol is connection oriented,
        you need to close the connection calling the close method.

        :param endpoint: The URL to connect the websocket.
        :param kwargs: Additional parameters for :py:func:`websocket.send`.
        """
        def __init__(self, endpoint, **kwargs):
            self.endpoint = endpoint
            self.request_kwargs = kwargs
            self.ws = websocket.create_connection(self.endpoint, **kwargs)

        def send_message(self, message, expect_reply=True):
            if not isinstance(message, six.binary_type):
                raise TypeError('str expected')
            self.ws.send(message)
            r = self.ws.recv()
            if expect_reply:
                return r

        def close(self):
            if self.ws is not None:
                self.ws.close()
else:
    class HttpWebSocketClientTransport(ClientTransport):
        def __init__(self, endpoint, **kwargs):
            raise TypeError('HttpWebSocketClientTransport depends on module '
                'geventwebsocket; "pip install geventwebsocket" to use this class')
