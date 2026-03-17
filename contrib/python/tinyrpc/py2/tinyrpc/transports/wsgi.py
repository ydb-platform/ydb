#!/usr/bin/env python
# -*- coding: utf-8 -*-

import six
from six.moves import queue as Queue

from werkzeug.wrappers import Response, Request

from . import ServerTransport


class WsgiServerTransport(ServerTransport):
    """WSGI transport.

    Requires :py:mod:`werkzeug`.

    Due to the nature of WSGI, this transport has a few pecularities: It must
    be run in a thread, greenlet or some other form of concurrent execution
    primitive.

    This is due to
    :py:func:`~tinyrpc.transports.wsgi.WsgiServerTransport.handle` blocking
    while waiting for a call to
    :py:func:`~tinyrpc.transports.wsgi.WsgiServerTransport.send_reply`.

    The parameter ``queue_class`` must be used to supply a proper queue class
    for the chosen concurrency mechanism (i.e. when using :py:mod:`gevent`,
    set it to :py:class:`gevent.queue.Queue`).

    :param max_content_length: The maximum request content size allowed. Should
                               be set to a sane value to prevent DoS-Attacks.
    :param queue_class: The Queue class to use.
    :param allow_origin: The ``Access-Control-Allow-Origin`` header. Defaults
                         to ``*`` (so change it if you need actual security).
    """
    def __init__(self, max_content_length=4096, queue_class=Queue.Queue,
                       allow_origin='*'):
        self._queue_class = queue_class
        self.messages = queue_class()
        self.max_content_length = max_content_length
        self.allow_origin = allow_origin

    def receive_message(self):
        return self.messages.get()

    def send_reply(self, context, reply):
        if not isinstance(reply, str):
            raise TypeError('str expected')
        context.put(reply)

    def handle(self, environ, start_response):
        """WSGI handler function.

        The transport will serve a request by reading the message and putting
        it into an internal buffer. It will then block until another
        concurrently running function sends a reply using
        :py:func:`~tinyrpc.transports.WsgiServerTransport.send_reply`.

        The reply will then be sent to the client being handled and handle will
        return.
        """
        request = Request(environ)
        request.max_content_length = self.max_content_length

        access_control_headers = {
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Origin': self.allow_origin,
            'Access-Control-Allow-Headers': \
                'Content-Type, X-Requested-With, Accept, Origin'
        }

        if request.method == 'OPTIONS':
            response = Response(headers=access_control_headers)

        elif request.method == 'POST':
            # message is encoded in POST, read it...
            msg = request.stream.read()

            # create new context
            context = self._queue_class()

            self.messages.put((context, msg))

            # ...and send the reply
            response = Response(context.get(), headers=access_control_headers)
        else:
            # nothing else supported at the moment
            response = Response('Only POST supported', 405)

        return response(environ, start_response)
