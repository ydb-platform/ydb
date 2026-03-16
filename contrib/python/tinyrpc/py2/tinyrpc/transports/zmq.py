#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import  # needed for zmq import
import six
import zmq

from . import ServerTransport, ClientTransport


class ZmqServerTransport(ServerTransport):
    """Server transport based on a :py:const:`zmq.ROUTER` socket.

    :param socket: A :py:const:`zmq.ROUTER` socket instance, bound to an
                   endpoint.
    """

    def __init__(self, socket):
        self.socket = socket

    def receive_message(self):
        msg = self.socket.recv_multipart()
        return msg[:-1], msg[-1]

    def send_reply(self, context, reply):
        if six.PY3 and isinstance(reply, six.string_types):
            # zmq won't accept unicode strings
            reply = reply.encode()

        self.socket.send_multipart(context + [reply])

    @classmethod
    def create(cls, zmq_context, endpoint):
        """Create new server transport.

        Instead of creating the socket yourself, you can call this function and
        merely pass the :py:class:`zmq.core.context.Context` instance.

        By passing a context imported from :py:mod:`zmq.green`, you can use
        green (gevent) 0mq sockets as well.

        :param zmq_context: A 0mq context.
        :param endpoint: The endpoint clients will connect to.
        """
        socket = zmq_context.socket(zmq.ROUTER)
        socket.bind(endpoint)
        return cls(socket)


class ZmqClientTransport(ClientTransport):
    """Client transport based on a :py:const:`zmq.REQ` socket.

    :param socket: A :py:const:`zmq.REQ` socket instance, connected to the
                   server socket.
    """

    def __init__(self, socket):
        self.socket = socket

    def send_message(self, message, expect_reply=True):
        if six.PY3 and isinstance(message, six.string_types):
            # pyzmq won't accept unicode strings
            message = message.encode()

        self.socket.send(message)

        if expect_reply:
            return self.socket.recv()

    @classmethod
    def create(cls, zmq_context, endpoint):
        """Create new client transport.

        Instead of creating the socket yourself, you can call this function and
        merely pass the :py:class:`zmq.core.context.Context` instance.

        By passing a context imported from :py:mod:`zmq.green`, you can use
        green (gevent) 0mq sockets as well.

        :param zmq_context: A 0mq context.
        :param endpoint: The endpoint the server is bound to.
        """
        socket = zmq_context.socket(zmq.REQ)
        socket.connect(endpoint)
        return cls(socket)
