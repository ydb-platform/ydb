#!/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Any, Tuple


class ServerTransport(object):
    """Abstract base class for all server transports.

    The server side implementation of the transport component.
    Requests and replies encoded by the protocol component are
    exchanged between client and server using the :py:class:`ServerTransport`
    and :py:class:`ClientTransport` classes.
    """
    def receive_message(self) -> Tuple[Any, bytes]:
        """Receive a message from the transport.

        Blocks until a message has been received.
        May return an opaque context object to its caller that should be passed on to
        :py:func:`~tinyrpc.transport.ServerTransport.send_reply` to identify
        the transport or requester later on.
        Use and function of the context object are entirely controlled by the transport
        instance.

        The message must be treated as a binary entity as only the protocol
        level will know how to interpret the message.

        If the transport encodes the message in some way, the opposite end
        is responsible for decoding it before it is passed to either client
        or server.

        :return: A tuple consisting of ``(context, message)``.
            Where ``context`` can be any valid Python type and
            ``message`` must be a :py:class:`bytes` object.
        """
        raise NotImplementedError()

    def send_reply(self, context: Any, reply: bytes) -> None:
        """Sends a reply to a client.

        The client is usually identified by passing ``context`` as returned
        from the original :py:meth:`receive_message` call.

        The reply must be a bytes object since only the protocol
        level will know how to construct the reply.

        :param any context: A context returned by :py:meth:`receive_message`.
        :param bytes reply: The reply to return to the client.
        """
        raise NotImplementedError


class ClientTransport(object):
    """Abstract base class for all client transports.

    The client side implementation of the transport component.
    Requests and replies encoded by the protocol component are
    exchanged between client and server using the :py:class:`ServerTransport`
    and :py:class:`ClientTransport` classes.    """
    def send_message(self, message: bytes, expect_reply: bool = True) -> bytes:
        """Send a message to the server and possibly receive a reply.

        Sends a message to the connected server.

        The message must be treated as a binary entity as only the protocol
        level will know how to interpret the message.

        If the transport encodes the message in some way, the opposite end
        is responsible for decoding it before it is passed to either client
        or server.

        This function will block until the reply has been received.

        :param bytes message: The request to send to the server.
        :param bool expect_reply: Some protocols allow notifications for which a
            reply is not expected. When this flag is ``False`` the transport may
            not wait for a response from the server.
            **Note** that it is still the responsibility of the transport layer how
            to implement this. It is still possible that the server sends some form
            of reply regardless the value of this flag.
        :return: The servers reply to the request.
        :rtype: bytes
        """
        raise NotImplementedError
