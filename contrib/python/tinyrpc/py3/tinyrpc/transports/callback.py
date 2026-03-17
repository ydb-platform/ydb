#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Callable, Tuple, Any

from . import ServerTransport


class CallbackServerTransport(ServerTransport):
    """Callback server transport.

    The :py:class:`CallbackServerTransport` uses the provided callbacks to implement
    communication with the counterpart.

    Used when tinyrpc is part of a system where it cannot directly attach
    to a socket or stream. The methods :py:meth:`receive_message` and
    :py:meth:`send_reply` are implemented by callback functions that are
    set when constructed.

    :param callable reader: Called when the transport wants to receive a new request.

        :returns: The RPC request.
        :rtype: bytes

    :param callable writer(reply): Called to return the response to the client.

        :param bytes reply: The response to the request.
    """
    def __init__(
            self, reader: Callable[[], bytes], writer: Callable[[bytes], None]
    ) -> None:
        super(CallbackServerTransport, self).__init__()
        self.reader = reader
        self.writer = writer

    def receive_message(self) -> Tuple[Any, bytes]:
        """Receive a message from the transport.

        Uses the callback function :py:attr:`reader` to obtain a :py:class:`bytes` ``message``.
        May return a context opaque to clients that should be passed on to
        :py:meth:`send_reply` to identify the client later on.

        :return: A tuple consisting of ``(context, message)``.
        """
        return None, self.reader()

    def send_reply(self, context: Any, reply: bytes):
        """Sends a reply to a client.

        The client is usually identified by passing ``context`` as returned
        from the original :py:meth:`receive_message` call.

        Uses the callback function :py:attr:`writer` to forward the reply.

        :param any context: A context returned by :py:meth:`receive_message`.
        :param bytes reply: The reply.
        """

        self.writer(reply)
