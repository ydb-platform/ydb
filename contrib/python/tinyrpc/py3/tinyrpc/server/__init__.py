#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Server definition.

Defines and implements a single-threaded, single-process, synchronous server.
"""

# FIXME: needs (more) unittests
# FIXME: needs checks for out-of-order, concurrency, etc as attributes
from typing import Any, Callable

import tinyrpc.exc
from tinyrpc import RPCProtocol
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.transports import ServerTransport


class RPCServer(object):
    """High level RPC server.

    The server is completely generic only assuming some form of RPC communication is intended.
    Protocol, data transport and method dispatching are injected into the server object.

    :param transport: The data transport mechanism to use.
    :param protocol: The RPC protocol to use.
    :param dispatcher: The dispatching mechanism to use.
    :type transport: :py:class:`~tinyrpc.transports.ServerTransport`
    :type protocol: :py:class:`~tinyrpc.protocols.RPCProtocol`
    :type dispatcher: :py:class:`~tinyrpc.dispatch.RPCDispatcher`
    """

    trace = None
    """Trace incoming and outgoing messages.

    When this attribute is set to a callable this callable will be called directly
    after a message has been received and immediately after a reply is sent.
    The callable should accept three positional parameters:
    
    :param str direction: Either '-->' for incoming or '<--' for outgoing data.
    :param any context: The context returned by :py:meth:`~tinyrpc.transports.ServerTransport.receive_message`.
    :param bytes message: The message itself.

    Example:
    
    .. code-block:: python

        def my_trace(direction, context, message):
            logger.debug('%s%s', direction, message)

        server = RPCServer(transport, protocol, dispatcher)
        server.trace = my_trace
        server.serve_forever()

    will log all incoming and outgoing traffic of the RPC service.
    
    Note that the ``message`` will be the data stream that is transported,
    not the interpreted meaning of that data.
    It is therefore possible that the binary stream is unreadable without further translation.
    """
    def __init__(
            self, transport: ServerTransport, protocol: RPCProtocol,
            dispatcher: RPCDispatcher
    ):
        self.transport = transport
        self.protocol = protocol
        self.dispatcher = dispatcher
        self.trace = None

    def serve_forever(self) -> None:
        """Handle requests forever.

        Starts the server loop; continuously calling :py:meth:`receive_one_message`
        to process the next incoming request.
        """
        while True:
            self.receive_one_message()

    def receive_one_message(self) -> None:
        """Handle a single request.

        Polls the transport for a new message.

        After a new message has arrived :py:meth:`_spawn` is called with a handler
        function and arguments to handle the request.

        The handler function will try to decode the message using the supplied
        protocol, if that fails, an error response will be sent. After decoding
        the message, the dispatcher will be asked to handle the resulting
        request and the return value (either an error or a result) will be sent
        back to the client using the transport.
        """
        context, message = self.transport.receive_message()
        if callable(self.trace):
            self.trace('-->', context, message)

        # assuming protocol is thread-safe and dispatcher is thread-safe, as
        # long as its immutable

        def handle_message(context: Any, message: bytes) -> None:
            """Parse, process and reply a single request."""
            try:
                request = self.protocol.parse_request(message)
            except tinyrpc.exc.RPCError as e:
                response = e.error_respond()
            else:
                response = self.dispatcher.dispatch(
                    request, getattr(self.protocol, '_caller', None)
                )

            # send reply
            if response is not None:
                result = response.serialize()
                if callable(self.trace):
                    self.trace('<--', context, result)
                self.transport.send_reply(context, result)

        self._spawn(handle_message, context, message)

    def _spawn(self, func: Callable, *args, **kwargs):
        """Spawn a handler function.

        This function is overridden in subclasses to provide concurrency.

        In the base implementation, it simply calls the supplied function
        ``func`` with ``*args`` and ``**kwargs``. This results in a
        single-threaded, single-process, synchronous server.

        :param func: A callable to call.
        :param args: Arguments to ``func``.
        :param kwargs: Keyword arguments to ``func``.
        """
        func(*args, **kwargs)
