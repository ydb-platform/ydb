#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
callback extends the tinyrpc package.

The CallbackServerTransport uses the provided callbacks to implement
communication with the counterpart.

(c) 2016, Leo Noordergraaf, Nextpertise BV
This code is made available under the same license as tinyrpc itself.
"""

import json

from . import ServerTransport

class CallbackServerTransport(ServerTransport):
    """Callback server transport.
    
    Used when tinyrpc is part of a system where it cannot directly attach
    to a socket or stream. The methods :py:meth:`receive_message` and 
    :py:meth:`send_reply` are implemented by callback functions that were
    passed to :py:meth:`__init__`.

    This transport is also useful for testing the other modules of tinyrpc.
    """
    def __init__(self, reader, writer):
        """Install callbacks.
        
        :param callable reader: Expected to return a string or other data structure.
                                A string is assumed to be json and is passed on as is,
                                otherwise the returned value is converted into a json
                                string.
        :param callable writer: Expected to accept a single parameter of type string.
        """
        
        super(CallbackServerTransport, self).__init__()
        self.reader = reader
        self.writer = writer

    def receive_message(self):
        """Receive a message from the transport.

        Uses the callback function :py:attr:`reader` to obtain a json string.
        May return a context opaque to clients that should be passed on
        :py:meth:`send_reply` to identify the client later on.

        :return: A tuple consisting of ``(context, message)``.
        """

        data = self.reader()
        if type(data) != str:
            # Turn non-string to string or die trying
            data = json.dumps(data)
        return None, data

    def send_reply(self, context, reply):
        """Sends a reply to a client.

        The client is usually identified by passing ``context`` as returned
        from the original :py:meth:`receive_message` call.

        Messages must be a string, it is up to the sender to convert it
        beforehand. A non-string value raises a :py:exc:`TypeError`.

        :param context: A context returned by :py:meth:`receive_message`.
        :param reply: A string to send back as the reply.
        """

        self.writer(reply)
