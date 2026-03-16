#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
cgi extends the tinyrpc package.

The CGIServerTransport adds CGI as a supported server protocol that can be used
with the regular HTTP client.

(c) 2016, Leo Noordergraaf, Nextpertise BV
This code is made available under the same license as tinyrpc itself.
"""

from __future__ import print_function

import os
import sys
import json
import cgi
try:
    import urllib.parse as urlparse
except ImportError:
    import urllib as urlparse

from . import ServerTransport

class CGIServerTransport(ServerTransport):
    """CGI transport.
    
    Reading stdin is blocking but, given that we've been called, something is
    waiting.  The transport accepts both GET and POST request.

    A POST request provides the entire JSON-RPC request in the body of the HTTP
    request.

    A GET request provides the elements of the JSON-RPC request in separate query
    parameters and only the params field contains a JSON object or array.
    i.e. curl 'http://server?jsonrpc=2.0&id=1&method="doit"&params={"arg"="something"}'
    """

    def receive_message(self):
        """Receive a message from the transport.

        Blocks until another message has been received. May return a context
        opaque to clients that should be passed on
        :py:func:`~tinyrpc.transport.CGIServerTransport.send_reply` to identify the
        client later on.

        :return: A tuple consisting of ``(context, message)``.
        """

        if 'CONTENT_LENGTH' in os.environ:
            # POST
            content_length = int(os.environ['CONTENT_LENGTH'])
            request_json = sys.stdin.read(content_length)
            request_json = urlparse.unquote(request_json)
        else:
            # GET
            fields = cgi.FieldStorage()
            jsonrpc = fields.getfirst("jsonrpc")
            id = fields.getfirst("id")
            method = fields.getfirst("method")
            params = fields.getfirst("params")
            # Create request string
            request_json = json.dumps({
                'jsonrpc': jsonrpc,
                'id': id,
                'method': method,
                'params': params
            })
        return None, request_json


    def send_reply(self, context, reply):
        """Sends a reply to a client.

        The client is usually identified by passing ``context`` as returned
        from the original
        :py:func:`~tinyrpc.transport.Transport.receive_message` call.

        Messages must be strings, it is up to the sender to convert the
        beforehand. A non-string value raises a :py:exc:`TypeError`.

        :param context: A context returned by
                        :py:func:`~tinyrpc.transport.CGIServerTransport.receive_message`.
        :param reply: A string to send back as the reply.
        """

        # context isn't used with cgi
        print("Content-Type: application/json")
        print("Cache-Control: no-cache")
        print("Pragma: no-cache")
        print("Content-Length: %d" % len(reply))
        print()
        print(reply)

