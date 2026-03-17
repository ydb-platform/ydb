# -*- coding: utf-8 -*-
import copy

import gevent
from gevent import Greenlet
from gevent.queue import Queue

from ws4py.client import WebSocketBaseClient

__all__ = ['WebSocketClient']

class WebSocketClient(WebSocketBaseClient):
    def __init__(self, url, protocols=None, extensions=None, heartbeat_freq=None, ssl_options=None, headers=None, exclude_headers=None):
        """
        WebSocket client that executes the
        :meth:`run() <ws4py.websocket.WebSocket.run>` into a gevent greenlet.

        .. code-block:: python

          ws = WebSocketClient('ws://localhost:9000/echo', protocols=['http-only', 'chat'])
          ws.connect()

          ws.send("Hello world")

          def incoming():
            while True:
               m = ws.receive()
               if m is not None:
                  print str(m)
               else:
                  break

          def outgoing():
            for i in range(0, 40, 5):
               ws.send("*" * i)

          greenlets = [
             gevent.spawn(incoming),
             gevent.spawn(outgoing),
          ]
          gevent.joinall(greenlets)
        """
        WebSocketBaseClient.__init__(self, url, protocols, extensions, heartbeat_freq,
                                     ssl_options=ssl_options, headers=headers, exclude_headers=exclude_headers)
        self._th = Greenlet(self.run)

        self.messages = Queue()
        """
        Queue that will hold received messages.
        """

    def handshake_ok(self):
        """
        Called when the upgrade handshake has completed
        successfully.

        Starts the client's thread.
        """
        self._th.start()

    def received_message(self, message):
        """
        Override the base class to store the incoming message
        in the `messages` queue.
        """
        self.messages.put(copy.deepcopy(message))

    def closed(self, code, reason=None):
        """
        Puts a :exc:`StopIteration` as a message into the
        `messages` queue.
        """
        # When the connection is closed, put a StopIteration
        # on the message queue to signal there's nothing left
        # to wait for
        self.messages.put(StopIteration)

    def receive(self, block=True):
        """
        Returns messages that were stored into the
        `messages` queue and returns `None` when the
        websocket is terminated or closed.
        `block` is passed though the gevent queue `.get()` method, which if 
        True will block until an item in the queue is available. Set this to 
        False if you just want to check the queue, which will raise an 
        Empty exception you need to handle if there is no message to return.
        """
        # If the websocket was terminated and there are no messages
        # left in the queue, return None immediately otherwise the client
        # will block forever
        if self.terminated and self.messages.empty():
            return None
        message = self.messages.get(block=block)
        if message is StopIteration:
            return None
        return message
