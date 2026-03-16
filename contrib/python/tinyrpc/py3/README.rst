tinyrpc: A small and modular way of handling web-related RPC
============================================================

.. image:: https://readthedocs.org/projects/tinyrpc/badge/?version=latest
    :target: https://tinyrpc.readthedocs.io/en/latest
.. image:: https://github.com/mbr/tinyrpc/actions/workflows/python-tox.yml/badge.svg
    :target: https://github.com/mbr/tinyrpc/actions/workflows/python-tox.yml
.. image:: https://badge.fury.io/py/tinyrpc.svg
    :target: https://pypi.org/project/tinyrpc/

Note
----

Tinyrpc has been revised.

The current version will support Python3 only.
Have a look at the 0.9.x version if you need Python2 support.
Python2 support will be dropped completely when Python2 retires,
somewhere in 2020.

Motivation
----------

As of this writing (in Jan 2013) there are a few jsonrpc_ libraries already out
there on PyPI_, most of them handling one specific use case (e.g. json via
WSGI, using Twisted, or TCP-sockets).

None of the libraries, however, makes it easy to reuse the jsonrpc_-parsing bits
and substitute a different transport (i.e. going from json_ via TCP_ to an
implementation using WebSockets_ or 0mq_).

In the end, all these libraries have their own dispatching interfaces and a
custom implementation of handling jsonrpc_.  Today (march 2019) that hasn't changed.

``tinyrpc`` aims to do better by dividing the problem into cleanly
interchangeable parts that allow easy addition of new transport methods, RPC
protocols or dispatchers.

Example:

To create a server process receiving and handling JSONRPC requests do:

.. code-block:: python

    import gevent
    import gevent.pywsgi
    import gevent.queue
    from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
    from tinyrpc.transports.wsgi import WsgiServerTransport
    from tinyrpc.server.gevent import RPCServerGreenlets
    from tinyrpc.dispatch import RPCDispatcher

    dispatcher = RPCDispatcher()
    transport = WsgiServerTransport(queue_class=gevent.queue.Queue)

    # start wsgi server as a background-greenlet
    wsgi_server = gevent.pywsgi.WSGIServer(('127.0.0.1', 5000), transport.handle)
    gevent.spawn(wsgi_server.serve_forever)

    rpc_server = RPCServerGreenlets(transport, JSONRPCProtocol(), dispatcher)

    @dispatcher.public
    def reverse_string(s):
        return s[::-1]

    # in the main greenlet, run our rpc_server
    rpc_server.serve_forever()

The corresponding client code looks like:

.. code-block:: python

    from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
    from tinyrpc.transports.http import HttpPostClientTransport
    from tinyrpc import RPCClient

    rpc_client = RPCClient(
        JSONRPCProtocol(),
        HttpPostClientTransport('http://127.0.0.1:5000/'))

    remote_server = rpc_client.get_proxy()

    # call a method called 'reverse_string' with a single string argument
    result = remote_server.reverse_string('Hello, World!')

    print("Server answered:", result)

Documentation
-------------

You'll quickly find that ``tinyrpc`` has more documentation and tests than core
code, hence the name. See the documentation at
<https://tinyrpc.readthedocs.org> for more details, especially the
Structure-section to get a birds-eye view.

Installation
------------

.. code-block:: sh

   pip install tinyrpc

will install ``tinyrpc`` with its default dependencies.

Optional dependencies
---------------------

Depending on the protocols and transports you want to use additional dependencies
are required. You can instruct pip to install these dependencies by specifying
extras to the basic install command.

.. code-block:: sh

   pip install tinyrpc[httpclient, wsgi]

will install ``tinyrpc`` with dependencies for the httpclient and wsgi transports.

Available extras are:

+------------+-------------------------------------------------------+
| Option     |  Needed to use objects of class                       |
+============+=======================================================+
| gevent     | optional in RPCClient, required by RPCServerGreenlets |
+------------+-------------------------------------------------------+
| httpclient | HttpPostClientTransport, HttpWebSocketClientTransport |
+------------+-------------------------------------------------------+
| msgpack    | implements MSGPACKRPCProtocol                         |
+------------+-------------------------------------------------------+
| jsonext    | optional in JSONRPCProtocol                           |
+------------+-------------------------------------------------------+
| rabbitmq   | RabbitMQServerTransport, RabbitMQClientTransport      |
+------------+-------------------------------------------------------+
| websocket  | WSServerTransport                                     |
+------------+-------------------------------------------------------+
| wsgi       | WsgiServerTransport                                   |
+------------+-------------------------------------------------------+
| zmq        | ZmqServerTransport, ZmqClientTransport                |
+------------+-------------------------------------------------------+

New in version 1.1.0
--------------------

Tinyrpc supports RabbitMQ has transport medium.

New in version 1.0.4
--------------------

Tinyrpc now supports the MSGPACK RPC protocol in addition to JSON-RPC.


.. _jsonrpc: http://www.jsonrpc.org/
.. _PyPI: http://pypi.python.org
.. _json: http://www.json.org/
.. _TCP: http://en.wikipedia.org/wiki/Transmission_Control_Protocol
.. _WebSockets: http://en.wikipedia.org/wiki/WebSocket
.. _0mq: http://www.zeromq.org/
