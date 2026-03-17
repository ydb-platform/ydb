tinyrpc: A small and modular way of handling web-related RPC
============================================================

.. note::

    Tinyrpc is being revised.

    The current version will enter maintenance mode.
    I will keep it alive until Python2 is discontinued.
    It can be installed from pip using its version number
    e.g. ``pip install tinyrpc==0.9.1``.

    The main line of development will start with version 1.0.0.
    It will not try to remain compatible with the 0.9.x version.
    It will support Python3 only.

    Main goals for version 1.0.0 are:

    - good Python3 support.
    - restructure the code to eliminate unnecessary dependencies.
    - rework and extend the documentation with more examples.

Motivation
----------

As of this writing (in Jan 2013) there are a few jsonrpc_ libraries already out
there on PyPI_, most of them handling one specific use case (e.g. json via
WSGI, using Twisted, or TCP-sockets).

None of the libraries, however, made it easy to reuse the jsonrpc_-parsing bits
and substitute a different transport (i.e. going from json_ via TCP_ to an
implementation using WebSockets_ or 0mq_).

In the end, all these libraries have their own dispatching interfaces and a
custom implementation of handling jsonrpc_.

``tinyrpc`` aims to do better by dividing the problem into cleanly
interchangeable parts that allow easy addition of new transport methods, RPC
protocols or dispatchers.

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
| jsonext    | optional in JSONRPCProtocol                           |
+------------+-------------------------------------------------------+
| websocket  | WSServerTransport                                     |
+------------+-------------------------------------------------------+
| wsgi       | WsgiServerTransport                                   |
+------------+-------------------------------------------------------+
| zmq        | ZmqServerTransport, ZmqClientTransport                |
+------------+-------------------------------------------------------+

.. _jsonrpc: http://www.jsonrpc.org/
.. _PyPI: http://pypi.python.org
.. _json: http://www.json.org/
.. _TCP: http://en.wikipedia.org/wiki/Transmission_Control_Protocol
.. _WebSockets: http://en.wikipedia.org/wiki/WebSocket
.. _0mq: http://www.zeromq.org/
