SockJS-tornado server
=====================

SockJS-tornado is a Python server side counterpart of `SockJS-client browser library <https://github.com/sockjs/sockjs-client>`_
running on top of `Tornado <http://tornadoweb.org>`_ framework.

Simplified echo SockJS server could look more or less like::
    from tornado import web, ioloop
    from sockjs.tornado import SockJSRouter, SockJSConnection
    
    class EchoConnection(SockJSConnection):
        def on_message(self, msg):
            self.send(msg)
        
    if __name__ == '__main__':
        EchoRouter = SockJSRouter(EchoConnection, '/echo')
        
        app = web.Application(EchoRouter.urls)
        app.listen(9999)
        ioloop.IOLoop.instance().start()

(Take look at `examples <https://github.com/MrJoes/sockjs-tornado/tree/master/examples>`_ for a complete version).

Subscribe to `SockJS mailing list <https://groups.google.com/forum/#!forum/sockjs>`_ for discussions and support.

SockJS-tornado API
------------------

SockJS provides slightly different API than ``tornado.websocket``. Main differences are:

1. Depending on transport, actual client connection might or might not be there. So, there is no _self.request_ and
    other ``tornado.web.RequestHandler`` properties.
2. Changed ``open`` callback name to ``on_open`` to be more consistent with other callbacks.
3. Instead of ``write_message``, all messages are sent using ``send`` method. Just in case, ``send`` in ``tornado.web.RequestHandler``
    sends raw data over the connection, without encoding it.
4. There is handy ``broadcast`` function, which accepts list (or iterator) of clients and message to send.

Settings
--------

You can pass various settings to the ``SockJSRouter``, in a dictionary::

    MyRouter = SockJSRouter(MyConnection, '/my', dict(disabled_transports=['websocket']))

Deployment
----------

sockjs-tornado properly works behind haproxy and it is recommended deployment approach.

Sample configuration file can be found `here <https://raw.github.com/sockjs/sockjs-node/master/examples/haproxy.cfg>`_.

If your log is full of "WARNING: Connection closed by the client", pass ``no_keep_alive`` as ``True`` to ``HTTPServer`` constructor::

    HTTPServer(app, no_keep_alive=True).listen(port)

or::

    app.listen(port, no_keep_alive=True)

