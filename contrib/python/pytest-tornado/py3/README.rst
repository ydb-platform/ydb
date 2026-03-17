pytest-tornado
==============

.. image:: https://travis-ci.org/eugeniy/pytest-tornado.svg?branch=master
    :target: https://travis-ci.org/eugeniy/pytest-tornado

.. image:: https://coveralls.io/repos/eugeniy/pytest-tornado/badge.svg
    :target: https://coveralls.io/r/eugeniy/pytest-tornado

A py.test_ plugin providing fixtures and markers to simplify testing
of asynchronous tornado applications.

Installation
------------

::

    pip install pytest-tornado


Example
-------

.. code-block:: python

    import pytest
    import tornado.web

    class MainHandler(tornado.web.RequestHandler):
        def get(self):
            self.write("Hello, world")

    application = tornado.web.Application([
        (r"/", MainHandler),
    ])

    @pytest.fixture
    def app():
        return application

    @pytest.mark.gen_test
    def test_hello_world(http_client, base_url):
        response = yield http_client.fetch(base_url)
        assert response.code == 200


Running tests
-------------

::

    py.test


Fixtures
--------

io_loop
    creates an instance of the `tornado.ioloop.IOLoop`_ for each test case

http_port
    get a port used by the test server

base_url
    Get an absolute base url for the test server,
    for example ``http://localhost:59828``.
    Can also be used in a test with HTTPS fixture and will then return
    a corresponding url, for example ``http://localhost:48372``.

http_server
    start a tornado HTTP server, you must create an ``app`` fixture,
    which returns the `tornado.web.Application`_ to be tested

http_client
    get an asynchronous HTTP client


There is also the possibility to test applications with HTTPS.
For running a server with HTTPS you need a certificate.

https_port
    Get a port used by the test server.

https_server
    Start a tornado HTTPS server. You must create an ``app`` fixture,
    which returns the `tornado.web.Application`_ to be tested, and
    an ``ssl_options`` fixture which returns the SSL options for the
    `tornado.httpserver.HTTPServer`_.

https_client
    Get an asynchronous HTTP client.
    In case your test uses an self-signed certificate you can set
    ``verify=False`` on the fetch method.


Show fixtures provided by the plugin::

    py.test --fixtures


Markers
-------

A ``gen_test`` marker lets you write a coroutine-style tests used with the
`tornado.gen`_ module:

.. code-block:: python

    @pytest.mark.gen_test
    def test_tornado(http_client):
        response = yield http_client.fetch('http://www.tornadoweb.org/')
        assert response.code == 200


This marker supports writing tests with async/await syntax as well:

.. code-block:: python

    @pytest.mark.gen_test
    async def test_tornado(http_client):
        response = await http_client.fetch('http://www.tornadoweb.org/')
        assert response.code == 200


Marked tests will time out after 5 seconds. The timeout can be modified by
setting an ``ASYNC_TEST_TIMEOUT`` environment variable,
``--async-test-timeout`` command line argument or a marker argument.

.. code-block:: python

    @pytest.mark.gen_test(timeout=5)
    def test_tornado(http_client):
        yield http_client.fetch('http://www.tornadoweb.org/')

The mark can also receive a run_sync flag, which if turned off will, instead of running the test synchronously, will add it as a coroutine and run the IOLoop (until the timeout). For instance, this allows to test things on both a client and a server at the same time.

.. code-block:: python

    @pytest.mark.gen_test(run_sync=False)
    def test_tornado(http_server, http_client):
        response = yield http_client.fetch('http://localhost:5555/my_local_server_test/')
        assert response.body == 'Run on the same IOLoop!'


Show markers provided by the plugin::

    py.test --markers


.. _py.test: http://pytest.org/
.. _`tornado.httpserver.HTTPServer`: https://www.tornadoweb.org/en/latest/httpserver.html#http-server
.. _`tornado.ioloop.IOLoop`: http://tornado.readthedocs.org/en/latest/ioloop.html#ioloop-objects
.. _`tornado.web.Application`: http://tornado.readthedocs.org/en/latest/web.html#application-configuration
.. _`tornado.gen`: http://tornado.readthedocs.org/en/latest/gen.html
