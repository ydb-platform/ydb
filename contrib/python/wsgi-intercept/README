Installs a WSGI application in place of a real host for testing.

Introduction
============

Testing a WSGI application sometimes involves starting a server at a
local host and port, then pointing your test code to that address.
Instead, this library lets you intercept calls to any specific host/port
combination and redirect them into a `WSGI application`_ importable by
your test program. Thus, you can avoid spawning multiple processes or
threads to test your Web app.

Supported Libaries
==================

``wsgi_intercept`` works with a variety of HTTP clients in Python 2.7,
3.7 and beyond, and in pypy.

* urllib2
* urllib.request
* httplib
* http.client
* httplib2
* requests
* urllib3 (<2.0.0, urllib3 2 support is in progress)

How Does It Work?
=================

``wsgi_intercept`` works by replacing ``httplib.HTTPConnection`` with a
subclass, ``wsgi_intercept.WSGI_HTTPConnection``. This class then
redirects specific server/port combinations into a WSGI application by
emulating a socket. If no intercept is registered for the host and port
requested, those requests are passed on to the standard handler.

The easiest way to use an intercept is to import an appropriate subclass
of ``~wsgi_intercept.interceptor.Interceptor`` and use that as a
context manager over web requests that use the library associated with
the subclass. For example::

    import httplib2
    from wsgi_intercept.interceptor import Httplib2Interceptor
    from mywsgiapp import app

    def load_app():
        return app

    http = httplib2.Http()
    with Httplib2Interceptor(load_app, host='example.com', port=80) as url:
        response, content = http.request('%s%s' % (url, '/path'))
        assert response.status == 200

The interceptor class may aslo be used directly to install intercepts.
See the module documentation for more information.

Older versions required that the functions ``add_wsgi_intercept(host,
port, app_create_fn, script_name='')`` and ``remove_wsgi_intercept(host,port)``
be used to specify which URLs should be redirected into what applications.
These methods are still available, but the ``Interceptor`` classes are likely
easier to use for most use cases.

.. note:: ``app_create_fn`` is a *function object* returning a WSGI
          application; ``script_name`` becomes ``SCRIPT_NAME`` in the WSGI
          app's environment, if set.

.. note:: If ``http_proxy`` or ``https_proxy`` is set in the environment
          this can cause difficulties with some of the intercepted libraries.
          If requests or urllib is being used, these will raise an exception
          if one of those variables is set.

.. note:: If ``wsgi_intercept.STRICT_RESPONSE_HEADERS`` is set to ``True``
          then response headers sent by an application will be checked to
          make sure they are of the type ``str`` native to the version of
          Python, as required by pep 3333. The default is ``False`` (to
          preserve backwards compatibility)


Install
=======

::

    pip install -U wsgi_intercept

Packages Intercepted
====================

Unfortunately each of the HTTP client libraries use their own specific
mechanism for making HTTP call-outs, so individual implementations are
needed. At this time there are implementations for ``httplib2``,
``urllib3`` (<2.0.0) and ``requests`` in both Python 2 and 3, ``urllib2`` and
``httplib`` in Python 2 and ``urllib.request`` and ``http.client``
in Python 3.

If you are using Python 2 and need support for a different HTTP
client, require a version of ``wsgi_intercept<0.6``. Earlier versions
include support for ``webtest``, ``webunit`` and ``zope.testbrowser``.

The best way to figure out how to use interception is to inspect
`the tests`_. More comprehensive documentation available upon
request.

.. _the tests: https://github.com/cdent/wsgi-intercept/tree/master/test


History
=======

Pursuant to Ian Bicking's `"best Web testing framework"`_ post, Titus
Brown put together an `in-process HTTP-to-WSGI interception mechanism`_
for his own Web testing system, twill. Because the mechanism is pretty
generic -- it works at the httplib level -- Titus decided to try adding
it into all of the *other* Python Web testing frameworks.

The Python 2 version of wsgi-intercept was the result. Kumar McMillan
later took over maintenance.

The current version is tested with Python 2.7, 3.5-3.11, and pypy and pypy3.
It was assembled by `Chris Dent`_. Testing and documentation improvements
from `Sasha Hart`_.

.. _"best Web testing framework":
     http://blog.ianbicking.org/best-of-the-web-app-test-frameworks.html
.. _in-process HTTP-to-WSGI interception mechanism:
     http://www.advogato.org/person/titus/diary.html?start=119
.. _WSGI application: http://www.python.org/peps/pep-3333.html
.. _Chris Dent: https://github.com/cdent
.. _Sasha Hart: https://github.com/sashahart

Project Home
============

This project lives on `GitHub`_. Please submit all bugs, patches,
failing tests, et cetera using the Issue Tracker.

Additional documentation is available on `Read The Docs`_.

.. _GitHub: http://github.com/cdent/wsgi-intercept
.. _Read The Docs: http://wsgi-intercept.readthedocs.org/en/latest/
