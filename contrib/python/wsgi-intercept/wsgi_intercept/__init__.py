"""Installs a WSGI application in place of a real host for testing.

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
"""
from __future__ import print_function

import sys
import traceback
from io import BytesIO

# Don't use six here because it is unquote_to_bytes that we want in
# Python 3.
try:
    from urllib.parse import unquote_to_bytes as url_unquote
except ImportError:
    from urllib import unquote as url_unquote

import six
from six.moves.http_client import HTTPConnection, HTTPSConnection


# Set this to True to cause response headers from the intercepted
# app to be confirmed as bytestrings, behaving as some wsgi servers.
STRICT_RESPONSE_HEADERS = False


debuglevel = 0
# 1 basic
# 2 verbose

####

#
# Specify which hosts/ports to target for interception to a given WSGI app.
#
# For simplicity's sake, intercept ENTIRE host/port combinations;
# intercepting only specific URL subtrees gets complicated, because we don't
# have that information in the HTTPConnection.connect() function that does the
# redirection.
#
# format: key=(host, port), value=(create_app, top_url)
#
# (top_url becomes the SCRIPT_NAME)

_wsgi_intercept = {}


def add_wsgi_intercept(host, port, app_create_fn, script_name=''):
    """
    Add a WSGI intercept call for host:port, using the app returned
    by app_create_fn with a SCRIPT_NAME of 'script_name' (default '').
    """
    _wsgi_intercept[(host, port)] = (app_create_fn, script_name)


def remove_wsgi_intercept(*args):
    """
    Remove the WSGI intercept call for (host, port).  If no arguments are
    given, removes all intercepts
    """
    global _wsgi_intercept
    if len(args) == 0:
        _wsgi_intercept = {}
    else:
        key = (args[0], args[1])
        if key in _wsgi_intercept:
            del _wsgi_intercept[key]
    return len(_wsgi_intercept)


#
# make_environ: behave like a Web server.  Take in 'input', and behave
# as if you're bound to 'host' and 'port'; build an environment dict
# for the WSGI app.
#
# This is where the magic happens, folks.
#
def make_environ(inp, host, port, script_name):
    """
    Take 'inp' as if it were HTTP-speak being received on host:port,
    and parse it into a WSGI-ok environment dictionary.  Return the
    dictionary.

    Set 'SCRIPT_NAME' from the 'script_name' input, and, if present,
    remove it from the beginning of the PATH_INFO variable.
    """
    #
    # parse the input up to the first blank line (or its end).
    #

    environ = {}

    method_line = inp.readline()
    if six.PY3:
        method_line = method_line.decode('ISO-8859-1')

    content_type = None
    content_length = None
    cookies = []

    for line in inp:
        if not line.strip():
            break

        k, v = line.strip().split(b':', 1)
        v = v.lstrip()
        # Make header value a "native" string. PEP 3333 requires that
        # string-like things in headers be of type `str`. Much of the
        # time this isn't a problem but the SimpleCookie library does
        # type checking against `type("")`.
        v = str(v.decode('ISO-8859-1'))

        #
        # take care of special headers, and for the rest, put them
        # into the environ with HTTP_ in front.
        #

        if k.lower() == b'content-type':
            content_type = v
        elif k.lower() == b'content-length':
            content_length = v
        elif k.lower() == b'cookie' or k.lower() == b'cookie2':
            cookies.append(v)
        else:
            h = k.upper()
            h = h.replace(b'-', b'_')
            environ['HTTP_' + str(h.decode('ISO-8859-1'))] = v

        if debuglevel >= 2:
            print('HEADER:', k, v)

    #
    # decode the method line
    #

    if debuglevel >= 2:
        print('METHOD LINE:', method_line)

    method, url, protocol = method_line.split(' ')

    # Store the URI as requested by the user, without modification
    # so that PATH_INFO munging can be corrected.
    environ['REQUEST_URI'] = url
    environ['RAW_URI'] = url

    # clean the script_name off of the url, if it's there.
    if not url.startswith(script_name):
        script_name = ''                # @CTB what to do -- bad URL.  scrap?
    else:
        url = url[len(script_name):]

    url = url.split('?', 1)
    path_info = url_unquote(url[0])
    query_string = ""
    if len(url) == 2:
        query_string = url[1]

    if debuglevel:
        print("method: %s; script_name: %s; path_info: %s; query_string: %s" %
                (method, script_name, path_info, query_string))

    r = inp.read()
    inp = BytesIO(r)

    #
    # fill out our dictionary.
    #

    # In Python3 turn the bytes of the path info into a string of
    # latin-1 code points, because that's what the spec says we must
    # do to be like a server. Later various libraries will be forced
    # to decode and then reencode to get the UTF-8 that everyone
    # wants.
    if six.PY3:
        path_info = path_info.decode('latin-1')

    environ.update({
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": inp,  # to read for POSTs
        "wsgi.errors": sys.stderr,
        "wsgi.multithread": 0,
        "wsgi.multiprocess": 0,
        "wsgi.run_once": 0,

        "PATH_INFO": path_info,
        "REMOTE_ADDR": '127.0.0.1',
        "REQUEST_METHOD": method,
        "SCRIPT_NAME": script_name,
        "SERVER_NAME": host,
        "SERVER_PORT": port,
        "SERVER_PROTOCOL": protocol,
    })

    #
    # query_string, content_type & length are optional.
    #

    if query_string:
        environ['QUERY_STRING'] = query_string

    if content_type:
        environ['CONTENT_TYPE'] = content_type
        if debuglevel >= 2:
            print('CONTENT-TYPE:', content_type)
    if content_length:
        environ['CONTENT_LENGTH'] = content_length
        if debuglevel >= 2:
            print('CONTENT-LENGTH:', content_length)

    #
    # handle cookies.
    #
    if cookies:
        environ['HTTP_COOKIE'] = "; ".join(cookies)

    if debuglevel:
        print('WSGI environ dictionary:', environ)

    return environ


class WSGIAppError(Exception):
    """
    An exception that wraps any Exception raised by the WSGI app
    that is called. This is done for two reasons: it ensures that
    intercepted libraries (such as requests) which use exceptions
    to trigger behaviors are not interfered with by exceptions from
    the WSGI app. It also helps to define a solid boundary, akin
    to the network boundary between server and client, in the
    testing environment.
    """
    def __init__(self, error, exc_info):
        Exception.__init__(self)
        self.error = error
        self.exception_type = exc_info[0]
        self.exception_value = exc_info[1]
        self.traceback = exc_info[2]

    def __str__(self):
        frame = traceback.extract_tb(self.traceback)[-1]
        formatted = "{0!r} at {1}:{2}".format(
            self.error,
            frame[0],
            frame[1],
        )
        return formatted


#
# fake socket for WSGI intercept stuff.
#
class wsgi_fake_socket:
    """
    Handle HTTP traffic and stuff into a WSGI application object instead.

    Note that this class assumes:

     1. 'makefile' is called (by the response class) only after all of the
        data has been sent to the socket by the request class;
     2. non-persistent (i.e. non-HTTP/1.1) connections.
    """
    def __init__(self, app, host, port, script_name, https=False):
        self.app = app                  # WSGI app object
        self.host = host
        self.port = port
        self.script_name = script_name  # SCRIPT_NAME (app mount point)

        self.inp = BytesIO()           # stuff written into this "socket"
        self.write_results = []          # results from the 'write_fn'
        self.results = None             # results from running the app
        self.output = BytesIO()        # all output from the app, incl headers
        self.https = https

    def makefile(self, *args, **kwargs):
        """
        'makefile' is called by the HTTPResponse class once all of the
        data has been written.  So, in this interceptor class, we need to:

          1. build a start_response function that grabs all the headers
             returned by the WSGI app;
          2. create a wsgi.input file object 'inp', containing all of the
             traffic;
          3. build an environment dict out of the traffic in inp;
          4. run the WSGI app & grab the result object;
          5. concatenate & return the result(s) read from the result object.
        """

        # dynamically construct the start_response function for no good reason.

        self.headers = []

        def start_response(status, headers, exc_info=None):
            # construct the HTTP request.
            self.output.write(
                b"HTTP/1.0 " + status.encode('ISO-8859-1') + b"\n")
            # Keep the reference of the headers list to write them only
            # when the whole application have been processed
            self.headers = headers
            return self.write_results.append

        # construct the wsgi.input file from everything that's been
        # written to this "socket".
        inp = BytesIO(self.inp.getvalue())

        # build the environ dictionary.
        environ = make_environ(inp, self.host, self.port, self.script_name)
        if self.https:
            environ['wsgi.url_scheme'] = 'https'

        # run the application.
        try:
            app_result = self.app(environ, start_response)
        except Exception as error:
            raise WSGIAppError(error, sys.exc_info())
        self.result = iter(app_result)

        ###

        # read all of the results.  the trick here is to get the *first*
        # bit of data from the app via the generator, *then* grab & return
        # the data passed back from the 'write' function, and then return
        # the generator data.  this is because the 'write' fn doesn't
        # necessarily get called until the first result is requested from
        # the app function.

        try:
            generator_data = None
            try:
                generator_data = next(self.result)

            finally:
                # send the headers

                for k, v in self.headers:
                    if STRICT_RESPONSE_HEADERS:
                        if not (isinstance(k, str) and isinstance(v, str)):
                            raise TypeError(
                                "Header has a key '%s' or value '%s' "
                                "which is not a native str." % (k, v))
                    try:
                        k = k.encode('ISO-8859-1')
                    except AttributeError:
                        pass
                    try:
                        v = v.encode('ISO-8859-1')
                    except AttributeError:
                        pass
                    self.output.write(k + b': ' + v + b"\n")
                self.output.write(b'\n')

                for data in self.write_results:
                    self.output.write(data)

            if generator_data is not None:
                try:
                    self.output.write(generator_data)
                except TypeError as exc:
                    raise TypeError('bytes required in response: %s' % exc)

                while 1:
                    data = next(self.result)
                    self.output.write(data)

        except StopIteration:
            pass

        if hasattr(app_result, 'close'):
            app_result.close()

        if debuglevel >= 2:
            print("***", self.output.getvalue(), "***")

        # return the concatenated results.
        return BytesIO(self.output.getvalue())

    def sendall(self, content):
        """
        Save all the traffic to self.inp.
        """
        if debuglevel >= 2:
            print(">>>", content, ">>>")

        try:
            self.inp.write(content)
        except TypeError:
            self.inp.write(content.encode('utf-8'))

    def close(self):
        "Do nothing, for now."
        pass


#
# WSGI_HTTPConnection
#
class WSGI_HTTPConnection(HTTPConnection):
    """
    Intercept all traffic to certain hosts & redirect into a WSGI
    application object.
    """
    def get_app(self, host, port):
        """
        Return the app object for the given (host, port).
        """
        key = (host, int(port))

        app, script_name = None, None

        if key in _wsgi_intercept:
            (app_fn, script_name) = _wsgi_intercept[key]
            app = app_fn()

        return app, script_name

    def connect(self):
        """
        Override the connect() function to intercept calls to certain
        host/ports.

        If no app at host/port has been registered for interception then
        a normal HTTPConnection is made.
        """
        if debuglevel:
            sys.stderr.write('connect: %s, %s\n' % (self.host, self.port,))

        try:
            (app, script_name) = self.get_app(self.host, self.port)
            if app:
                if debuglevel:
                    sys.stderr.write('INTERCEPTING call to %s:%s\n' %
                                     (self.host, self.port,))
                self.sock = wsgi_fake_socket(app, self.host, self.port,
                                             script_name)
            else:
                HTTPConnection.connect(self)

        except Exception:
            if debuglevel:              # intercept & print out tracebacks
                traceback.print_exc()
            raise


#
# WSGI_HTTPSConnection
#


class WSGI_HTTPSConnection(HTTPSConnection, WSGI_HTTPConnection):
    """
    Intercept all traffic to certain hosts & redirect into a WSGI
    application object.
    """
    def get_app(self, host, port):
        """
        Return the app object for the given (host, port).
        """
        key = (host, int(port))

        app, script_name = None, None

        if key in _wsgi_intercept:
            (app_fn, script_name) = _wsgi_intercept[key]
            app = app_fn()

        return app, script_name

    def connect(self):
        """
        Override the connect() function to intercept calls to certain
        host/ports.

        If no app at host/port has been registered for interception then
        a normal HTTPSConnection is made.
        """
        if debuglevel:
            sys.stderr.write('connect: %s, %s\n' % (self.host, self.port,))

        try:
            (app, script_name) = self.get_app(self.host, self.port)
            if app:
                if debuglevel:
                    sys.stderr.write('INTERCEPTING call to %s:%s\n' %
                                     (self.host, self.port,))
                self.sock = wsgi_fake_socket(app, self.host, self.port,
                                             script_name, https=True)
            else:
                try:
                    import ssl
                    if hasattr(self, '_context'):
                        self._context.check_hostname = self.assert_hostname
                        self._check_hostname = self.assert_hostname     # Py3.6
                        if hasattr(ssl, 'VerifyMode'):
                            # Support for Python3.6 and higher
                            if isinstance(self.cert_reqs, ssl.VerifyMode):
                                self._context.verify_mode = self.cert_reqs
                            else:
                                self._context.verify_mode = ssl.VerifyMode[
                                    self.cert_reqs]
                        elif isinstance(self.cert_reqs, six.string_types):
                            # Support for Python3.5 and below
                            self._context.verify_mode = getattr(ssl,
                                    self.cert_reqs,
                                    self._context.verify_mode)
                        else:
                            self._context.verify_mode = self.cert_reqs

                    if not hasattr(self, 'key_file'):
                        self.key_file = None
                    if not hasattr(self, 'cert_file'):
                        self.cert_file = None
                    if not hasattr(self, '_context'):
                        try:
                            self._context = ssl.create_default_context()
                        except AttributeError:
                            self._context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                            self._context.options |= ssl.OP_NO_SSLv2
                            if not hasattr(self, 'check_hostname'):
                                self._check_hostname = (
                                    self._context.verify_mode != ssl.CERT_NONE
                                )
                            else:
                                self._check_hostname = self.check_hostname
                except (ImportError, AttributeError):
                    import traceback
                    traceback.print_exc()
                HTTPSConnection.connect(self)

        except Exception:
            if debuglevel:              # intercept & print out tracebacks
                traceback.print_exc()
            raise
