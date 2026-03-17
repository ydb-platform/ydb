import sys
import functools
import warnings
from tornado import gen

PY3 = sys.version_info[0] == 3

if PY3:
    MAXSIZE = sys.maxsize

    def bytes_to_str(b):
        if isinstance(b, bytes):
            return str(b, 'utf8')
        return b

    def str_to_bytes(s):
        if isinstance(s, bytes):
            return s
        return s.encode('utf8')

    import urllib.parse
    unquote_plus = urllib.parse.unquote_plus
else:
    if sys.platform == "java":
        # Jython always uses 32 bits.
        MAXSIZE = int((1 << 31) - 1)
    else:
        # It's possible to have sizeof(long) != sizeof(Py_ssize_t).
        class X(object):
            def __len__(self):
                return 1 << 31
        try:
            len(X())
        except OverflowError:
            # 32-bit
            MAXSIZE = int((1 << 31) - 1)
        else:
            # 64-bit
            MAXSIZE = int((1 << 63) - 1)
            del X

    def bytes_to_str(s):
        if isinstance(s, unicode):
            return s.encode('utf-8')
        return s

    def str_to_bytes(s):
        if isinstance(s, unicode):
            return s.encode('utf8')
        return s

    import urllib
    unquote_plus = urllib.unquote_plus


def asynchronous(method):
    """Wrap request handler methods with this if they are asynchronous.
    This decorator is for callback-style asynchronous methods; for
    coroutines, use the ``@gen.coroutine`` decorator without
    ``@asynchronous``. (It is legal for legacy reasons to use the two
    decorators together provided ``@asynchronous`` is first, but
    ``@asynchronous`` will be ignored in this case)
    This decorator should only be applied to the :ref:`HTTP verb
    methods <verbs>`; its behavior is undefined for any other method.
    This decorator does not *make* a method asynchronous; it tells
    the framework that the method *is* asynchronous.  For this decorator
    to be useful the method must (at least sometimes) do something
    asynchronous.
    If this decorator is given, the response is not finished when the
    method returns. It is up to the request handler to call
    `self.finish() <RequestHandler.finish>` to finish the HTTP
    request. Without this decorator, the request is automatically
    finished when the ``get()`` or ``post()`` method returns. Example:
    .. testcode::
       class MyRequestHandler(RequestHandler):
           @asynchronous
           def get(self):
              http = httpclient.AsyncHTTPClient()
              http.fetch("http://friendfeed.com/", self._on_download)
           def _on_download(self, response):
              self.write("Downloaded!")
              self.finish()
    .. testoutput::
       :hide:
    .. versionchanged:: 3.1
       The ability to use ``@gen.coroutine`` without ``@asynchronous``.
    .. versionchanged:: 4.3 Returning anything but ``None`` or a
       yieldable object from a method decorated with ``@asynchronous``
       is an error. Such return values were previously ignored silently.
    .. deprecated:: 5.1
       This decorator is deprecated and will be removed in Tornado 6.0.
       Use coroutines instead.
    """
    warnings.warn("@asynchronous is deprecated, use coroutines instead",
                  DeprecationWarning)
    # Delay the IOLoop import because it's not available on app engine.
    from tornado.ioloop import IOLoop

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        self._auto_finish = False
        result = method(self, *args, **kwargs)
        if result is not None:
            result = gen.convert_yielded(result)

            # If @asynchronous is used with @gen.coroutine, (but
            # not @gen.engine), we can automatically finish the
            # request when the future resolves.  Additionally,
            # the Future will swallow any exceptions so we need
            # to throw them back out to the stack context to finish
            # the request.
            def future_complete(f):
                f.result()
                if not self._finished:
                    self.finish()
            IOLoop.current().add_future(result, future_complete)
            # Once we have done this, hide the Future from our
            # caller (i.e. RequestHandler._when_complete), which
            # would otherwise set up its own callback and
            # exception handler (resulting in exceptions being
            # logged twice).
            return None
        return result
    return wrapper
