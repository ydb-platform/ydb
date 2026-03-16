from __future__ import absolute_import, print_function

"""M2Crypto enhancement to Python's urllib for handling
'https' url's.

FIXME: it is questionable whether we need this old-style module at all. urllib
(not urllib2) is in Python 3 support just as a legacy API.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

import base64
import warnings

from M2Crypto import SSL, httpslib, six

from M2Crypto.six.moves.urllib_response import addinfourl
from typing import AnyStr, Optional  # noqa

# six.moves doesn't support star imports
if six.PY3:
    from urllib.request import *  # noqa for other modules to import
    from urllib.parse import *  # noqa for other modules to import
    from urllib.error import *  # noqa for other modules to import
else:
    from urllib import *  # noqa


def open_https(self, url, data=None, ssl_context=None):
    # type: (URLOpener, AnyStr, Optional[bytes], Optional[SSL.Context]) -> addinfourl
    """
    Open URL over the SSL connection.

    :param url: URL to be opened
    :param data: data for the POST request
    :param ssl_context: SSL.Context to be used
    :return:
    """
    if six.PY3:
        warnings.warn('URLOpener has been deprecated in Py3k', DeprecationWarning)

    if ssl_context is not None and isinstance(ssl_context, SSL.Context):
        self.ctx = ssl_context
    else:
        self.ctx = SSL.Context()
    user_passwd = None
    if isinstance(url, six.string_types):
        try:               # python 2
            # http://pydoc.org/2.5.1/urllib.html
            host, selector = splithost(url)
            if host:
                user_passwd, host = splituser(host)
                host = unquote(host)
            realhost = host
        except NameError:  # python 3 has no splithost
            # https://docs.python.org/3/library/urllib.parse.html
            parsed = urlparse(url)
            host = parsed.hostname
            if parsed.port:
                host += ":{0}".format(parsed.port)
            user_passwd = parsed.password
            if parsed.password:
                user_passwd += ":{0}".format(parsed.password)
            selector = parsed.path
    else:
        host, selector = url
        urltype, rest = splittype(selector)
        url = rest
        user_passwd = None
        if urltype.lower() != 'http':
            realhost = None
        else:
            try:               # python 2
                realhost, rest = splithost(rest)
                if realhost:
                    user_passwd, realhost = splituser(realhost)
                    if user_passwd:
                        selector = "%s://%s%s" % (urltype, realhost, rest)
            except NameError:  # python 3 has no splithost
                parsed = urlparse(rest)
                host = parsed.hostname
                if parsed.port:
                    host += ":{0}".format(parsed.port)
                user_passwd = parsed.username
                if parsed.password:
                    user_passwd += ":{0}".format(parsed.password)
        # print("proxy via http:", host, selector)
    if not host:
        raise IOError('http error', 'no host given')
    if user_passwd:
        if six.PY3:
            auth = base64.encodebytes(user_passwd).strip()
        else:
            auth = base64.encodestring(user_passwd).strip()
    else:
        auth = None
    # Start here!
    h = httpslib.HTTPSConnection(host=host, ssl_context=self.ctx)
    # h.set_debuglevel(1)
    # Stop here!
    if data is not None:
        h.putrequest('POST', selector)
        h.putheader('Content-type', 'application/x-www-form-urlencoded')
        h.putheader('Content-length', '%d' % len(data))
    else:
        h.putrequest('GET', selector)
    if auth:
        h.putheader('Authorization', 'Basic %s' % auth)
    for args in self.addheaders:
        h.putheader(*args)   # for python3 - used to use apply
    h.endheaders()
    if data is not None:
        h.send(data + '\r\n')
    # Here again!
    resp = h.getresponse()
    fp = resp.fp
    return addinfourl(fp, resp.msg, "https:" + url)
    # Stop again.

# Minor brain surgery.
URLopener.open_https = open_https
