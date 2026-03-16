from __future__ import absolute_import

"""M2Crypto enhancement to xmlrpclib.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

import base64

from M2Crypto import __version__ as __M2Version

from M2Crypto import SSL, httpslib, m2urllib, six
from typing import Any, AnyStr, Callable, Optional  # noqa

from M2Crypto.six.moves.xmlrpc_client import ProtocolError, Transport
# six.moves doesn't support star imports
if six.PY3:
    from xmlrpc.client import *  # noqa
else:
    from xmlrpclib import *  # noqa

__version__ = __M2Version


class SSL_Transport(Transport):

    user_agent = "M2Crypto_XMLRPC/%s - %s" % (__version__,
                                              Transport.user_agent)

    def __init__(self, ssl_context=None, *args, **kw):
        # type: (Optional[SSL.Context], *Any, **Any) -> None
        Transport.__init__(self, *args, **kw)
        if ssl_context is None:
            self.ssl_ctx = SSL.Context()
        else:
            self.ssl_ctx = ssl_context

    def request(self, host, handler, request_body, verbose=0):
        # type: (AnyStr, Callable, bytes, int) -> object
        # Handle username and password.
        user_passwd, host_port = m2urllib.splituser(host)
        _host, _port = m2urllib.splitport(host_port)
        h = httpslib.HTTPSConnection(_host, int(_port),
                                     ssl_context=self.ssl_ctx)
        if verbose:
            h.set_debuglevel(1)

        # What follows is as in xmlrpclib.Transport. (Except the authz bit.)
        h.putrequest("POST", handler)

        # required by HTTP/1.1
        h.putheader("Host", _host)

        # required by XML-RPC
        h.putheader("User-Agent", self.user_agent)
        h.putheader("Content-Type", "text/xml")
        h.putheader("Content-Length", str(len(request_body)))

        # Authorisation.
        if user_passwd is not None:
            auth = base64.encodestring(user_passwd).strip()
            h.putheader('Authorization', 'Basic %s' % auth)

        h.endheaders()

        if request_body:
            h.send(request_body)

        response = h.getresponse()

        if response.status != 200:
            raise ProtocolError(
                host + handler,
                response.status, response.reason,
                response.getheaders()
            )

        self.verbose = verbose
        return self.parse_response(response)
