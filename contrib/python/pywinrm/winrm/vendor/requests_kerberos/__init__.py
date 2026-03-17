# ISC License
#
# Copyright (c) 2012 Kenneth Reitz
#
# Permission to use, copy, modify and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS-IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""
requests Kerberos/GSSAPI authentication library
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Requests is an HTTP library, written in Python, for human beings. This library
adds optional Kerberos/GSSAPI authentication support and supports mutual
authentication. Basic GET usage:

    >>> import requests
    >>> from requests_kerberos import HTTPKerberosAuth
    >>> r = requests.get("http://example.org", auth=HTTPKerberosAuth())

The entire `requests.api` should be supported.
"""
import logging

from .compat import NullHandler
from .exceptions import MutualAuthenticationError
from .kerberos_ import DISABLED, OPTIONAL, REQUIRED, HTTPKerberosAuth

logging.getLogger(__name__).addHandler(NullHandler())

__all__ = ('HTTPKerberosAuth', 'MutualAuthenticationError', 'REQUIRED',
           'OPTIONAL', 'DISABLED')
__version__ = '0.12.0'
