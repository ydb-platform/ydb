#!/usr/bin/env python
#
# Copyright (c) nexB, Inc.
# Copyright (c) 2012-2015 SEOmoz, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

'''This is a module for dealing with urls. In particular, sanitizing them.
This version is a friendly fork of the upstream from Moz to keep a pure Python
version around to run on Python on all OSes.
It also uses an alternate publicsuffix list provider package.
'''

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import codecs
import re
import sys

try:
    import urlparse
except ImportError:  # pragma: no cover
    # Python 3 support
    import urllib.parse as urlparse

try:
    from urllib.parse import quote as urllib_quote
    from urllib.parse import unquote as urllib_unquote
except ImportError:
    from urllib import quote as urllib_quote
    from urllib import unquote as urllib_unquote


try:
    unicode
    str = unicode
except NameError:
    unicode = str


# Python versions
_sys_v0 = sys.version_info[0]
py2 = _sys_v0 == 2
py3 = _sys_v0 == 3


# For publicsuffix utilities
from publicsuffix2 import PublicSuffixList
psl = PublicSuffixList()


# Come codes that we'll need
IDNA = codecs.lookup('idna')
UTF8 = codecs.lookup('utf-8')
ASCII = codecs.lookup('ascii')
W1252 = codecs.lookup('windows-1252')

# The default ports associated with each scheme
PORTS = {
    'http': 80,
    'https': 443
}


def parse(url):
    '''Parse the provided url string and return an URL object'''
    return URL.parse(url)


class URL(object):
    '''
    For more information on how and what we parse / sanitize:
        http://tools.ietf.org/html/rfc1808.html
    The more up-to-date RFC is this one:
        http://www.ietf.org/rfc/rfc3986.txt
    '''

    # Via http://www.ietf.org/rfc/rfc3986.txt
    _GEN_DELIMS = ":/?#[]@"
    _SUB_DELIMS = "!$&'()*+,;="
    _ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    _DIGIT = "0123456789"
    _UNRESERVED = _ALPHA + _DIGIT + "-._~"
    _RESERVED = _GEN_DELIMS + _SUB_DELIMS
    _PCHAR = _UNRESERVED + _SUB_DELIMS + ":@"
    PATH = (_PCHAR + "/").encode('utf-8')
    QUERY = (_PCHAR + "/?").encode('utf-8')
    FRAGMENT = (_PCHAR + "/?").encode('utf-8')
    USERINFO = (_UNRESERVED + _SUB_DELIMS + ":").encode('utf-8')

    PERCENT_ESCAPING_RE = re.compile(r'(%([a-fA-F0-9]{2})|.)', re.S)

    @classmethod
    def parse(cls, url):
        '''Parse the provided url, and return a URL instance'''
        if isinstance(url, URL):
            return url
        parsed = urlparse.urlparse(url)

        try:
            port = parsed.port
        except ValueError:
            port = None

        userinfo = parsed.username
        if userinfo and parsed.password:
            userinfo += ':%s' % parsed.password

        return cls(parsed.scheme, parsed.hostname, port,
            parsed.path, parsed.params, parsed.query, parsed.fragment, userinfo)

    def __init__(self, scheme, host, port, path, params, query, fragment, userinfo=None):
        self.scheme = scheme
        self.host = host
        self.port = port
        self.path = path or '/'
        self.params = re.sub(r'^;+', '', str(params))
        self.params = re.sub(r'^;|;$', '', re.sub(r';{2,}', ';', self.params))
        # Strip off extra leading ?'s
        self.query = re.sub(r'^\?+', '', str(query))
        self.query = re.sub(r'^&|&$', '', re.sub(r'&{2,}', '&', self.query))
        self.fragment = fragment
        self.userinfo = userinfo

    def copy(self):
        '''Return a new instance of an identical URL.'''
        return URL(
            self.scheme,
            self.host,
            self.port,
            self.path,
            self.params,
            self.query,
            self.fragment,
            self.userinfo)

    def equiv(self, other):
        '''Return true if this url is equivalent to another'''
        _other = self.parse(other)
        _other.canonical().defrag().abspath().escape().punycode()

        _self = self.parse(self)
        _self.canonical().defrag().abspath().escape().punycode()

        result = (
            _self.scheme == _other.scheme    and
            _self.host == _other.host      and
            _self.path == _other.path      and
            _self.params == _other.params    and
            _self.query == _other.query)

        if result:
            if _self.port and not _other.port:
                # Make sure _self.port is the default for the scheme
                return _self.port == PORTS.get(_self.scheme, None)
            elif _other.port and not _self.port:
                # Make sure _other.port is the default for the scheme
                return _other.port == PORTS.get(_other.scheme, None)
            else:
                return _self.port == _other.port
        else:
            return False

    def __eq__(self, other):
        '''Return true if this url is /exactly/ equal to another'''
        other = self.parse(other)
        return (
            self.scheme == other.scheme    and
            self.host == other.host      and
            self.path == other.path      and
            self.port == other.port      and
            self.params == other.params    and
            self.query == other.query     and
            self.fragment == other.fragment  and
            self.userinfo == other.userinfo)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        netloc = self.host or ''
        if self.port:
            netloc += (':' + str(self.port))

        if self.userinfo is not None:
            netloc = '{}@{}'.format(self.userinfo, netloc)

        result = urlparse.urlunparse((
            str(self.scheme),
            str(netloc),
            str(self.path),
            str(self.params),
            str(self.query),
            self.fragment))
        if isinstance(result, bytes):
            result = result.decode('utf-8')
        return result

    def __repr__(self):
        return '<urlpy.URL object "{}">'.format(str(self))

    def canonical(self):
        '''Canonicalize this url. This includes reordering parameters and args
        to have a consistent ordering'''
        self.query = '&'.join(sorted([q for q in self.query.split('&')]))
        self.params = ';'.join(sorted([q for q in self.params.split(';')]))
        return self

    def defrag(self):
        '''Remove the fragment from this url'''
        self.fragment = None
        return self

    def deparam(self, params):
        '''Strip any of the provided parameters out of the url'''
        lowered = set([p.lower() for p in params])
        def function(name, _):
            return name.lower() in lowered
        return self.filter_params(function)

    def filter_params(self, function):
        '''Remove parameters if function(name, value)'''
        def keep(query):
            name, _, value = query.partition('=')
            return not function(name, value)
        self.query = '&'.join(q for q in self.query.split('&') if q and keep(q))
        self.params = ';'.join(q for q in self.params.split(';') if q and keep(q))
        return self

    def deuserinfo(self):
        '''Remove any userinfo'''
        self.userinfo = None
        return self

    def abspath(self):
        '''Clear out any '..' and excessive slashes from the path'''
        # Remove double forward-slashes from the path
        path = re.sub(r'\/{2,}', '/', self.path)
        # With that done, go through and remove all the relative references
        unsplit = []
        directory = False
        for part in path.split('/'):
            # If we encounter the parent directory, and there's
            # a segment to pop off, then we should pop it off.
            if part == '..' and (not unsplit or unsplit.pop() != None):
                directory = True
            elif part != '.':
                unsplit.append(part)
                directory = False
            else:
                directory = True

        # With all these pieces, assemble!
        if directory:
            # If the path ends with a period, then it refers to a directory,
            # not a file path
            self.path = '/'.join(unsplit) + '/'
        else:
            self.path = '/'.join(unsplit)
        return self

    def sanitize(self):
        '''A shortcut to abspath and escape'''
        return self.abspath().escape()

    def remove_default_port(self):
        '''If a port is provided an is the default, remove it.'''
        if self.port and self.scheme and (self.port == PORTS[self.scheme]):
            self.port = None
        return self

    def escape(self):
        '''Make sure that the path is correctly escaped'''
        upath = urllib_unquote(self.path).encode('utf-8')
        self.path = urllib_quote(upath, safe=URL.PATH)
        if py2:
            self.path = self.path.decode('utf-8')

        # Safe characters taken from:
        #    http://tools.ietf.org/html/rfc3986#page-50
        uquery = urllib_unquote(self.query).encode('utf-8')
        self.query = urllib_quote(uquery, safe=URL.QUERY)
        if py2:
            self.query = self.query.decode('utf-8')

        # The safe characters for URL parameters seemed a little more vague.
        # They are interpreted here as *pchar despite this page, since the
        # updated RFC seems to offer no replacement
        #    http://tools.ietf.org/html/rfc3986#page-54
        uparams = urllib_unquote(self.params).encode('utf-8')
        self.params = urllib_quote(uparams, safe=URL.QUERY)
        if py2:
            self.params = self.params.decode('utf-8')

        if self.userinfo:
            uuserinfo = urllib_unquote(self.userinfo).encode('utf-8')
            self.userinfo = urllib_quote(uuserinfo, safe=URL.USERINFO)
            if py2:
                self.userinfo = self.userinfo.decode('utf-8')
        return self

    def unescape(self):
        '''Unescape the path'''
        path = self.path
        if py2:
            path = self.path.encode('utf-8')
        path = urllib_unquote(path)
        if py2:
            path = path.decode('utf-8')
        self.path = path
        return self

    def relative(self, path):
        '''Evaluate the new path relative to the current url'''
        newurl = urlparse.urljoin(str(self), path)
        return URL.parse(newurl)

    def punycode(self):
        '''Convert to punycode hostname'''
        if self.host:
            self.host = (IDNA.encode(self.host)[0]).decode('utf-8')
            return self
        raise TypeError('Cannot punycode a relative url (%s)' % repr(self))

    def unpunycode(self):
        '''Convert to an unpunycoded hostname'''
        if self.host:
            self.host = IDNA.decode(self.host.encode('utf-8'))[0]
            return self
        raise TypeError('Cannot unpunycode a relative url (%s)' % repr(self))

    @property
    def hostname(self):
        '''Return the hostname of the url.'''
        return self.host or ''

    @property
    def pld(self):
        '''Return the 'pay-level domain' of the url
            (http://moz.com/blog/what-the-heck-should-we-call-domaincom)'''
        if self.host:
            return psl.get_public_suffix(self.host)
        return ''

    @property
    def tld(self):
        '''Return the top-level domain of a url'''
        if self.host:
            return '.'.join(self.pld.split('.')[1:])
        return ''

    @property
    def absolute(self):
        '''Return True if this is a fully-qualified URL with a hostname and
        everything'''
        return bool(self.host)

    @property
    def unicode(self):
        '''Return a utf-8 version of this url'''
        return str(self)
