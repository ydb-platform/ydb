# -*- coding: utf-8 -
#
# This file is part of http-parser released under the MIT license.
# See the NOTICE for more information.

from libc.stdlib cimport *
import os
try:
    from urllib.parse import urlsplit
except ImportError:
    from urlparse import urlsplit

import zlib

from http_parser.util import b, bytes_to_str, IOrderedDict, unquote

cdef extern from "pyversion_compat.h":
    pass

from cpython cimport PyBytes_FromStringAndSize

cdef extern from "http_parser.h" nogil:
    cdef enum http_errno:
        HPE_OK, HPE_UNKNOWN

    cdef enum http_method:
        HTTP_DELETE, HTTP_GET, HTTP_HEAD, HTTP_POST, HTTP_PUT,
        HTTP_CONNECT, HTTP_OPTIONS, HTTP_TRACE, HTTP_COPY, HTTP_LOCK,
        HTTP_MKCOL, HTTP_MOVE, HTTP_PROPFIND, HTTP_PROPPATCH, HTTP_UNLOCK,
        HTTP_REPORT, HTTP_MKACTIVITY, HTTP_CHECKOUT, HTTP_MERGE, HTTP_MSEARCH,
        HTTP_NOTIFY, HTTP_SUBSCRIBE, HTTP_UNSUBSCRIBE, HTTP_PATCH,
        HTTP_PURGE

    cdef enum http_parser_type:
        HTTP_REQUEST, HTTP_RESPONSE, HTTP_BOTH

    cdef struct http_parser:
        int content_length
        unsigned short http_major
        unsigned short http_minor
        unsigned short status_code
        unsigned char method
        unsigned char http_errno
        char upgrade
        void *data

    ctypedef int (*http_data_cb) (http_parser*, char *at, size_t length)
    ctypedef int (*http_cb) (http_parser*)

    struct http_parser_settings:
        http_cb on_message_begin
        http_data_cb on_url
        http_data_cb on_header_field
        http_data_cb on_header_value
        http_cb on_headers_complete
        http_data_cb on_body
        http_cb on_message_complete

    void http_parser_init(http_parser *parser,
            http_parser_type ptype)

    size_t http_parser_execute(http_parser *parser,
            http_parser_settings *settings, char *data,
            size_t len)

    int http_should_keep_alive(http_parser *parser)

    char *http_method_str(http_method)

    char *http_errno_name(http_errno)

    char *http_errno_description(http_errno)


cdef int on_url_cb(http_parser *parser, char *at,
        size_t length):
    res = <object>parser.data
    res.url += bytes_to_str(PyBytes_FromStringAndSize(at, length))
    return 0

cdef int on_header_field_cb(http_parser *parser, char *at,
        size_t length):
    header_field = PyBytes_FromStringAndSize(at, length)
    res = <object>parser.data

    if res._last_was_value:
        res._last_field = ""
    res._last_field += bytes_to_str(header_field)
    res._last_was_value = False
    return 0

cdef int on_header_value_cb(http_parser *parser, char *at,
        size_t length):
    res = <object>parser.data
    header_value = bytes_to_str(PyBytes_FromStringAndSize(at, length))

    if res._last_field in res.headers:
        hval = res.headers[res._last_field]
        if not res._last_was_value:
            header_value = "%s, %s" % (hval, header_value)
        else:
            header_value = "%s %s" % (hval, header_value)

    # add to headers
    res.headers[res._last_field] = header_value
    res._last_was_value = True
    return 0

cdef int on_headers_complete_cb(http_parser *parser):
    res = <object>parser.data
    res.headers_complete = True

    if res.decompress:
        encoding = res.headers.get('content-encoding')
        if encoding == 'gzip':
            res.decompressobj = zlib.decompressobj(16+zlib.MAX_WBITS)
            res._decompress_first_try = False
            del res.headers['content-encoding']
        elif encoding == 'deflate':
            res.decompressobj = zlib.decompressobj()
            del res.headers['content-encoding']
        else:
            res.decompress = False

    return res.header_only and 1 or 0

cdef int on_message_begin_cb(http_parser *parser):
    res = <object>parser.data
    res.message_begin = True
    return 0

cdef int on_body_cb(http_parser *parser, char *at,
        size_t length):
    res = <object>parser.data
    value = PyBytes_FromStringAndSize(at, length)

    res.partial_body = True

    # decompress the value if needed
    if res.decompress:
        if not res._decompress_first_try:
            value = res.decompressobj.decompress(value)
        else:
            try:
                value = res.decompressobj.decompress(value)
            except zlib.error:
                res.decompressobj = zlib.decompressobj(-zlib.MAX_WBITS)
                value = res.decompressobj.decompress(value)
            res._decompress_first_try = False

    res.body.append(value)
    return 0

cdef int on_message_complete_cb(http_parser *parser):
    res = <object>parser.data
    res.message_complete = True
    return 0


def get_errno_name(errno):
    if not HPE_OK <= errno <= HPE_UNKNOWN:
        raise ValueError('errno out of range')
    return http_errno_name(<http_errno>errno)

def get_errno_description(errno):
    if not HPE_OK <= errno <= HPE_UNKNOWN:
        raise ValueError('errno out of range')
    return http_errno_description(<http_errno>errno)


class _ParserData(object):

    def __init__(self, decompress=False, header_only=False):
        self.url = ""
        self.body = []
        self.headers = IOrderedDict()
        self.header_only = header_only

        self.decompress = decompress
        self.decompressobj = None
        self._decompress_first_try = True

        self.chunked = False

        self.headers_complete = False
        self.partial_body = False
        self.message_begin = False
        self.message_complete = False

        self._last_field = ""
        self._last_was_value = False

cdef class HttpParser:
    """ Low level HTTP parser.  """

    cdef http_parser _parser
    cdef http_parser_settings _settings
    cdef object _data

    cdef str _path
    cdef str _query_string
    cdef str _fragment
    cdef object _parsed_url

    def __init__(self, kind=2, decompress=False, header_only=False):
        """ constructor of HttpParser object.
        :
        attr kind: Int,  could be 0 to parseonly requests,
        1 to parse only responses or 2 if we want to let
        the parser detect the type.
        """

        # set parser type
        if kind == 2:
            parser_type = HTTP_BOTH
        elif kind == 1:
            parser_type = HTTP_RESPONSE
        elif kind == 0:
            parser_type = HTTP_REQUEST

        # initialize parser
        http_parser_init(&self._parser, parser_type)
        self._data = _ParserData(decompress=decompress, header_only=header_only)
        self._parser.data = <void *>self._data
        self._parsed_url = None
        self._path = ""
        self._query_string = ""
        self._fragment = ""

        # set callback
        self._settings.on_url = <http_data_cb>on_url_cb
        self._settings.on_body = <http_data_cb>on_body_cb
        self._settings.on_header_field = <http_data_cb>on_header_field_cb
        self._settings.on_header_value = <http_data_cb>on_header_value_cb
        self._settings.on_headers_complete = <http_cb>on_headers_complete_cb
        self._settings.on_message_begin = <http_cb>on_message_begin_cb
        self._settings.on_message_complete = <http_cb>on_message_complete_cb

    def execute(self, char *data, size_t length):
        """ Execute the parser with the last chunk. We pass the length
        to let the parser know when EOF has been received. In this case
        length == 0.

        :return recved: Int, received length of the data parsed. if
        recvd != length you should return an error.
        """
        return http_parser_execute(&self._parser, &self._settings,
                data, length)

    def get_errno(self):
        """ get error state """
        return self._parser.http_errno

    def get_version(self):
        """ get HTTP version """
        return (self._parser.http_major, self._parser.http_minor)

    def get_method(self):
        """ get HTTP method as string"""
        return bytes_to_str(http_method_str(<http_method>self._parser.method))

    def get_status_code(self):
        """ get status code of a response as integer """
        return self._parser.status_code

    def get_url(self):
        """ get full url of the request """
        return self._data.url

    def maybe_parse_url(self):
        raw_url = self.get_url()
        if not self._parsed_url and raw_url:
            self._parsed_url = urlsplit(raw_url)
            self._path =  self._parsed_url.path or ""
            self._query_string = self._parsed_url.query or ""
            self._fragment = self._parsed_url.fragment or ""

    def get_path(self):
        """ get path of the request (url without query string and
        fragment """
        self.maybe_parse_url()
        return self._path

    def get_query_string(self):
        """ get query string of the url """
        self.maybe_parse_url()
        return self._query_string

    def get_fragment(self):
        """ get fragment of the url """
        self.maybe_parse_url()
        return self._fragment

    def get_headers(self):
        """ get request/response headers, headers are returned in a
        OrderedDict that allows you to get value using insensitive keys. """
        return self._data.headers

    def get_wsgi_environ(self):
        """ get WSGI environ based on the current request """
        self.maybe_parse_url()

        environ = dict()
        script_name = os.environ.get("SCRIPT_NAME", "")
        for key, val in self._data.headers.items():
            ku = key.upper()
            if ku == "CONTENT-TYPE":
                environ['CONTENT_TYPE'] = val
            elif ku == "CONTENT-LENGTH":
                environ['CONTENT_LENGTH'] = val
            elif ku == "SCRIPT_NAME":
                environ['SCRIPT_NAME'] = val
            else:
                environ['HTTP_%s' % ku.replace('-','_')] = val

        if script_name:
            path_info = self._path.split(script_name, 1)[1]
        else:
            path_info = self._path

        environ.update({
            'REQUEST_METHOD': self.get_method(),
            'SERVER_PROTOCOL': "HTTP/%s" % ".".join(map(str,
                self.get_version())),
            'PATH_INFO': path_info,
            'SCRIPT_NAME': script_name,
            'QUERY_STRING': self._query_string,
            'RAW_URI': self._data.url})

        return environ

    def recv_body(self):
        """ return last chunk of the parsed body"""
        body = b("").join(self._data.body)
        self._data.body = []
        self._data.partial_body = False
        return body

    def recv_body_into(self, barray):
        """ Receive the last chunk of the parsed body and store the data
        in a buffer rather than creating a new string. """
        l = len(barray)
        body = b("").join(self._data.body)
        m = min(len(body), l)
        data, rest = body[:m], body[m:]
        barray[0:m] = bytes(data)
        if not rest:
            self._data.body = []
            self._data.partial_body = False
        else:
            self._data.body = [rest]
        return m

    def is_upgrade(self):
        """ Do we get upgrade header in the request. Useful for
        websockets """
        return self._parser.upgrade

    def is_headers_complete(self):
        """ return True if all headers have been parsed. """
        return self._data.headers_complete

    def is_partial_body(self):
        """ return True if a chunk of body have been parsed """
        return self._data.partial_body

    def is_message_begin(self):
        """ return True if the parsing start """
        return self._data.message_begin

    def is_message_complete(self):
        """ return True if the parsing is done (we get EOF) """
        return self._data.message_complete

    def is_chunked(self):
        """ return True if Transfer-Encoding header value is chunked"""
        te = self._data.headers.get('transfer-encoding', '').lower()
        return te == 'chunked'

    def should_keep_alive(self):
        """ return True if the connection should be kept alive
        """
        return http_should_keep_alive(&self._parser)
