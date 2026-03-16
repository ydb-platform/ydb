# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import sys
import re
import os

from oscrypto import tls, errors as oscrypto_errors, version
from asn1crypto.util import OrderedDict

if sys.version_info < (3,):
    from urlparse import urlparse
    str_cls = unicode  # noqa
else:
    from urllib.parse import urlparse
    str_cls = str


class HttpsClientException(Exception):
    pass


class HttpsClientError(HttpsClientException):
    pass


class HttpsClient():

    def __init__(self, keep_alive=True, ignore_close=False):
        self.socket = None
        self.timeout = None
        self.url_info = None
        self.keep_alive = keep_alive
        self.ignore_close = ignore_close

    def close(self):
        """
        Closes any open connection
        """

        if not self.socket:
            return
        self.socket.close()
        self.socket = None

    def download(self, url, timeout):
        """
        Downloads a URL and returns the contents

        :param url:
            The URL to download

        :param timeout:
            The int number of seconds to set the timeout to

        :return:
            The string contents of the URL
        """

        self.setup_connection(url, timeout)
        tries = 0

        while tries < 2:
            tries += 1
            try:
                self.ensure_connected()

                req_headers = OrderedDict()
                req_headers['Host'] = self.url_info[0]
                if self.url_info[1] != 443:
                    req_headers['Host'] += ':%d' % self.url_info[1]
                req_headers['Connection'] = 'Keep-Alive' if self.keep_alive else 'Close'
                req_headers["User-Agent"] = 'oscrypto %s TLS HTTP Client' % version.__version__

                request = 'GET '
                url_info = urlparse(url)
                path = '/' if not url_info.path else url_info.path
                if url_info.query:
                    path += '?' + url_info.query
                request += path + ' HTTP/1.1'
                self.write_request(request, req_headers)

                response = self.read_headers()
                if not response:
                    self.close()
                    continue

                v, code, message, resp_headers = response
                data = self.read_body(code, resp_headers, timeout)

                if code == 301:
                    location = resp_headers.get('location')
                    if not isinstance(location, str_cls):
                        raise HttpsClientError('Missing or duplicate Location HTTP header')
                    if not re.match(r'https?://', location):
                        if not location.startswith('/'):
                            location = os.path.dirname(url_info.path) + location
                        location = url_info.scheme + '://' + url_info.netloc + location
                    return self.download(location, timeout)

                if code != 200:
                    raise HttpsClientError('HTTP error %s downloading %s.' % (code, url))

                else:
                    return data

            except (oscrypto_errors.TLSGracefulDisconnectError):
                self.close()
                continue

    def setup_connection(self, url, timeout):
        """
        :param url:
            The URL to download

        :param timeout:
            The int number of seconds to set the timeout to

        :return:
            A boolean indicating if the connection was reused
        """

        url_info = urlparse(url)
        if url_info.scheme == 'http':
            raise HttpsClientException('Can not connect to a non-TLS server')
        hostname = url_info.hostname
        port = url_info.port
        if not port:
            port = 443

        if self.socket and self.url_info != (hostname, port):
            self.close()

        self.timeout = timeout
        self.url_info = (hostname, port)

        return self.ensure_connected()

    def ensure_connected(self):
        """
        Make sure a valid tls.TLSSocket() is open to the server

        :return:
            A boolean indicating if the connection was reused
        """

        if self.socket:
            return True

        host, port = self.url_info
        session = tls.TLSSession()
        self.socket = tls.TLSSocket(host, port, timeout=self.timeout, session=session)
        return False

    def write_request(self, request, headers):
        """
        :param request:
            A unicode string of the first line of the HTTP request

        :param headers:
            An OrderedDict of the request headers
        """

        lines = [request]
        for header, value in headers.items():
            lines.append('%s: %s' % (header, value))
        lines.extend(['', ''])

        request = '\r\n'.join(lines).encode('iso-8859-1')
        self.socket.write(request)

    def read_headers(self):
        """
        Reads the HTTP response headers from the socket

        :return:
            On error, None, otherwise a 4-element tuple:
              0: A 2-element tuple of integers representing the HTTP version
              1: An integer representing the HTTP response code
              2: A unicode string of the HTTP response code name
              3: An OrderedDict of HTTP headers with lowercase unicode key and unicode values
        """

        version = None
        code = None
        text = None
        headers = OrderedDict()

        data = self.socket.read_until(b'\r\n\r\n')
        string = data.decode('iso-8859-1')
        first = False
        for line in string.split('\r\n'):
            line = line.strip()
            if first is False:
                if line == '':
                    continue
                match = re.match(r'^HTTP/(1\.[01]) +(\d+) +(.*)$', line)
                if not match:
                    return None
                version = tuple(map(int, match.group(1).split('.')))
                code = int(match.group(2))
                text = match.group(3)
                first = True
            else:
                if not len(line):
                    continue
                parts = line.split(':', 1)
                if len(parts) == 2:
                    name = parts[0].strip().lower()
                    value = parts[1].strip()
                    if name in headers:
                        if isinstance(headers[name], tuple):
                            headers[name] = headers[name] + (value,)
                        else:
                            headers[name] = (headers[name], value)
                    else:
                        headers[name] = value

        return (version, code, text, headers)

    def parse_content_length(self, headers):
        """
        Returns the content-length from a dict of headers

        :return:
            An integer of the content length
        """

        content_length = headers.get('content-length')
        if isinstance(content_length, str_cls) and len(content_length) > 0:
            content_length = int(content_length)
        return content_length

    def read_body(self, code, resp_headers, timeout):
        """

        """

        data = b''
        transfer_encoding = resp_headers.get('transfer-encoding')
        if transfer_encoding and transfer_encoding.lower() == 'chunked':
            while True:
                line = self.socket.read_until(b'\r\n').decode('iso-8859-1').rstrip()
                if re.match(r'^[a-fA-F0-9]+$', line):
                    chunk_length = int(line, 16)
                    if chunk_length == 0:
                        break
                    data += self.socket.read_exactly(chunk_length)
                    if self.socket.read_exactly(2) != b'\r\n':
                        raise HttpsClientException('Unable to parse chunk newline')
                else:
                    self.close()
                    raise HttpsClientException('Unable to parse chunk length')
        else:
            content_length = self.parse_content_length(resp_headers)
            if content_length is not None:
                if content_length > 0:
                    data = self.socket.read_exactly(content_length)
            elif code == 304 or code == 204 or (code >= 100 and code < 200):
                # Per https://tools.ietf.org/html/rfc7230#section-3.3.3 these have no body
                pass
            else:
                # This should only happen if the server is going to close the connection
                while self.socket.select_read(timeout=timeout):
                    data += self.socket.read(8192)
                self.close()

        if not self.ignore_close and resp_headers.get('connection', '').lower() == 'close':
            self.close()

        return data
