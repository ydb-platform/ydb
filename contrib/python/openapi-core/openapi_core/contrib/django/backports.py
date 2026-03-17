"""OpenAPI core contrib django backports module"""


class HttpHeaders(dict):
    HTTP_PREFIX = 'HTTP_'
    # PEP 333 gives two headers which aren't prepended with HTTP_.
    UNPREFIXED_HEADERS = {'CONTENT_TYPE', 'CONTENT_LENGTH'}

    def __init__(self, environ):
        headers = {}
        for header, value in environ.items():
            name = self.parse_header_name(header)
            if name:
                headers[name] = value
        super(HttpHeaders, self).__init__(headers)

    @classmethod
    def parse_header_name(cls, header):
        if header.startswith(cls.HTTP_PREFIX):
            header = header[len(cls.HTTP_PREFIX):]
        elif header not in cls.UNPREFIXED_HEADERS:
            return None
        return header.replace('_', '-').title()


def request_current_scheme_host(req):
    return '{}://{}'.format(req.scheme, req.get_host())
