from .mock_response import MockHTTPResponse
from datetime import datetime
from requests.models import PreparedRequest, Response
from requests.packages.urllib3 import HTTPResponse
from requests.structures import CaseInsensitiveDict
from requests.status_codes import _codes
from requests.cookies import RequestsCookieJar

try:
    from requests.packages.urllib3._collections import HTTPHeaderDict
except ImportError:
    from .headers import HTTPHeaderDict

import base64
import io
import sys


def coerce_content(content, encoding=None):
    if hasattr(content, 'decode'):
        content = content.decode(encoding or 'utf-8', 'replace')
    return content


def body_io(string, encoding=None):
    if hasattr(string, 'encode'):
        string = string.encode(encoding or 'utf-8')
    return io.BytesIO(string)


def from_list(value):
    if isinstance(value, list):
        return value[0]
    return value


def add_body(r, preserve_exact_body_bytes, body_dict):
    """Simple function which takes a response or request and coerces the body.

    This function adds either ``'string'`` or ``'base64_string'`` to
    ``body_dict``. If ``preserve_exact_body_bytes`` is ``True`` then it
    encodes the body as a base64 string and saves it like that. Otherwise,
    it saves the plain string.

    :param r: This is either a PreparedRequest instance or a Response
        instance.
    :param preserve_exact_body_bytes bool: Either True or False.
    :param body_dict dict: A dictionary already containing the encoding to be
        used.
    """
    body = getattr(r, 'raw', getattr(r, 'body', None))
    if hasattr(body, 'read'):
        body = body.read()

    if not body:
        body = ''

    if (preserve_exact_body_bytes or
            'gzip' in r.headers.get('Content-Encoding', '')):
        if sys.version_info >= (3, 0) and hasattr(body, 'encode'):
            body = body.encode(body_dict['encoding'] or 'utf-8')

        body_dict['base64_string'] = base64.b64encode(body).decode()
    else:
        body_dict['string'] = coerce_content(body, body_dict['encoding'])


def serialize_prepared_request(request, preserve_exact_body_bytes):
    headers = request.headers
    body = {'encoding': 'utf-8'}
    add_body(request, preserve_exact_body_bytes, body)
    return {
        'body': body,
        'headers': dict(
            (coerce_content(k, 'utf-8'), [v]) for (k, v) in headers.items()
        ),
        'method': request.method,
        'uri': request.url,
    }


def deserialize_prepared_request(serialized):
    p = PreparedRequest()
    p._cookies = RequestsCookieJar()
    body = serialized['body']
    if isinstance(body, dict):
        original_body = body.get('string')
        p.body = original_body or base64.b64decode(
            body.get('base64_string', '').encode())
    else:
        p.body = body
    h = [(k, from_list(v)) for k, v in serialized['headers'].items()]
    p.headers = CaseInsensitiveDict(h)
    p.method = serialized['method']
    p.url = serialized['uri']
    return p


def serialize_response(response, preserve_exact_body_bytes):
    body = {'encoding': response.encoding}
    add_body(response, preserve_exact_body_bytes, body)
    header_map = HTTPHeaderDict(response.raw.headers)
    headers = {}
    for header_name in header_map.keys():
        headers[header_name] = header_map.getlist(header_name)

    return {
        'body': body,
        'headers': headers,
        'status': {'code': response.status_code, 'message': response.reason},
        'url': response.url,
    }


def deserialize_response(serialized):
    r = Response()
    r.encoding = serialized['body']['encoding']
    header_dict = HTTPHeaderDict()

    for header_name, header_list in serialized['headers'].items():
        if isinstance(header_list, list):
            for header_value in header_list:
                header_dict.add(header_name, header_value)
        else:
            header_dict.add(header_name, header_list)
    r.headers = CaseInsensitiveDict(header_dict)

    r.url = serialized.get('url', '')
    if 'status' in serialized:
        r.status_code = serialized['status']['code']
        r.reason = serialized['status']['message']
    else:
        r.status_code = serialized['status_code']
        r.reason = _codes[r.status_code][0].upper()
    add_urllib3_response(serialized, r, header_dict)
    return r


def add_urllib3_response(serialized, response, headers):
    if 'base64_string' in serialized['body']:
        body = io.BytesIO(
            base64.b64decode(serialized['body']['base64_string'].encode())
        )
    else:
        body = body_io(**serialized['body'])

    h = HTTPResponse(
        body,
        status=response.status_code,
        reason=response.reason,
        headers=headers,
        preload_content=False,
        original_response=MockHTTPResponse(headers)
    )
    # NOTE(sigmavirus24):
    # urllib3 updated it's chunked encoding handling which breaks on recorded
    # responses. Since a recorded response cannot be streamed appropriately
    # for this handling to work, we can preserve the integrity of the data in
    # the response by forcing the chunked attribute to always be False.
    # This isn't pretty, but it is much better than munging a response.
    h.chunked = False
    response.raw = h


def timestamp():
    stamp = datetime.utcnow().isoformat()
    try:
        i = stamp.rindex('.')
    except ValueError:
        return stamp
    else:
        return stamp[:i]


_SENTINEL = object()


def _option_from(option, kwargs, defaults):
    value = kwargs.get(option, _SENTINEL)
    if value is _SENTINEL:
        value = defaults.get(option)
    return value
